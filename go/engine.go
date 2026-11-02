package main

// 事务、多版本、行锁、间隙锁。纯 Go，不依赖 cgo，可直接 `go test -race`。
// 设计见 docs/plan-mvcc.md 与 docs/plan-gap-latch.md。
//
// 没有全局数据锁：并发控制分三层——
//   - 逻辑层：行锁（lm，分片）+ 间隙锁（gap.go）+ MVCC 版本链；
//   - 物理层：每棵 B+ 树自己的两级锁（btree.go：smo 锁 + 叶子锁）；
//   - 版本链受它所在的"主索引叶子锁"保护：读者在 GetFn/Locate 回调里遍历链，写者在 Upsert/Modify 回调里修改链。
//
// 加锁顺序（必须遵守，否则会死锁；任何等待都发生在释放下面第 2~4 项之后）：
//
//  1. 行锁（lm 分片）——只在不持有任何别的锁时才可能阻塞
//  2. 唯一键条纹锁（按下标升序）
//  3. 各索引 gapMu：发布者持读锁（按索引号升序），登记间隙的游标持写锁
//  4. 树锁：smo → 叶子锁。同一个 goroutine 不嵌套任何一种读写锁的读锁，也最多同时持有一个叶子锁
//  5. e.tmu、lm.dmu、e.pmu、e.mu 都是叶子锁，持有它们时不再取别的锁
import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// 加锁模式
const (
	lockNone = 0
	lockS    = 1
	lockX    = 2
)

// 隔离级别（与 MySQL 的 enum_tx_isolation 数值一致）
const (
	isoReadUncommitted = 0
	isoReadCommitted   = 1
	isoRepeatableRead  = 2
	isoSerializable    = 3
)

var (
	ErrNotFound    = errors.New("myengine: not found")
	ErrDeadlock    = errors.New("myengine: deadlock detected")
	ErrLockTimeout = errors.New("myengine: lock wait timeout")
	ErrKilled      = errors.New("myengine: query interrupted")
)

// killPoll：等待期间每隔这么久检查一次连接是否被 KILL。
const killPoll = 100 * time.Millisecond

// DupError 表示唯一键冲突；Idx 是冲突的索引号（0 为主键）。
type DupError struct{ Idx int }

func (e DupError) Error() string { return "myengine: duplicate key" }

// ---------------------------------------------------------------- 数据结构

// secKey 是一个版本在某个二级索引下的键（不含主键后缀）。
type secKey struct {
	idx    int
	key    []byte
	unique bool // 调用方已排除含 NULL 的键
}

// rowVer 是行的一个版本。字段创建后不再修改（prev 在 purge 时除外，且只在叶子写锁内）。
type rowVer struct {
	trx     *Txn
	deleted bool // 墓碑
	data    []byte
	sec     []secKey
	prev    *rowVer
}

// rowHead 是主索引的值：版本链头（最新版本）。head 为 nil 表示链是空的（回滚弹出最后一个版本到删除键之间的短暂窗口），
// 所有读者都要把它当作"不存在"。head 的内容受它所在叶子的锁保护。
type rowHead struct{ head *rowVer }

var tableSeq atomic.Uint32

type table struct {
	id   uint32
	name string
	pri  *BTree      // 主键编码 -> *rowHead
	sec  []*BTree    // 下标 = 索引号，[0] 不用；条目键 = 二级索引键 + 主键，值为 nil
	ix   []*idxState // 下标 = 索引号（含 0），每个索引的间隙锁状态

	amu      sync.Mutex
	auto     uint64
	autoInit bool
}

func newTable(name string, nidx int) *table {
	t := &table{id: tableSeq.Add(1), name: name, pri: NewBTree()}
	t.sec = make([]*BTree, nidx)
	t.ix = make([]*idxState, nidx)
	for i := 0; i < nidx; i++ {
		t.ix[i] = &idxState{}
		if i > 0 {
			t.sec[i] = NewBTree()
		}
	}
	return t
}

func (tb *table) tree(idx int) *BTree {
	if idx == 0 {
		return tb.pri
	}
	return tb.sec[idx]
}

// withHead 在主索引叶子读锁内对 pk 的版本链头调用 fn；键不存在或链为空时 h 为 nil。
// fn 必须很短，且不得再取别的锁。
func (tb *table) withHead(pk []byte, fn func(h *rowHead)) {
	if !tb.pri.GetFn(pk, func(v any) {
		if h := v.(*rowHead); h.head != nil {
			fn(h)
		} else {
			fn(nil)
		}
	}) {
		fn(nil)
	}
}

// undoRec 记录一次版本压入，供回滚、语句回滚、保存点使用。
type undoRec struct {
	tbl  *table
	pk   []byte
	head *rowHead
	ver  *rowVer
}

type ReadView struct {
	snap uint64
	me   *Txn
}

type Txn struct {
	e        *Engine
	id       uint64
	commitTS atomic.Uint64 // 0 = 未提交
	viewSnap atomic.Int64  // 当前视图的快照号，-1 = 还没有视图（供 purge 计算最老视图）
	view     *ReadView
	iso      int
	timeout  time.Duration
	undo     []undoRec
	stmtBeg  int
	inStmt   bool        // 正在一条（顶层）语句之中：触发器/存储函数里再打开别的表不算新语句
	killed   func() bool // 连接是否被 KILL（由 C++ 提供 thd_killed）；nil 表示不检查
	done     bool
	doneCh   chan struct{}            // 事务结束时关闭（供"等待事务结束"的等待者）
	gaps     map[*idxState][]*gapLock // 本事务登记的间隙锁（仅所有者访问）
	held     map[lockKey]struct{}     // 持有的行锁（仅所有者访问）

	blockedOn []*Txn // 正在等待的事务集合，由 lm.dmu 保护
}

type purgeItem struct {
	tbl *table
	pk  []byte
	ts  uint64
}

const nStripes = 256

type Engine struct {
	mu     sync.RWMutex // 只保护 tables 映射（打开/删除/重命名表），不在数据路径上
	tables map[string]*table

	tmu    sync.Mutex
	clock  uint64
	nextID uint64
	active map[*Txn]struct{}

	lm lockMgr

	ustripes [nStripes]sync.Mutex // 唯一二级索引键的"检查+发布"条纹锁

	pmu sync.Mutex
	pq  []purgeItem

	LockTimeout time.Duration
}

func NewEngine() *Engine {
	e := &Engine{
		tables:      map[string]*table{},
		active:      map[*Txn]struct{}{},
		LockTimeout: 50 * time.Second,
	}
	for i := range e.lm.shards {
		e.lm.shards[i].locks = map[lockKey]*rowLock{}
	}
	return e
}

// ---------------------------------------------------------------- 表管理

func (e *Engine) CreateTable(name string, nidx int) *table {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := newTable(name, nidx)
	e.tables[name] = t
	return t
}

// OpenTable 不存在则创建（mysqld 重启后数据字典里有表而内存里没有，按空表处理）。
func (e *Engine) OpenTable(name string, nidx int) *table {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.tables[name]
	if t == nil {
		t = newTable(name, nidx)
		e.tables[name] = t
	}
	return t
}

func (e *Engine) DropTable(name string) {
	e.mu.Lock()
	delete(e.tables, name)
	e.mu.Unlock()
}

func (e *Engine) RenameTable(from, to string) {
	e.mu.Lock()
	if t, ok := e.tables[from]; ok {
		t.name = to
		e.tables[to] = t
		delete(e.tables, from)
	}
	e.mu.Unlock()
}

func (e *Engine) Count(tb *table) int { return tb.pri.Len() }

// ---------------------------------------------------------------- 事务

func (e *Engine) Begin(iso int) *Txn {
	t := &Txn{e: e, iso: iso, timeout: e.LockTimeout, held: map[lockKey]struct{}{}, doneCh: make(chan struct{})}
	t.viewSnap.Store(-1)
	e.tmu.Lock()
	e.nextID++
	t.id = e.nextID
	e.active[t] = struct{}{}
	e.tmu.Unlock()
	return t
}

// StmtBegin 在一条语句打开它的表时调用（external_lock / start_stmt）。
// 只有这条**顶层语句的第一个表**才算语句开始：记录语句回滚点、（非 RR 时）丢弃上一条语句的视图。
// 触发器、存储函数在语句中途打开别的表时会再调一次，那不是新语句——重设语句起点会让失败时只回滚到较晚的起点，
// 前面的修改残留在事务里；重置 READ COMMITTED 的视图则会让同一条语句读到两份快照。
// 语句结束由 StmtEnd 标记（MySQL 的 hton->commit/rollback 的 all=false 钩子，每条顶层语句结束都会调用）。
func (t *Txn) StmtBegin(iso int) {
	t.iso = iso
	if t.inStmt {
		return
	}
	t.inStmt = true
	t.stmtBeg = len(t.undo)
	if iso != isoRepeatableRead && iso != isoSerializable {
		t.view = nil
		t.viewSnap.Store(-1)
	}
}

// StmtEnd 标记一条顶层语句结束；下一次 StmtBegin 才会开始新语句。
func (t *Txn) StmtEnd() { t.inStmt = false }

// SetKillCheck 设置"连接是否被 KILL"的检查函数，所有锁等待会定期调用它。
func (t *Txn) SetKillCheck(f func() bool) { t.killed = f }

// waitOn 等待 ch 被关闭（被唤醒），最长到 deadline；等待期间每 killPoll 检查一次是否被 KILL。
// 返回 nil 表示被唤醒，ErrLockTimeout 表示超时，ErrKilled 表示连接被 KILL。
func (t *Txn) waitOn(ch <-chan struct{}, deadline time.Time) error {
	for {
		if t.killed != nil && t.killed() {
			return ErrKilled
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrLockTimeout
		}
		d := remaining
		if t.killed != nil && d > killPoll {
			d = killPoll
		}
		timer := time.NewTimer(d)
		select {
		case <-ch:
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// readView 惰性创建视图：RR 在事务内第一次一致性读时创建并保持到事务结束。
// 只取 tmu（叶子锁），可以在任何地方调用。
func (t *Txn) readView() *ReadView {
	if t.view != nil {
		return t.view
	}
	t.e.tmu.Lock()
	snap := t.e.clock
	t.viewSnap.Store(int64(snap))
	t.e.tmu.Unlock()
	t.view = &ReadView{snap: snap, me: t}
	return t.view
}

func (v *rowVer) visibleTo(rv *ReadView) bool {
	if v.trx == rv.me {
		return true
	}
	ts := v.trx.commitTS.Load()
	return ts != 0 && ts <= rv.snap
}

// visible 返回链上第一个对视图可见的版本。调用者须持有该链所在叶子的锁。
func (h *rowHead) visible(rv *ReadView) *rowVer {
	for v := h.head; v != nil; v = v.prev {
		if v.visibleTo(rv) {
			return v
		}
	}
	return nil
}

func (e *Engine) Commit(t *Txn) {
	if t.done {
		return
	}
	t.done = true
	e.tmu.Lock()
	e.clock++
	ts := e.clock
	t.commitTS.Store(ts)
	delete(e.active, t)
	e.tmu.Unlock()

	if len(t.undo) > 0 {
		e.pmu.Lock()
		for _, u := range t.undo {
			e.pq = append(e.pq, purgeItem{u.tbl, u.pk, ts})
		}
		e.pmu.Unlock()
	}
	t.undo = nil
	e.lm.releaseAll(t) // 先标记提交再放锁，等待者醒来就能看到已提交的版本
	t.releaseGaps()
	close(t.doneCh)
}

func (e *Engine) Rollback(t *Txn) {
	if t.done {
		return
	}
	t.done = true
	e.undoTo(t, 0) // 放锁之前撤销：此时仍持有这些行的 X 锁，没有别的写者
	e.tmu.Lock()
	delete(e.active, t)
	e.tmu.Unlock()
	e.lm.releaseAll(t)
	t.releaseGaps()
	close(t.doneCh)
}

// RollbackStmt 只撤销当前语句的修改，行锁保留（与 InnoDB 一致）。
func (e *Engine) RollbackStmt(t *Txn) { e.undoTo(t, t.stmtBeg) }

func (t *Txn) Savepoint() int { return len(t.undo) }

func (e *Engine) RollbackToSavepoint(t *Txn, sp int) {
	e.undoTo(t, sp)
	if t.stmtBeg > len(t.undo) {
		t.stmtBeg = len(t.undo)
	}
}

// undoTo 把撤销日志回退到长度 n。调用者持有这些行的 X 锁，所以没有并发写者（purge 也会因为拿不到 X 锁而跳过）。
func (e *Engine) undoTo(t *Txn, n int) {
	for len(t.undo) > n {
		u := t.undo[len(t.undo)-1]
		t.undo = t.undo[:len(t.undo)-1]
		var drop []secKey
		empty := false
		// 1. 在主索引叶子写锁内弹出版本，并算出哪些二级索引条目不再被链上任何版本引用
		u.tbl.pri.Modify(u.pk, func(v any) {
			h := v.(*rowHead)
			h.head = u.ver.prev
			for _, sk := range u.ver.sec {
				if !chainReferences(h, sk) {
					drop = append(drop, sk)
				}
			}
			empty = h.head == nil
		})
		// 2. 删除这些二级索引条目（读者此时即使还看到它们，也会因为"条目与可见版本不匹配"而跳过）
		for _, sk := range drop {
			u.tbl.sec[sk.idx].Delete(concat(sk.key, u.pk))
		}
		// 3. 链空了就删掉主索引里的键
		if empty {
			u.tbl.pri.DeleteIf(u.pk, func(v any) bool { return v.(*rowHead).head == nil })
		}
	}
}

func chainReferences(h *rowHead, sk secKey) bool {
	for v := h.head; v != nil; v = v.prev {
		for _, s := range v.sec {
			if s.idx == sk.idx && bytes.Equal(s.key, sk.key) {
				return true
			}
		}
	}
	return false
}

func concat(a, b []byte) []byte {
	r := make([]byte, 0, len(a)+len(b))
	return append(append(r, a...), b...)
}

func copySec(sec []secKey) []secKey {
	r := make([]secKey, len(sec))
	for i, s := range sec {
		r[i] = secKey{s.idx, append([]byte(nil), s.key...), s.unique}
	}
	return r
}

// pushOn 把新版本压到链头并记 undo。调用者须在该链所在叶子的写锁内（Upsert/Modify 的回调里）。
func (t *Txn) pushOn(tb *table, pk []byte, h *rowHead, v *rowVer) {
	v.prev = h.head
	h.head = v
	t.undo = append(t.undo, undoRec{tb, append([]byte(nil), pk...), h, v})
}

// ---------------------------------------------------------------- 行锁管理器（分片）

type lockKey struct {
	tbl *table
	pk  string
}

type waiter struct {
	ch  chan struct{}
	txn *Txn
}

type rowLock struct {
	x       *Txn
	s       map[*Txn]struct{}
	waiters []*waiter
}

const lmShards = 64

type lockShard struct {
	mu    sync.Mutex
	locks map[lockKey]*rowLock
}

// lockMgr 的锁表分成 64 个分片；等待图（Txn.blockedOn）由独立的小锁 dmu 保护，只在要阻塞或唤醒等待者时才取。
// 顺序：分片锁 -> dmu。
type lockMgr struct {
	shards [lmShards]lockShard
	dmu    sync.Mutex
}

func (lm *lockMgr) shard(key lockKey) *lockShard {
	h := uint32(2166136261)
	for i := 0; i < len(key.pk); i++ {
		h ^= uint32(key.pk[i])
		h *= 16777619
	}
	h ^= key.tbl.id * 2654435761
	h *= 16777619
	return &lm.shards[h%lmShards]
}

func (sh *lockShard) cleanup(key lockKey, rl *rowLock) {
	if rl.x == nil && len(rl.s) == 0 && len(rl.waiters) == 0 {
		delete(sh.locks, key)
	}
}

// lock 取行锁，必要时阻塞。调用者不得持有任何树锁、gapMu、条纹锁。
func (e *Engine) lock(t *Txn, tb *table, pk []byte, mode int) error {
	return e.lm.lock(t, tb, pk, mode)
}

func (lm *lockMgr) lock(t *Txn, tb *table, pk []byte, mode int) error {
	key := lockKey{tb, string(pk)}
	sh := lm.shard(key)
	var deadline time.Time
	waited := false
	for {
		sh.mu.Lock()
		rl := sh.locks[key]
		if rl == nil {
			rl = &rowLock{s: map[*Txn]struct{}{}}
			sh.locks[key] = rl
		}
		if rl.x == t {
			sh.mu.Unlock()
			lm.clearWait(t, waited)
			return nil
		}
		if mode == lockS {
			if _, ok := rl.s[t]; ok {
				sh.mu.Unlock()
				lm.clearWait(t, waited)
				return nil
			}
		}
		var blockers []*Txn
		if rl.x != nil {
			blockers = append(blockers, rl.x)
		}
		if mode == lockX {
			for h := range rl.s {
				if h != t {
					blockers = append(blockers, h)
				}
			}
		}
		if len(blockers) == 0 { // 授予
			if mode == lockX {
				rl.x = t
				delete(rl.s, t) // S -> X 升级
			} else {
				rl.s[t] = struct{}{}
			}
			t.held[key] = struct{}{}
			sh.mu.Unlock()
			lm.clearWait(t, waited)
			return nil
		}
		lm.dmu.Lock()
		t.blockedOn = blockers
		cyc := lm.deadlock(t)
		if cyc {
			t.blockedOn = nil
		}
		lm.dmu.Unlock()
		if cyc {
			sh.cleanup(key, rl)
			sh.mu.Unlock()
			return ErrDeadlock
		}
		w := &waiter{ch: make(chan struct{}), txn: t}
		rl.waiters = append(rl.waiters, w)
		sh.mu.Unlock()
		waited = true

		if deadline.IsZero() {
			deadline = time.Now().Add(t.timeout)
		}
		if err := t.waitOn(w.ch, deadline); err != nil {
			lm.abandon(t, key, w)
			return err
		}
	}
}

func (lm *lockMgr) clearWait(t *Txn, waited bool) {
	if waited {
		lm.dmu.Lock()
		t.blockedOn = nil
		lm.dmu.Unlock()
	}
}

// tryLock 不阻塞地尝试取行锁，取不到返回 false（purge 用）。
func (lm *lockMgr) tryLock(t *Txn, tb *table, pk []byte, mode int) bool {
	key := lockKey{tb, string(pk)}
	sh := lm.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	rl := sh.locks[key]
	if rl == nil {
		rl = &rowLock{s: map[*Txn]struct{}{}}
		sh.locks[key] = rl
	}
	if rl.x != nil && rl.x != t || mode == lockX && len(rl.s) > 0 {
		sh.cleanup(key, rl)
		return false
	}
	if mode == lockX {
		rl.x = t
	} else {
		rl.s[t] = struct{}{}
	}
	t.held[key] = struct{}{}
	return true
}

// abandon：等待超时，退出等待队列。
func (lm *lockMgr) abandon(t *Txn, key lockKey, w *waiter) {
	sh := lm.shard(key)
	sh.mu.Lock()
	if rl := sh.locks[key]; rl != nil {
		for i, x := range rl.waiters {
			if x == w {
				rl.waiters = append(rl.waiters[:i], rl.waiters[i+1:]...)
				break
			}
		}
		sh.cleanup(key, rl)
	}
	sh.mu.Unlock()
	lm.dmu.Lock()
	t.blockedOn = nil
	lm.dmu.Unlock()
}

// deadlock 沿"等待 -> 持有者"的边做 DFS，若能回到 t 就是死锁。调用者持有 lm.dmu。
func (lm *lockMgr) deadlock(t *Txn) bool {
	seen := map[*Txn]bool{}
	var dfs func(x *Txn) bool
	dfs = func(x *Txn) bool {
		for _, b := range x.blockedOn {
			if b == t {
				return true
			}
			if !seen[b] {
				seen[b] = true
				if dfs(b) {
					return true
				}
			}
		}
		return false
	}
	return dfs(t)
}

func (lm *lockMgr) releaseAll(t *Txn) {
	for key := range t.held {
		sh := lm.shard(key)
		sh.mu.Lock()
		if rl := sh.locks[key]; rl != nil {
			if rl.x == t {
				rl.x = nil
			}
			delete(rl.s, t)
			if len(rl.waiters) > 0 {
				lm.dmu.Lock()
				for _, w := range rl.waiters {
					w.txn.blockedOn = nil // 被唤醒者的等待边已过期，清掉，避免后续死锁检测误判
					close(w.ch)
				}
				lm.dmu.Unlock()
				rl.waiters = nil
			}
			sh.cleanup(key, rl)
		}
		sh.mu.Unlock()
	}
	clear(t.held)
}

// ---------------------------------------------------------------- 唯一键条纹锁

// lockUniqueStripes 对本次写入涉及的每个唯一二级索引键取条纹锁（按下标升序），返回释放函数。
// 同一唯一键的并发"检查+发布"因此被串行化，不同键互不影响。
func (e *Engine) lockUniqueStripes(tb *table, sec []secKey) func() {
	var idx []int
	for _, sk := range sec {
		if !sk.unique {
			continue
		}
		h := uint32(2166136261)
		for _, b := range sk.key {
			h ^= uint32(b)
			h *= 16777619
		}
		h ^= tb.id*2654435761 + uint32(sk.idx)*40503
		s := int(h % nStripes)
		dup := false
		for _, x := range idx {
			if x == s {
				dup = true
				break
			}
		}
		if !dup {
			idx = append(idx, s)
		}
	}
	if len(idx) == 0 {
		return func() {}
	}
	sort.Ints(idx)
	for _, s := range idx {
		e.ustripes[s].Lock()
	}
	return func() {
		for i := len(idx) - 1; i >= 0; i-- {
			e.ustripes[idx[i]].Unlock()
		}
	}
}

// ---------------------------------------------------------------- 写操作

// checkUnique 检查唯一二级索引。skip 是应忽略的主键（自己和被更新的旧行）。
// 返回 wait != nil 表示必须先等待该行上的另一个活跃事务结束，再重新检查。调用者已持有相关的条纹锁。
func (e *Engine) checkUnique(t *Txn, tb *table, skip [][]byte, sec []secKey) (wait []byte, err error) {
	for _, sk := range sec {
		if !sk.unique {
			continue
		}
		tb.sec[sk.idx].ScanPrefix(sk.key, func(entry []byte) bool {
			cpk := entry[len(sk.key):]
			for _, s := range skip {
				if bytes.Equal(s, cpk) {
					return true
				}
			}
			tb.withHead(cpk, func(h *rowHead) {
				if h == nil {
					return
				}
				v := h.head
				if v.trx != t && v.trx.commitTS.Load() == 0 {
					wait = cpk // 另一个活跃事务改过这一行：等它结束
					return
				}
				if v.deleted {
					return
				}
				for _, s := range v.sec {
					if s.idx == sk.idx && bytes.Equal(s.key, sk.key) {
						err = DupError{sk.idx}
						return
					}
				}
			})
			return wait == nil && err == nil
		})
		if wait != nil || err != nil {
			return wait, err
		}
	}
	return nil, nil
}

// putSecEntries 为新版本发布它的二级索引条目。
func putSecEntries(tb *table, pk []byte, v *rowVer) {
	for _, s := range v.sec {
		tb.sec[s.idx].Put(concat(s.key, pk), nil, true)
	}
}

func (e *Engine) Insert(t *Txn, tb *table, pk, row []byte, sec []secKey) error {
	if err := e.lm.lock(t, tb, pk, lockX); err != nil {
		return err
	}
	for {
		unlockU := e.lockUniqueStripes(tb, sec)
		live := false
		exists := tb.pri.GetFn(pk, func(v any) {
			if h := v.(*rowHead); h.head != nil && !h.head.deleted {
				live = true
			}
		})
		if live {
			unlockU()
			return DupError{0}
		}
		wait, err := e.checkUnique(t, tb, [][]byte{pk}, sec)
		if err != nil {
			unlockU()
			return err
		}
		if wait != nil {
			unlockU()
			if err := e.lm.lock(t, tb, wait, lockS); err != nil {
				return err
			}
			continue
		}
		// 插入意向：新条目落在别的事务的间隙锁里就必须等它结束；检查和发布在同一把 gapMu 读锁内完成
		entries := newEntries(tb, pk, exists, sec)
		unlockIdx := tb.lockIdxForPublish(entries)
		if holder := tb.gapHolder(t, entries); holder != nil {
			unlockIdx()
			unlockU()
			if err := e.lm.waitTxn(t, holder); err != nil {
				return err
			}
			continue
		}
		v := &rowVer{trx: t, data: append([]byte(nil), row...), sec: copySec(sec)}
		dup := false
		tb.pri.Upsert(pk, func(old any, ex bool) (any, bool) {
			if ex {
				h := old.(*rowHead)
				if h.head != nil && !h.head.deleted {
					dup = true
					return nil, false
				}
				t.pushOn(tb, pk, h, v)
				return nil, false
			}
			h := &rowHead{}
			t.pushOn(tb, pk, h, v)
			return h, true
		})
		if dup {
			unlockIdx()
			unlockU()
			return DupError{0}
		}
		putSecEntries(tb, pk, v)
		unlockIdx()
		unlockU()
		return nil
	}
}

// Update 把 oldpk 的行改成 row；newpk 与 oldpk 不同表示主键被修改。
func (e *Engine) Update(t *Txn, tb *table, oldpk, newpk, row []byte, sec []secKey) error {
	same := bytes.Equal(oldpk, newpk)
	if err := e.lm.lock(t, tb, oldpk, lockX); err != nil {
		return err
	}
	if !same {
		if err := e.lm.lock(t, tb, newpk, lockX); err != nil {
			return err
		}
	}
	for {
		unlockU := e.lockUniqueStripes(tb, sec)
		oldLive := false
		tb.withHead(oldpk, func(h *rowHead) { oldLive = h != nil && !h.head.deleted })
		if !oldLive {
			unlockU()
			return ErrNotFound
		}
		newExists := false
		if !same {
			newLive := false
			newExists = tb.pri.GetFn(newpk, func(v any) {
				if h := v.(*rowHead); h.head != nil && !h.head.deleted {
					newLive = true
				}
			})
			if newLive {
				unlockU()
				return DupError{0}
			}
		}
		wait, err := e.checkUnique(t, tb, [][]byte{oldpk, newpk}, sec)
		if err != nil {
			unlockU()
			return err
		}
		if wait != nil {
			unlockU()
			if err := e.lm.lock(t, tb, wait, lockS); err != nil {
				return err
			}
			continue
		}
		// 插入意向：主键不变时只有"新出现的二级索引条目"算插入；主键变了则新主键和它的二级索引条目都算
		var entries []gapEntry
		target := oldpk
		if same {
			entries = newEntries(tb, oldpk, true, sec)
		} else {
			target = newpk
			entries = newEntries(tb, newpk, newExists, sec)
		}
		unlockIdx := tb.lockIdxForPublish(entries)
		if holder := tb.gapHolder(t, entries); holder != nil {
			unlockIdx()
			unlockU()
			if err := e.lm.waitTxn(t, holder); err != nil {
				return err
			}
			continue
		}
		nv := &rowVer{trx: t, data: append([]byte(nil), row...), sec: copySec(sec)}
		if same {
			tb.pri.Modify(oldpk, func(v any) { t.pushOn(tb, oldpk, v.(*rowHead), nv) })
		} else {
			tb.pri.Modify(oldpk, func(v any) { t.pushOn(tb, oldpk, v.(*rowHead), &rowVer{trx: t, deleted: true}) })
			tb.pri.Upsert(newpk, func(old any, ex bool) (any, bool) {
				if ex {
					t.pushOn(tb, newpk, old.(*rowHead), nv)
					return nil, false
				}
				h := &rowHead{}
				t.pushOn(tb, newpk, h, nv)
				return h, true
			})
		}
		putSecEntries(tb, target, nv)
		unlockIdx()
		unlockU()
		return nil
	}
}

func (e *Engine) Delete(t *Txn, tb *table, pk []byte) error {
	if err := e.lm.lock(t, tb, pk, lockX); err != nil {
		return err
	}
	tb.pri.Modify(pk, func(v any) {
		h := v.(*rowHead)
		if h.head == nil || h.head.deleted {
			return
		}
		t.pushOn(tb, pk, h, &rowVer{trx: t, deleted: true})
	})
	return nil
}

// ---------------------------------------------------------------- 读操作

// GetRow 按主键读一行。lock == lockNone 为一致性读（快照），否则先加锁再读最新版本（当前读）。
func (e *Engine) GetRow(t *Txn, tb *table, pk []byte, lock int) ([]byte, error) {
	var data []byte
	ok := false
	if lock == lockNone {
		rv := t.readView()
		tb.withHead(pk, func(h *rowHead) {
			if h == nil {
				return
			}
			if v := h.visible(rv); v != nil && !v.deleted {
				data, ok = v.data, true
			}
		})
	} else {
		if err := e.lm.lock(t, tb, pk, lock); err != nil {
			return nil, err
		}
		tb.withHead(pk, func(h *rowHead) {
			if h != nil && !h.head.deleted {
				data, ok = h.head.data, true
			}
		})
	}
	if !ok {
		return nil, ErrNotFound
	}
	return data, nil
}

// Cursor 是带事务语义的游标：只返回对当前事务可见（或已加锁的最新）的行。
type Cursor struct {
	e       *Engine
	t       *Txn
	tbl     *table
	idx     int
	lock    int
	pkWidth int // 主键编码宽度；二级索引条目末尾的 pkWidth 字节就是主键
	raw     rawCursor
	ix      *idxState

	// 一致性读主索引时，在定位回调（叶子读锁内）里顺便解析可见版本，省掉第二次下探
	snapRV *ReadView
	resRow []byte
	resOK  bool

	// 一致性读的批量预取缓冲：一次叶子读锁内取后面最多 16 个条目（主索引：同时解析可见版本；二级索引：只取键），后续 Step 直接从缓冲取，不碰树锁。
	// 对快照读是安全的：我的视图下每个键能看到的版本是固定的，之后别人的修改对我本来就不可见。
	buf     []batchEnt
	bufPos  int
	bufDir  int
	batchFn func(k []byte, v any)
	batchN  int // 下一次预取的大小：从 2 倍增到 16，避免 LIMIT 1 之类的短扫描白白多解析

	// 扫描边界（见 gap.go）
	bKind      int // 外部设置的边界（SetBound）
	bKey       []byte
	bIncl      bool
	bUniq      bool   // 外部终点边界是"唯一索引的全键"：扫到恰好等于终点的记录就结束（InnoDB 的做法）
	done       bool   // 已命中唯一终点，下一次 Step 直接返回结束
	autoPrefix []byte // Seek 在精确/前缀模式下自动设置的同键前缀边界；下一次非精确 Seek 会清掉，不影响外部边界
}

type batchEnt struct {
	key []byte
	row []byte // nil 表示对当前视图没有可见的、未删除的版本（真实的行数据永远非空）
}

func (e *Engine) OpenCursor(t *Txn, tb *table, idx, lock, pkWidth int) *Cursor {
	c := &Cursor{e: e, t: t, tbl: tb, idx: idx, lock: lock, pkWidth: pkWidth, raw: rawCursor{t: tb.tree(idx)}, ix: tb.ix[idx], batchN: 2}
	if idx == 0 && lock == lockNone {
		c.raw.hook = c.snapshotHook
	}
	return c
}

// batchVisit 在叶子读锁内被调用：解析一个条目对当前视图的可见版本，放进缓冲。
func (c *Cursor) batchVisit(k []byte, val any) {
	e := batchEnt{key: k}
	if c.idx != 0 { // 二级索引：只批量取键；可见版本要回主索引解析，不能在二级树的叶子锁内做
		c.buf = append(c.buf, e)
		return
	}
	if h := val.(*rowHead); h.head != nil {
		if v := h.visible(c.snapRV); v != nil && !v.deleted {
			e.row = v.data
		}
	}
	c.buf = append(c.buf, e)
}

// pop 从缓冲取下一个条目，作为当前候选（二级索引的 row 为 nil，由调用者回主索引解析）。
func (c *Cursor) pop() bool {
	e := c.buf[c.bufPos]
	c.bufPos++
	c.raw.key, c.raw.valid = e.key, true
	c.resRow, c.resOK = e.row, e.row != nil
	return true
}

// nextCandidate 沿 dir 取下一个候选。一致性读（主索引和二级索引）走批量预取缓冲，加锁读逐条取。
func (c *Cursor) nextCandidate(dir int) bool {
	if c.lock != lockNone {
		return c.raw.step(dir)
	}
	if c.bufPos < len(c.buf) {
		if c.bufDir == dir {
			return c.pop()
		}
		// 方向变了：缓冲作废；游标的位置停在批末而不是当前返回的键，所以让位置失效，改按当前键重新下探
		c.raw.loc = Loc{}
	}
	if c.buf == nil { // 一次分配到位，避免 2→4→8→16 的逐次扩容
		c.buf = make([]batchEnt, 0, 16)
		c.batchFn = c.batchVisit
	}
	c.buf, c.bufPos, c.bufDir = c.buf[:0], 0, dir
	n := c.raw.stepBatch(dir, c.batchN, c.batchFn)
	if c.batchN < 16 {
		c.batchN *= 2
	}
	if n == 0 {
		return false
	}
	return c.pop()
}

// snapshotHook 在叶子读锁内被调用：对主索引候选解析对当前视图可见的版本。
func (c *Cursor) snapshotHook(_ []byte, val any) {
	if h := val.(*rowHead); h.head != nil {
		if v := h.visible(c.snapRV); v != nil && !v.deleted {
			c.resRow, c.resOK = v.data, true
		}
	}
}

func (c *Cursor) pk() []byte {
	if c.idx == 0 {
		return c.raw.key
	}
	k := c.raw.key
	return k[len(k)-c.pkWidth:]
}

// entryMatches：二级索引条目是否属于版本 v（条目键去掉主键后缀 == v 在该索引下的键）。
func (c *Cursor) entryMatches(v *rowVer) bool {
	if c.idx == 0 {
		return true
	}
	k := c.raw.key
	prefix := k[:len(k)-c.pkWidth]
	for _, s := range v.sec {
		if s.idx == c.idx {
			return bytes.Equal(s.key, prefix)
		}
	}
	return false
}

// snapshotSecondary 一致性读：二级索引候选回主索引取对当前视图可见的版本，并核对条目属于该版本。
func (c *Cursor) snapshotSecondary(rv *ReadView) (data []byte, ok bool) {
	c.tbl.withHead(c.pk(), func(h *rowHead) {
		if h == nil {
			return
		}
		if v := h.visible(rv); v != nil && !v.deleted && c.entryMatches(v) {
			data, ok = v.data, true
		}
	})
	return
}

// currentRow 当前读（已持有行锁）：读链头。
func (c *Cursor) currentRow(pk []byte) (data []byte, ok bool) {
	c.tbl.withHead(pk, func(h *rowHead) {
		if h != nil && !h.head.deleted && c.entryMatches(h.head) {
			data, ok = h.head.data, true
		}
	})
	return
}

// 唯一索引全键（flags 位）：精确查找，或范围以 >= K 开始；K 恰好存在时只加记录锁，不登记间隙。
const seekUniqueFull = 1

// Seek 定位并返回第一个符合条件的行；Step 沿 dir 方向取下一行。
func (c *Cursor) Seek(key []byte, mode int) ([]byte, error) { return c.advance(true, key, mode, 0, 0) }
func (c *Cursor) SeekFlags(key []byte, mode, flags int) ([]byte, error) {
	return c.advance(true, key, mode, 0, flags)
}
func (c *Cursor) Step(dir int) ([]byte, error) { return c.advance(false, nil, 0, dir, 0) }

func (c *Cursor) advance(isSeek bool, key []byte, mode, dir, flags int) ([]byte, error) {
	e := c.e
	gap := c.gapMode()
	firstUniq := false
	if isSeek {
		dir = seekDir(mode)
		if seekNeedsPrefix(mode) {
			// 同键扫描：之后的 Step（index_next_same）遇到不再匹配的条目就停止
			c.autoPrefix = append([]byte(nil), key...)
		} else {
			c.autoPrefix = nil
		}
		// 唯一索引全键：精确查找，或范围以 >= K 开始（K 恰好存在时都只加记录锁，不锁它前面的间隙）
		firstUniq = flags&seekUniqueFull != 0 && (mode == seekExact || mode == seekGE)
		c.done = false
		c.buf, c.bufPos, c.batchN = c.buf[:0], 0, 2
	} else if c.done {
		c.raw.valid = false
		return nil, ErrNotFound
	}
	for {
		var rv *ReadView
		if c.lock == lockNone {
			rv = c.t.readView()
			c.snapRV = rv
			c.resRow, c.resOK = nil, false
		}
		if gap {
			c.ix.gapMu.Lock() // 登记者：与"检查间隙冲突 + 发布条目"的插入者互斥
		}
		var ok bool
		isFirstPos := isSeek // 本次是不是扫描的起点（反向扫描的起点要多锁一段）
		if isSeek {
			// 精确/前缀模式在内部用 GE/LE 定位：未命中时也要保留"第一个 >= 查找键"的候选，
			// 才能登记它前面的间隙；是否命中由前缀边界（beyond）判断，效果与原来等价。
			rawMode := mode
			switch mode {
			case seekExact:
				rawMode = seekGE
			case seekPrefixLast:
				rawMode = seekLE
			}
			ok = c.raw.seek(key, rawMode)
			isSeek = false
		} else {
			ok = c.nextCandidate(dir)
		}
		inRange := ok && !c.beyond(c.raw.key)                          // 候选存在且没有越过扫描边界
		hit := firstUniq && inRange && bytes.HasPrefix(c.raw.key, key) // 唯一全键恰好命中
		if gap {
			// 唯一全键等值命中：先不登记间隙，等拿到行锁确认行存在后才算（命中且存在 = 只有记录锁）。
			// 其余情形：候选在范围内 = next-key（登记它前面的间隙，行锁随后加）；越界/索引末尾 = 只登记间隙，不锁该行。
			if !hit {
				c.registerGap(dir, ok, isFirstPos)
			}
			c.ix.gapMu.Unlock()
		}
		if !inRange {
			c.raw.valid = false
			return nil, ErrNotFound
		}
		if c.lock == lockNone {
			var row []byte
			good := false
			if c.idx == 0 {
				row, good = c.resRow, c.resOK
			} else {
				row, good = c.snapshotSecondary(rv)
			}
			if good {
				c.noteReturned()
				return row, nil
			}
			continue
		}
		pk := c.pk()
		deadKey := c.raw.key
		// 加锁读：此刻没有持有任何树锁/gapMu，可以安全地等行锁；醒来后树可能变了，游标会按 key 重新定位
		if err := e.lm.lock(c.t, c.tbl, pk, c.lock); err != nil {
			return nil, err
		}
		row, ok2 := c.currentRow(pk)
		if ok2 {
			c.noteReturned()
			return row, nil
		}
		if hit && gap {
			// 唯一键命中的那一行其实已被删除：按"未命中"处理，补登记它前面的间隙，之后按普通同键扫描继续
			c.ix.gapMu.Lock()
			if c.raw.seek(deadKey, seekGE) && bytes.Equal(c.raw.key, deadKey) {
				c.registerGap(1, true, false)
			}
			c.ix.gapMu.Unlock()
		}
		firstUniq = false
	}
}

// ---------------------------------------------------------------- 自增

// AutoReserve 预留 n 个自增值（值之间相差 increment，起点对齐到 offset），返回第一个值。
// 计数器还没初始化时返回 ok=false，调用者应先 AutoInit。
func (tb *table) AutoReserve(offset, increment, n uint64) (first uint64, ok bool) {
	tb.amu.Lock()
	defer tb.amu.Unlock()
	if !tb.autoInit {
		return 0, false
	}
	if increment == 0 {
		increment = 1
	}
	next := tb.auto + 1
	if increment > 1 || offset > 1 {
		if next < offset {
			next = offset
		} else {
			next = offset + ((next-offset+increment-1)/increment)*increment
		}
	}
	if n == 0 {
		n = 1
	}
	tb.auto = next + (n-1)*increment
	return next, true
}

func (tb *table) AutoInit(max uint64) {
	tb.amu.Lock()
	if !tb.autoInit {
		tb.auto, tb.autoInit = max, true
	}
	tb.amu.Unlock()
}

// AutoBump 在写入了更大的显式自增值后抬高计数器。
func (tb *table) AutoBump(v uint64) {
	tb.amu.Lock()
	if tb.autoInit && v > tb.auto {
		tb.auto = v
	}
	tb.amu.Unlock()
}

// ---------------------------------------------------------------- purge

// oldestSnap 返回所有活跃视图中最老的快照号（没有视图的事务以后创建的视图不会更老）。
func (e *Engine) oldestSnap() uint64 {
	e.tmu.Lock()
	defer e.tmu.Unlock()
	oldest := e.clock
	for t := range e.active {
		if s := t.viewSnap.Load(); s >= 0 && uint64(s) < oldest {
			oldest = uint64(s)
		}
	}
	return oldest
}

// purgeSome 最多处理 budget 个待清理项，返回处理个数。
// 每一项先**尝试**取该行的 X 锁（拿不到就留到下次）：这保证 purge 不会和并发写者、回滚、
// 正在读它的加锁读者竞争，也就不再需要任何全局锁。
func (e *Engine) purgeSome(budget int) int {
	oldest := e.oldestSnap()
	e.pmu.Lock()
	n := 0
	for n < len(e.pq) && n < budget && e.pq[n].ts <= oldest {
		n++
	}
	items := append([]purgeItem(nil), e.pq[:n]...)
	e.pq = e.pq[n:]
	e.pmu.Unlock()
	if n == 0 {
		return 0
	}
	pt := &Txn{e: e, held: map[lockKey]struct{}{}}
	var retry []purgeItem
	for _, it := range items {
		if !e.lm.tryLock(pt, it.tbl, it.pk, lockX) {
			retry = append(retry, it)
			continue
		}
		e.pruneChain(it.tbl, it.pk, oldest)
		e.lm.releaseAll(pt)
	}
	if len(retry) > 0 {
		e.pmu.Lock()
		e.pq = append(e.pq, retry...)
		e.pmu.Unlock()
	}
	return n - len(retry)
}

// purgeAll 反复清理直到没有可清理的项（测试用）。
func (e *Engine) purgeAll() {
	for e.purgeSome(1<<20) > 0 {
	}
}

// pruneChain 丢弃对所有活跃视图都不再可见的旧版本。调用者持有该行的 X 锁。
func (e *Engine) pruneChain(tb *table, pk []byte, oldest uint64) {
	var del []secKey
	removeRow := false
	tb.pri.Modify(pk, func(val any) {
		h := val.(*rowHead)
		var keep *rowVer
		for v := h.head; v != nil; v = v.prev {
			if ts := v.trx.commitTS.Load(); ts != 0 && ts <= oldest {
				keep = v
				break
			}
		}
		if keep == nil {
			return
		}
		dropped := keep.prev
		keep.prev = nil
		removeRow = keep == h.head && keep.deleted // 只剩一个对所有人都已生效的墓碑：整行可以删掉
		for d := dropped; d != nil; d = d.prev {
			for _, sk := range d.sec {
				if removeRow || !chainReferences(h, sk) {
					del = append(del, sk)
				}
			}
		}
	})
	for _, sk := range del {
		tb.sec[sk.idx].Delete(concat(sk.key, pk))
	}
	if removeRow {
		tb.pri.DeleteIf(pk, func(v any) bool {
			h := v.(*rowHead)
			return h.head != nil && h.head.deleted && h.head.prev == nil
		})
	}
}

// StartPurger 启动后台清理协程。
func (e *Engine) StartPurger(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				e.purgeSome(5000)
			}
		}
	}()
	return func() { close(done) }
}

// LastLatest 返回索引 idx 上最大键对应的、最新且未删除的行（不管事务可见性）。
func (e *Engine) LastLatest(tb *table, idx, pkWidth int) ([]byte, bool) {
	c := &Cursor{e: e, tbl: tb, idx: idx, pkWidth: pkWidth, raw: rawCursor{t: tb.tree(idx)}}
	for ok := c.raw.seek(nil, seekLast); ok; ok = c.raw.step(-1) {
		if row, ok2 := c.currentRow(c.pk()); ok2 {
			return row, true
		}
	}
	return nil, false
}

// RecordsInRange 估算索引 idx 上落在 [lo, hi] 内的条目数（最多数 limit 个）。lo/hi 可以是键的前缀。
// 只用于优化器估算：不区分可见性，包含已删除/未提交的条目。
func (e *Engine) RecordsInRange(tb *table, idx int, lo []byte, hasLo, loIncl bool, hi []byte, hasHi, hiIncl bool, limit int) (n int, truncated bool) {
	c := &Cursor{e: e, tbl: tb, idx: idx, raw: rawCursor{t: tb.tree(idx)}}
	var ok bool
	switch {
	case !hasLo:
		ok = c.raw.seek(nil, seekFirst)
	case loIncl:
		ok = c.raw.seek(lo, seekGE)
	default:
		ok = c.raw.seek(lo, seekAfter)
	}
	if hasHi {
		c.SetBound(boundEndFwd, hi, hiIncl)
	}
	for ; ok && !c.beyond(c.raw.key); ok = c.raw.step(1) {
		if n++; n >= limit {
			return n, true
		}
	}
	return n, false
}
