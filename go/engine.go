package main

// 事务、多版本、行锁。纯 Go，不依赖 cgo，可直接 `go test -race`。设计见 docs/plan-mvcc.md。
//
// 锁顺序（必须遵守，否则会死锁）：
//
//	行锁(lm) —— 先取行锁，且取行锁时不得持有 e.mu
//	e.mu     —— 保护所有 B+ 树的结构和版本链
//	e.tmu    —— 保护 clock / active / 各事务的视图快照；永远在 e.mu 之内取，不反过来
//	lm.mu    —— 只保护锁表，内部不再取其它锁
import (
	"bytes"
	"errors"
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
)

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

// rowVer 是行的一个版本。字段创建后不再修改（prev 在 purge 时除外，且只在写锁下）。
type rowVer struct {
	trx     *Txn
	deleted bool // 墓碑
	data    []byte
	sec     []secKey
	prev    *rowVer
}

// rowHead 是主索引的值：版本链头（最新版本）。
type rowHead struct{ head *rowVer }

type table struct {
	name string
	pri  *BTree   // 主键编码 -> *rowHead
	sec  []*BTree // 下标 = 索引号，[0] 不用；条目键 = 二级索引键 + 主键，值为 nil

	amu      sync.Mutex
	auto     uint64
	autoInit bool
}

func newTable(name string, nidx int) *table {
	t := &table{name: name, pri: NewBTree()}
	t.sec = make([]*BTree, nidx)
	for i := 1; i < nidx; i++ {
		t.sec[i] = NewBTree()
	}
	return t
}

func (tb *table) head(pk []byte) *rowHead {
	v, ok := tb.pri.Get(pk)
	if !ok {
		return nil
	}
	return v.(*rowHead)
}

func (tb *table) tree(idx int) *BTree {
	if idx == 0 {
		return tb.pri
	}
	return tb.sec[idx]
}

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
	done     bool

	// 以下两个字段由 lm.mu 保护
	held      map[lockKey]struct{}
	blockedOn []*Txn
}

type purgeItem struct {
	tbl *table
	pk  []byte
	ts  uint64
}

type Engine struct {
	mu     sync.RWMutex
	tables map[string]*table

	tmu    sync.Mutex
	clock  uint64
	nextID uint64
	active map[*Txn]struct{}

	lm lockMgr

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
	e.lm.locks = map[lockKey]*rowLock{}
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

func (e *Engine) Count(tb *table) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return tb.pri.Len()
}

// ---------------------------------------------------------------- 事务

func (e *Engine) Begin(iso int) *Txn {
	t := &Txn{e: e, iso: iso, timeout: e.LockTimeout, held: map[lockKey]struct{}{}}
	t.viewSnap.Store(-1)
	e.tmu.Lock()
	e.nextID++
	t.id = e.nextID
	e.active[t] = struct{}{}
	e.tmu.Unlock()
	return t
}

// StmtBegin 在每条语句开始时调用：记录语句回滚点；非 RR 隔离级别下丢弃上一条语句的视图。
func (t *Txn) StmtBegin(iso int) {
	t.iso = iso
	t.stmtBeg = len(t.undo)
	if iso != isoRepeatableRead && iso != isoSerializable {
		t.view = nil
		t.viewSnap.Store(-1)
	}
}

// readView 惰性创建视图：RR 在事务内第一次一致性读时创建并保持到事务结束。
// 调用者须持有 e.mu（读锁或写锁）。
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

// visible 返回链上第一个对视图可见的版本。
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
}

func (e *Engine) Rollback(t *Txn) {
	if t.done {
		return
	}
	t.done = true
	e.mu.Lock()
	e.undoTo(t, 0)
	e.mu.Unlock()
	e.tmu.Lock()
	delete(e.active, t)
	e.tmu.Unlock()
	e.lm.releaseAll(t)
}

// RollbackStmt 只撤销当前语句的修改，行锁保留（与 InnoDB 一致）。
func (e *Engine) RollbackStmt(t *Txn) {
	e.mu.Lock()
	e.undoTo(t, t.stmtBeg)
	e.mu.Unlock()
}

func (t *Txn) Savepoint() int { return len(t.undo) }

func (e *Engine) RollbackToSavepoint(t *Txn, sp int) {
	e.mu.Lock()
	e.undoTo(t, sp)
	e.mu.Unlock()
	if t.stmtBeg > len(t.undo) {
		t.stmtBeg = len(t.undo)
	}
}

// undoTo 把撤销日志回退到长度 n。调用者须持有 e.mu 写锁。
func (e *Engine) undoTo(t *Txn, n int) {
	for len(t.undo) > n {
		u := t.undo[len(t.undo)-1]
		t.undo = t.undo[:len(t.undo)-1]
		// 持有行 X 锁期间链头一定是自己最新压入的版本
		u.head.head = u.ver.prev
		for _, sk := range u.ver.sec {
			if !chainReferences(u.head, sk) {
				u.tbl.sec[sk.idx].Delete(concat(sk.key, u.pk))
			}
		}
		if u.head.head == nil {
			u.tbl.pri.Delete(u.pk)
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

// ---------------------------------------------------------------- 行锁管理器

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

type lockMgr struct {
	mu    sync.Mutex
	locks map[lockKey]*rowLock
}

// lock 取行锁，必要时阻塞。调用者不得持有 e.mu。
func (e *Engine) lock(t *Txn, tb *table, pk []byte, mode int) error {
	return e.lm.lock(t, tb, pk, mode)
}

func (lm *lockMgr) lock(t *Txn, tb *table, pk []byte, mode int) error {
	key := lockKey{tb, string(pk)}
	deadline := time.Now().Add(t.timeout)
	for {
		lm.mu.Lock()
		rl := lm.locks[key]
		if rl == nil {
			rl = &rowLock{s: map[*Txn]struct{}{}}
			lm.locks[key] = rl
		}
		if rl.x == t {
			lm.mu.Unlock()
			return nil
		}
		if mode == lockS {
			if _, ok := rl.s[t]; ok {
				lm.mu.Unlock()
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
			t.blockedOn = nil
			lm.mu.Unlock()
			return nil
		}
		t.blockedOn = blockers
		if lm.deadlock(t) {
			t.blockedOn = nil
			lm.cleanup(key, rl)
			lm.mu.Unlock()
			return ErrDeadlock
		}
		w := &waiter{ch: make(chan struct{}), txn: t}
		rl.waiters = append(rl.waiters, w)
		lm.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			lm.abandon(t, key, w)
			return ErrLockTimeout
		}
		timer := time.NewTimer(remaining)
		select {
		case <-w.ch:
			timer.Stop()
		case <-timer.C:
			lm.abandon(t, key, w)
			return ErrLockTimeout
		}
	}
}

// abandon：等待超时，退出等待队列。
func (lm *lockMgr) abandon(t *Txn, key lockKey, w *waiter) {
	lm.mu.Lock()
	t.blockedOn = nil
	if rl := lm.locks[key]; rl != nil {
		for i, x := range rl.waiters {
			if x == w {
				rl.waiters = append(rl.waiters[:i], rl.waiters[i+1:]...)
				break
			}
		}
		lm.cleanup(key, rl)
	}
	lm.mu.Unlock()
}

func (lm *lockMgr) cleanup(key lockKey, rl *rowLock) {
	if rl.x == nil && len(rl.s) == 0 && len(rl.waiters) == 0 {
		delete(lm.locks, key)
	}
}

// deadlock 沿"等待 -> 持有者"的边做 DFS，若能回到 t 就是死锁。调用者持有 lm.mu。
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
	lm.mu.Lock()
	for key := range t.held {
		rl := lm.locks[key]
		if rl == nil {
			continue
		}
		if rl.x == t {
			rl.x = nil
		}
		delete(rl.s, t)
		for _, w := range rl.waiters {
			w.txn.blockedOn = nil // 被唤醒者的等待边已过期，清掉，避免后续死锁检测误判
			close(w.ch)
		}
		rl.waiters = nil
		lm.cleanup(key, rl)
	}
	t.held = map[lockKey]struct{}{}
	t.blockedOn = nil
	lm.mu.Unlock()
}

// ---------------------------------------------------------------- 写操作

// checkUnique 检查唯一二级索引。skip 是应忽略的主键（自己和被更新的旧行）。
// 返回 wait != nil 表示必须先等待该行上的另一个活跃事务结束，再重新检查。调用者持有 e.mu 写锁。
func (e *Engine) checkUnique(t *Txn, tb *table, skip [][]byte, sec []secKey) (wait []byte, err error) {
	for _, sk := range sec {
		if !sk.unique {
			continue
		}
		for p := tb.sec[sk.idx].LowerBound(sk.key); p.valid() && bytes.HasPrefix(p.key(), sk.key); p = p.Next() {
			cpk := p.key()[len(sk.key):]
			skipped := false
			for _, s := range skip {
				if bytes.Equal(s, cpk) {
					skipped = true
					break
				}
			}
			if skipped {
				continue
			}
			h := tb.head(cpk)
			if h == nil {
				continue
			}
			v := h.head
			if v.trx != t && v.trx.commitTS.Load() == 0 {
				return cpk, nil // 另一个活跃事务改过这一行：等它结束
			}
			if v.deleted {
				continue
			}
			for _, s := range v.sec {
				if s.idx == sk.idx && bytes.Equal(s.key, sk.key) {
					return nil, DupError{sk.idx}
				}
			}
		}
	}
	return nil, nil
}

// pushVersion 把新版本压到链头（h 为 nil 则新建链）。调用者持有 e.mu 写锁。
func (e *Engine) pushVersion(t *Txn, tb *table, pk []byte, h *rowHead, v *rowVer) {
	if h == nil {
		h = &rowHead{head: v}
		tb.pri.Put(pk, h, false)
	} else {
		v.prev = h.head
		h.head = v
	}
	t.undo = append(t.undo, undoRec{tb, append([]byte(nil), pk...), h, v})
	for _, s := range v.sec {
		tb.sec[s.idx].Put(concat(s.key, pk), nil, true)
	}
}

func (e *Engine) Insert(t *Txn, tb *table, pk, row []byte, sec []secKey) error {
	if err := e.lm.lock(t, tb, pk, lockX); err != nil {
		return err
	}
	for {
		e.mu.Lock()
		h := tb.head(pk)
		if h != nil && !h.head.deleted {
			e.mu.Unlock()
			return DupError{0}
		}
		wait, err := e.checkUnique(t, tb, [][]byte{pk}, sec)
		if err != nil {
			e.mu.Unlock()
			return err
		}
		if wait != nil {
			e.mu.Unlock()
			if err := e.lm.lock(t, tb, wait, lockS); err != nil {
				return err
			}
			continue
		}
		e.pushVersion(t, tb, pk, h, &rowVer{trx: t, data: append([]byte(nil), row...), sec: copySec(sec)})
		e.mu.Unlock()
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
		e.mu.Lock()
		oh := tb.head(oldpk)
		if oh == nil || oh.head.deleted {
			e.mu.Unlock()
			return ErrNotFound
		}
		var nh *rowHead
		if !same {
			nh = tb.head(newpk)
			if nh != nil && !nh.head.deleted {
				e.mu.Unlock()
				return DupError{0}
			}
		}
		wait, err := e.checkUnique(t, tb, [][]byte{oldpk, newpk}, sec)
		if err != nil {
			e.mu.Unlock()
			return err
		}
		if wait != nil {
			e.mu.Unlock()
			if err := e.lm.lock(t, tb, wait, lockS); err != nil {
				return err
			}
			continue
		}
		nv := &rowVer{trx: t, data: append([]byte(nil), row...), sec: copySec(sec)}
		if same {
			e.pushVersion(t, tb, oldpk, oh, nv)
		} else {
			e.pushVersion(t, tb, oldpk, oh, &rowVer{trx: t, deleted: true})
			e.pushVersion(t, tb, newpk, nh, nv)
		}
		e.mu.Unlock()
		return nil
	}
}

func (e *Engine) Delete(t *Txn, tb *table, pk []byte) error {
	if err := e.lm.lock(t, tb, pk, lockX); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	h := tb.head(pk)
	if h == nil || h.head.deleted {
		return nil
	}
	e.pushVersion(t, tb, pk, h, &rowVer{trx: t, deleted: true})
	return nil
}

// ---------------------------------------------------------------- 读操作

// GetRow 按主键读一行。lock == lockNone 为一致性读（快照），否则先加锁再读最新版本（当前读）。
func (e *Engine) GetRow(t *Txn, tb *table, pk []byte, lock int) ([]byte, error) {
	if lock == lockNone {
		e.mu.RLock()
		defer e.mu.RUnlock()
		h := tb.head(pk)
		if h == nil {
			return nil, ErrNotFound
		}
		v := h.visible(t.readView())
		if v == nil || v.deleted {
			return nil, ErrNotFound
		}
		return v.data, nil
	}
	if err := e.lm.lock(t, tb, pk, lock); err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	h := tb.head(pk)
	if h == nil || h.head.deleted {
		return nil, ErrNotFound
	}
	return h.head.data, nil
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
}

func (e *Engine) OpenCursor(t *Txn, tb *table, idx, lock, pkWidth int) *Cursor {
	return &Cursor{e: e, t: t, tbl: tb, idx: idx, lock: lock, pkWidth: pkWidth, raw: rawCursor{t: tb.tree(idx)}}
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

// snapshotRow 一致性读当前候选；调用者持有 e.mu（读锁即可）。
func (c *Cursor) snapshotRow() ([]byte, bool) {
	var h *rowHead
	if c.idx == 0 {
		h = c.raw.val.(*rowHead)
	} else {
		h = c.tbl.head(c.pk())
	}
	if h == nil {
		return nil, false
	}
	v := h.visible(c.t.readView())
	if v == nil || v.deleted || !c.entryMatches(v) {
		return nil, false
	}
	return v.data, true
}

// currentRow 当前读（已持有行锁）：读链头。
func (c *Cursor) currentRow(pk []byte) ([]byte, bool) {
	h := c.tbl.head(pk)
	if h == nil || h.head.deleted || !c.entryMatches(h.head) {
		return nil, false
	}
	return h.head.data, true
}

// Seek 定位并返回第一个符合条件的行；Step 沿 dir 方向取下一行。
func (c *Cursor) Seek(key []byte, mode int) ([]byte, error) { return c.advance(true, key, mode, 0) }
func (c *Cursor) Step(dir int) ([]byte, error)              { return c.advance(false, nil, 0, dir) }

func (c *Cursor) advance(isSeek bool, key []byte, mode, dir int) ([]byte, error) {
	e := c.e
	var prefix []byte
	if isSeek {
		dir = seekDir(mode)
		if seekNeedsPrefix(mode) {
			prefix = append([]byte(nil), key...)
		}
	}
	for {
		e.mu.RLock()
		var ok bool
		if isSeek {
			ok = c.raw.seek(key, mode)
			isSeek = false
		} else {
			ok = c.raw.step(dir)
		}
		if ok && prefix != nil && !bytes.HasPrefix(c.raw.key, prefix) {
			c.raw.valid, ok = false, false
		}
		if !ok {
			e.mu.RUnlock()
			return nil, ErrNotFound
		}
		if c.lock == lockNone {
			row, ok := c.snapshotRow()
			e.mu.RUnlock()
			if ok {
				return row, nil
			}
			continue
		}
		pk := c.pk()
		e.mu.RUnlock()
		// 加锁读：先放掉 e.mu 再等行锁；醒来后树可能变了，游标会按 key 重新定位
		if err := e.lm.lock(c.t, c.tbl, pk, c.lock); err != nil {
			return nil, err
		}
		e.mu.RLock()
		row, ok2 := c.currentRow(pk)
		e.mu.RUnlock()
		if ok2 {
			return row, nil
		}
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
	e.mu.Lock()
	for _, it := range items {
		e.pruneChain(it.tbl, it.pk, oldest)
	}
	e.mu.Unlock()
	return n
}

// purgeAll 反复清理直到没有可清理的项（测试用）。
func (e *Engine) purgeAll() {
	for e.purgeSome(1<<20) > 0 {
	}
}

// pruneChain 丢弃对所有活跃视图都不再可见的旧版本。调用者持有 e.mu 写锁。
func (e *Engine) pruneChain(tb *table, pk []byte, oldest uint64) {
	h := tb.head(pk)
	if h == nil {
		return
	}
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
	if keep == h.head && keep.deleted { // 只剩一个对所有人都已生效的墓碑：整行可以删掉
		tb.pri.Delete(pk)
		h.head = nil
	}
	for d := dropped; d != nil; d = d.prev {
		for _, sk := range d.sec {
			if h.head == nil || !chainReferences(h, sk) {
				tb.sec[sk.idx].Delete(concat(sk.key, pk))
			}
		}
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	c := &Cursor{e: e, tbl: tb, idx: idx, pkWidth: pkWidth, raw: rawCursor{t: tb.tree(idx)}}
	for ok := c.raw.seek(nil, seekLast); ok; ok = c.raw.step(-1) {
		if row, ok2 := c.currentRow(c.pk()); ok2 {
			return row, true
		}
	}
	return nil, false
}
