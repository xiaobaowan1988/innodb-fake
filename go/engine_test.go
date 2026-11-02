package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func pk4(i int) []byte    { b := make([]byte, 4); binary.BigEndian.PutUint32(b, uint32(i)); return b }
func row8(v int) []byte   { b := make([]byte, 8); binary.BigEndian.PutUint64(b, uint64(v)); return b }
func rowVal(b []byte) int { return int(binary.BigEndian.Uint64(b)) }

// 测试表：主键 4 字节；索引 1 为非唯一二级索引 (键 = v)；索引 2 为唯一二级索引 (键 = v)。
func newTestEngine(t *testing.T) (*Engine, *table) {
	e := NewEngine()
	e.LockTimeout = 2 * time.Second
	return e, e.CreateTable("t", 3)
}

func secs(v int, uniq bool) []secKey {
	s := []secKey{{idx: 1, key: pk4(v)}}
	if uniq {
		s = append(s, secKey{idx: 2, key: pk4(v), unique: true})
	}
	return s
}

func mustInsert(t *testing.T, e *Engine, tx *Txn, tb *table, id, v int) {
	t.Helper()
	if err := e.Insert(tx, tb, pk4(id), row8(v), secs(v, false)); err != nil {
		t.Fatalf("insert %d: %v", id, err)
	}
}

func get(e *Engine, tx *Txn, tb *table, id int) (int, bool) {
	r, err := e.GetRow(tx, tb, pk4(id), lockNone)
	if err != nil {
		return 0, false
	}
	return rowVal(r), true
}

func scanAll(e *Engine, tx *Txn, tb *table) map[int]int {
	out := map[int]int{}
	c := e.OpenCursor(tx, tb, 0, lockNone, 4)
	r, err := c.Seek(nil, seekFirst)
	for err == nil {
		out[int(binary.BigEndian.Uint32(c.raw.key))] = rowVal(r)
		r, err = c.Step(1)
	}
	return out
}

func TestVisibilityRR(t *testing.T) {
	e, tb := newTestEngine(t)
	a := e.Begin(isoRepeatableRead)
	mustInsert(t, e, a, tb, 1, 10)
	if v, ok := get(e, a, tb, 1); !ok || v != 10 {
		t.Fatal("own write must be visible")
	}
	b := e.Begin(isoRepeatableRead)
	a.StmtBegin(isoRepeatableRead)
	b.StmtBegin(isoRepeatableRead)
	if _, ok := get(e, b, tb, 1); ok {
		t.Fatal("dirty read")
	}
	e.Commit(a)
	if _, ok := get(e, b, tb, 1); ok {
		t.Fatal("RR: view was created before commit, must not see it") // b 的视图在 a 提交前的第一次读时创建
	}
	c := e.Begin(isoRepeatableRead)
	c.StmtBegin(isoRepeatableRead)
	if v, ok := get(e, c, tb, 1); !ok || v != 10 {
		t.Fatal("later txn must see committed row")
	}
	// c 更新并提交；c2(RR, 之前已建立视图)看不到，RC 看得到
	c2 := e.Begin(isoRepeatableRead)
	c2.StmtBegin(isoRepeatableRead)
	get(e, c2, tb, 1)
	rc := e.Begin(isoReadCommitted)
	rc.StmtBegin(isoReadCommitted)
	get(e, rc, tb, 1)
	if err := e.Update(c, tb, pk4(1), pk4(1), row8(20), secs(20, false)); err != nil {
		t.Fatal(err)
	}
	e.Commit(c)
	if v, _ := get(e, c2, tb, 1); v != 10 {
		t.Fatalf("RR repeatable read broken: %d", v)
	}
	if v, _ := get(e, rc, tb, 1); v != 10 {
		t.Fatalf("RC within the same statement should keep its view: %d", v)
	}
	rc.StmtEnd()
	rc.StmtBegin(isoReadCommitted) // 新语句，新视图
	if v, _ := get(e, rc, tb, 1); v != 20 {
		t.Fatalf("RC next statement must see committed update: %d", v)
	}
}

func TestRollbackAndStmtRollbackAndSavepoint(t *testing.T) {
	e, tb := newTestEngine(t)
	base := e.Begin(isoRepeatableRead)
	mustInsert(t, e, base, tb, 1, 10)
	mustInsert(t, e, base, tb, 2, 20)
	e.Commit(base)

	x := e.Begin(isoRepeatableRead)
	x.StmtBegin(isoRepeatableRead)
	mustInsert(t, e, x, tb, 3, 30)
	e.Update(x, tb, pk4(1), pk4(1), row8(11), secs(11, false))
	e.Delete(x, tb, pk4(2))
	sp := x.Savepoint()
	mustInsert(t, e, x, tb, 4, 40)
	e.Update(x, tb, pk4(1), pk4(1), row8(12), secs(12, false))
	e.RollbackToSavepoint(x, sp)
	got := scanAll(e, x, tb)
	if fmt.Sprint(got) != "map[1:11 3:30]" {
		t.Fatalf("after savepoint rollback: %v", got)
	}
	// 语句回滚
	x.StmtEnd()
	x.StmtBegin(isoRepeatableRead)
	mustInsert(t, e, x, tb, 5, 50)
	e.RollbackStmt(x)
	if fmt.Sprint(scanAll(e, x, tb)) != "map[1:11 3:30]" {
		t.Fatalf("after stmt rollback: %v", scanAll(e, x, tb))
	}
	e.Rollback(x)
	y := e.Begin(isoRepeatableRead)
	if fmt.Sprint(scanAll(e, y, tb)) != "map[1:10 2:20]" {
		t.Fatalf("after full rollback: %v", scanAll(e, y, tb))
	}
	// 二级索引也被清干净：回滚插入的行不应在二级索引里留下条目
	for _, id := range []int{3, 4, 5} {
		n := 0
		tb.sec[1].ScanPrefix(pk4(id*10), func([]byte) bool { n++; return true })
		if n != 0 {
			t.Fatalf("stale secondary entry for rolled back row %d", id)
		}
	}
	if tb.pri.Len() != 2 {
		t.Fatalf("primary tree should have 2 keys, has %d", tb.pri.Len())
	}
}

func TestDuplicatePrimaryKey(t *testing.T) {
	e, tb := newTestEngine(t)
	a := e.Begin(isoRepeatableRead)
	mustInsert(t, e, a, tb, 1, 10)
	e.Commit(a)
	b := e.Begin(isoRepeatableRead)
	err := e.Insert(b, tb, pk4(1), row8(99), secs(99, false))
	var d DupError
	if !errors.As(err, &d) || d.Idx != 0 {
		t.Fatalf("want dup on PK, got %v", err)
	}
	// 删除后可以重新插入
	e.Delete(b, tb, pk4(1))
	if err := e.Insert(b, tb, pk4(1), row8(7), secs(7, false)); err != nil {
		t.Fatalf("reinsert after delete: %v", err)
	}
	if v, _ := get(e, b, tb, 1); v != 7 {
		t.Fatal("reinserted value not visible")
	}
}

func TestLockCompatibility(t *testing.T) {
	e, tb := newTestEngine(t)
	e.LockTimeout = 200 * time.Millisecond
	a, b := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	a.timeout, b.timeout = 200*time.Millisecond, 200*time.Millisecond
	k := pk4(1)
	if e.lock(a, tb, k, lockS) != nil || e.lock(b, tb, k, lockS) != nil {
		t.Fatal("S/S must be compatible")
	}
	if err := e.lock(a, tb, k, lockX); !errors.Is(err, ErrLockTimeout) { // b 也持有 S，a 无法升级
		t.Fatalf("S->X upgrade with another S holder must wait/timeout, got %v", err)
	}
	e.Commit(b)
	if err := e.lock(a, tb, k, lockX); err != nil {
		t.Fatalf("upgrade after other released: %v", err)
	}
	c := e.Begin(isoRepeatableRead)
	c.timeout = 100 * time.Millisecond
	if err := e.lock(c, tb, k, lockS); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("X blocks S: %v", err)
	}
	if err := e.lock(a, tb, k, lockX); err != nil { // 重入
		t.Fatal("reentrant X failed")
	}
	e.Commit(a)
	if err := e.lock(c, tb, k, lockS); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

func TestDeadlockTwoAndThreeWay(t *testing.T) {
	for _, n := range []int{2, 3} {
		e, tb := newTestEngine(t)
		txs := make([]*Txn, n)
		for i := range txs {
			txs[i] = e.Begin(isoRepeatableRead)
			txs[i].timeout = 3 * time.Second
			if err := e.lock(txs[i], tb, pk4(i), lockX); err != nil {
				t.Fatal(err)
			}
		}
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range txs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = e.lock(txs[i], tb, pk4((i+1)%n), lockX) // 环形等待
				if errs[i] != nil {
					e.Rollback(txs[i]) // 牺牲者回滚，释放它的锁，让其余的继续
				} else {
					e.Commit(txs[i])
				}
			}(i)
		}
		wg.Wait()
		dead := 0
		for _, err := range errs {
			if errors.Is(err, ErrDeadlock) {
				dead++
			} else if err != nil {
				t.Fatalf("n=%d: unexpected error %v", n, err)
			}
		}
		if dead < 1 {
			t.Fatalf("n=%d: expected at least one deadlock victim, got %v", n, errs)
		}
	}
}

func TestUniqueSecondaryWaitsThenDup(t *testing.T) {
	e, tb := newTestEngine(t)
	a, b := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	if err := e.Insert(a, tb, pk4(1), row8(5), secs(5, true)); err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	go func() { res <- e.Insert(b, tb, pk4(2), row8(5), secs(5, true)) }() // 同一个唯一值
	select {
	case err := <-res:
		t.Fatalf("b must wait for a, got %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	e.Commit(a)
	var d DupError
	if err := <-res; !errors.As(err, &d) || d.Idx != 2 {
		t.Fatalf("after a commits b must get dup on idx 2, got %v", err)
	}
	// a 回滚的情形：b 应当成功
	c, d2 := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	e.Insert(c, tb, pk4(10), row8(77), secs(77, true))
	go func() { res <- e.Insert(d2, tb, pk4(11), row8(77), secs(77, true)) }()
	time.Sleep(100 * time.Millisecond)
	e.Rollback(c)
	if err := <-res; err != nil {
		t.Fatalf("after rollback second insert must succeed, got %v", err)
	}
}

// 读-改-写：N 个事务并发给同一行加 1，当前读 + 行锁保证不丢更新。
func TestNoLostUpdate(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	mustInsert(t, e, s, tb, 1, 0)
	e.Commit(s)
	const W, N = 8, 300
	var wg sync.WaitGroup
	for w := 0; w < W; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				c := e.OpenCursor(tx, tb, 0, lockX, 4)
				r, err := c.Seek(pk4(1), seekExact)
				if err != nil {
					t.Error(err)
					return
				}
				if err := e.Update(tx, tb, pk4(1), pk4(1), row8(rowVal(r)+1), secs(rowVal(r)+1, false)); err != nil {
					t.Error(err)
					return
				}
				e.Commit(tx)
			}
		}()
	}
	wg.Wait()
	v := e.Begin(isoRepeatableRead)
	if got, _ := get(e, v, tb, 1); got != W*N {
		t.Fatalf("lost updates: got %d want %d", got, W*N)
	}
}

// 转账：总额守恒。写者锁两行（顺序随机，必然出现死锁并重试），读者在 RR 快照里扫描求和，必须恒等于总额。
func TestBankTransferInvariant(t *testing.T) {
	e, tb := newTestEngine(t)
	const accounts, initial = 20, 1000
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < accounts; i++ {
		mustInsert(t, e, s, tb, i, initial)
	}
	e.Commit(s)
	var stop atomic.Bool
	var deadlocks, commits atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for !stop.Load() {
				a, b := rng.Intn(accounts), rng.Intn(accounts)
				if a == b {
					continue
				}
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				ok := true
				for _, id := range []int{a, b} { // 随机顺序，制造死锁
					c := e.OpenCursor(tx, tb, 0, lockX, 4)
					r, err := c.Seek(pk4(id), seekExact)
					if err != nil {
						ok = false
						break
					}
					delta := 5
					if id == a {
						delta = -5
					}
					if err := e.Update(tx, tb, pk4(id), pk4(id), row8(rowVal(r)+delta), secs(rowVal(r)+delta, false)); err != nil {
						ok = false
						break
					}
				}
				if ok {
					e.Commit(tx)
					commits.Add(1)
				} else {
					e.Rollback(tx)
					deadlocks.Add(1)
				}
			}
		}(w)
	}
	var bad atomic.Int64
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				sum, n := 0, 0
				for _, v := range scanAll(e, tx, tb) {
					sum += v
					n++
				}
				if sum != accounts*initial || n != accounts {
					bad.Add(1)
				}
				e.Commit(tx)
			}
		}()
	}
	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d snapshot reads saw an inconsistent total", bad.Load())
	}
	v := e.Begin(isoRepeatableRead)
	sum := 0
	for _, x := range scanAll(e, v, tb) {
		sum += x
	}
	if sum != accounts*initial {
		t.Fatalf("final total %d", sum)
	}
	t.Logf("commits=%d aborted(deadlock/timeout)=%d", commits.Load(), deadlocks.Load())
	if commits.Load() == 0 {
		t.Fatal("no transfer committed")
	}
}

// 并发更新二级索引列时，用二级索引扫描得到的行数恒为 N（对应 SQL 层 T3 的问题）。
func TestSecondaryIndexScanConsistent(t *testing.T) {
	e, tb := newTestEngine(t)
	const N = 300
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < N; i++ {
		mustInsert(t, e, s, tb, i, i)
	}
	e.Commit(s)
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(9))
		for !stop.Load() {
			id := rng.Intn(N)
			tx := e.Begin(isoRepeatableRead)
			tx.StmtBegin(isoRepeatableRead)
			nv := rng.Intn(5000)
			if e.Update(tx, tb, pk4(id), pk4(id), row8(nv), secs(nv, false)) == nil {
				e.Commit(tx)
			} else {
				e.Rollback(tx)
			}
		}
	}()
	var bad atomic.Int64
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				c := e.OpenCursor(tx, tb, 1, lockNone, 4)
				n := 0
				_, err := c.Seek(nil, seekFirst)
				for err == nil {
					n++
					_, err = c.Step(1)
				}
				if n != N {
					bad.Add(1)
				}
				e.Commit(tx)
			}
		}()
	}
	time.Sleep(1200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d secondary index scans returned a wrong row count", bad.Load())
	}
}

func TestPurge(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < 50; i++ {
		mustInsert(t, e, s, tb, i, i)
	}
	e.Commit(s)
	old := e.Begin(isoRepeatableRead) // 长事务：先建立视图，固定住历史
	old.StmtBegin(isoRepeatableRead)
	scanAll(e, old, tb)
	for round := 1; round <= 5; round++ {
		w := e.Begin(isoRepeatableRead)
		for i := 0; i < 50; i++ {
			e.Update(w, tb, pk4(i), pk4(i), row8(i+round*100), secs(i+round*100, false))
		}
		e.Commit(w)
	}
	w := e.Begin(isoRepeatableRead)
	for i := 0; i < 25; i++ {
		e.Delete(w, tb, pk4(i))
	}
	e.Commit(w)
	e.purgeAll() // old 还活着：旧版本必须保留
	if got := scanAll(e, old, tb); len(got) != 50 || got[3] != 3 {
		t.Fatalf("purge removed versions that an active view still needs: %v", got[3])
	}
	chain := func(id int) int {
		n := 0
		tb.withHead(pk4(id), func(h *rowHead) {
			for v := h.head; v != nil; v = v.prev {
				n++
			}
		})
		return n
	}
	if chain(40) < 2 {
		t.Fatal("history should be preserved while old view is active")
	}
	e.Commit(old)
	e.purgeAll()
	if chain(40) != 1 {
		t.Fatalf("history should be pruned after old view ends, chain=%d", chain(40))
	}
	if tb.pri.Len() != 25 {
		t.Fatalf("deleted rows should be removed from the primary tree: %d keys", tb.pri.Len())
	}
	// 二级索引：每个存活行只剩 1 个条目，已删除行没有条目
	if tb.sec[1].Len() != 25 {
		t.Fatalf("secondary index should hold exactly 25 entries after purge, has %d", tb.sec[1].Len())
	}
	v := e.Begin(isoRepeatableRead)
	got := scanAll(e, v, tb)
	if len(got) != 25 || got[30] != 30+500 {
		t.Fatalf("data wrong after purge: len=%d got[30]=%d", len(got), got[30])
	}
}

func TestSeekModesWithInvisibleRows(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	for i := 1; i <= 9; i++ {
		mustInsert(t, e, s, tb, i, i)
	}
	e.Commit(s)
	d := e.Begin(isoRepeatableRead)
	for _, i := range []int{3, 4, 5} { // 删除后未提交，别的事务仍能看到；删除者自己看不到
		e.Delete(d, tb, pk4(i))
	}
	c := e.OpenCursor(d, tb, 0, lockNone, 4)
	r, err := c.Seek(pk4(3), seekGE)
	if err != nil || rowVal(r) != 6 {
		t.Fatalf("GE over deleted rows: %v %v", r, err)
	}
	r, err = c.Seek(pk4(5), seekLE)
	if err != nil || rowVal(r) != 2 {
		t.Fatalf("LE over deleted rows: %v %v", r, err)
	}
	if _, err = c.Seek(pk4(4), seekExact); !errors.Is(err, ErrNotFound) {
		t.Fatalf("EXACT on an invisible row must not slide to a neighbour: %v", err)
	}
}

// 一条语句在中途又打开了别的表（触发器、存储函数）：再次 StmtBegin 不是新语句——
// 语句起点不能被推迟（否则失败时只回滚到较晚的起点，前面的修改残留），READ COMMITTED 的视图也不能被重置。
func TestNestedStmtBeginDoesNotMoveStatementStart(t *testing.T) {
	e, tb := newTestEngine(t)
	tx := e.Begin(isoReadCommitted)
	tx.StmtBegin(isoReadCommitted)
	mustInsert(t, e, tx, tb, 1, 1) // 第一个表上的写入
	tx.StmtBegin(isoReadCommitted) // 触发器中途打开另一个表
	mustInsert(t, e, tx, tb, 2, 2)
	e.RollbackStmt(tx) // 语句失败：必须把两行都撤销
	if n := len(scanAll(e, tx, tb)); n != 0 {
		t.Fatalf("statement rollback left %d rows behind", n)
	}
	// 视图：同一条语句里先读一次，别人提交之后再读，RC 在同一条语句内必须看到同一份快照
	tx.StmtEnd()
	tx.StmtBegin(isoReadCommitted)
	w := e.Begin(isoRepeatableRead)
	mustInsert(t, e, w, tb, 10, 1)
	e.Commit(w)
	before := len(scanAll(e, tx, tb))
	tx2 := e.Begin(isoRepeatableRead)
	mustInsert(t, e, tx2, tb, 11, 1)
	e.Commit(tx2)
	tx.StmtBegin(isoReadCommitted) // 嵌套：不能换视图
	if after := len(scanAll(e, tx, tb)); after != before {
		t.Fatalf("view was replaced in the middle of a statement: %d -> %d rows", before, after)
	}
	// 上一条语句 + 事务内先前的语句不受影响：已经提交的修改在语句回滚时保留
	tx.StmtEnd()
	tx.StmtBegin(isoReadCommitted)
	mustInsert(t, e, tx, tb, 20, 1)
	tx.StmtEnd() // 语句 A 成功结束
	tx.StmtBegin(isoReadCommitted)
	mustInsert(t, e, tx, tb, 21, 1)
	e.RollbackStmt(tx)
	got := scanAll(e, tx, tb)
	if _, ok := got[20]; !ok {
		t.Fatal("rolling back statement B must not undo the earlier statement A")
	}
	if _, ok := got[21]; ok {
		t.Fatal("statement B was not rolled back")
	}
}

// 被 KILL 的连接：所有等待都应当在一个轮询周期内返回 ErrKilled，而不是等到锁超时。
func TestKilledWaitsReturnPromptly(t *testing.T) {
	e, tb := newTestEngine(t)
	e.LockTimeout = 30 * time.Second
	holder := e.Begin(isoRepeatableRead)
	mustInsert(t, e, holder, tb, 1, 1)
	var killed atomic.Bool
	for name, op := range map[string]func(tx *Txn) error{
		"row lock": func(tx *Txn) error { _, err := e.GetRow(tx, tb, pk4(1), lockX); return err },
		"insert":   func(tx *Txn) error { return e.Insert(tx, tb, pk4(1), row8(2), secs(2, false)) },
		"gap wait": func(tx *Txn) error {
			g := e.Begin(isoRepeatableRead)
			lockedRangeRead(t, e, g, tb, 100, 200, lockX) // 在 g 上登记一个间隙锁，下面的插入会等它
			defer e.Rollback(g)
			return e.Insert(tx, tb, pk4(150), row8(1), secs(1, false))
		},
	} {
		killed.Store(false)
		tx := e.Begin(isoRepeatableRead)
		tx.timeout = 30 * time.Second
		tx.SetKillCheck(func() bool { return killed.Load() })
		done := make(chan error, 1)
		go func() { done <- op(tx) }()
		time.Sleep(200 * time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("%s: returned early: %v", name, err)
		default:
		}
		killed.Store(true)
		select {
		case err := <-done:
			if !errors.Is(err, ErrKilled) {
				t.Fatalf("%s: want ErrKilled, got %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: a killed wait did not return within 2s", name)
		}
		e.Rollback(tx)
	}
	e.Rollback(holder)
}

// ---------------------------------------------------------------- 行锁队列公平性、NOWAIT / SKIP LOCKED

func lockAsync(e *Engine, tx *Txn, tb *table, id, mode int) chan error {
	ch := make(chan error, 1)
	go func() { ch <- e.lm.lock(tx, tb, pk4(id), mode) }()
	return ch
}

func waitDone(ch chan error, d time.Duration) (error, bool) {
	select {
	case err := <-ch:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// A 持有 S，B 请求 X（等待），C 再请求 S：C 必须排在 B 后面，不能插队。
func TestLockQueueIsFifo(t *testing.T) {
	e, tb := newTestEngine(t)
	a, b, c := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	for _, x := range []*Txn{a, b, c} {
		x.timeout = 5 * time.Second
	}
	if err := e.lm.lock(a, tb, pk4(1), lockS); err != nil {
		t.Fatal(err)
	}
	chB := lockAsync(e, b, tb, 1, lockX)
	if _, done := waitDone(chB, 150*time.Millisecond); done {
		t.Fatal("B (X) must wait for A's S lock")
	}
	chC := lockAsync(e, c, tb, 1, lockS)
	if _, done := waitDone(chC, 200*time.Millisecond); done {
		t.Fatal("C (S) jumped the queue ahead of the waiting writer B")
	}
	e.Commit(a)
	if err, done := waitDone(chB, 2*time.Second); !done || err != nil {
		t.Fatalf("B should get X after A commits: %v %v", err, done)
	}
	if _, done := waitDone(chC, 150*time.Millisecond); done {
		t.Fatal("C must still wait while B holds X")
	}
	e.Commit(b)
	if err, done := waitDone(chC, 2*time.Second); !done || err != nil {
		t.Fatalf("C should get S after B commits: %v %v", err, done)
	}
	e.Commit(c)
}

// 持续有读者轮流持有 S 锁（任何时刻至少一个读者持有），写者必须在有限时间内拿到 X——没有队列的话会被无限期饿死。
func TestWriterIsNotStarvedByOverlappingReaders(t *testing.T) {
	e, tb := newTestEngine(t)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			time.Sleep(time.Duration(r) * 3 * time.Millisecond) // 错开：保证任意时刻都有读者持有锁
			for !stop.Load() {
				tx := e.Begin(isoRepeatableRead)
				if e.lm.lock(tx, tb, pk4(1), lockS) == nil {
					time.Sleep(12 * time.Millisecond)
				}
				e.Commit(tx)
			}
		}(r)
	}
	time.Sleep(50 * time.Millisecond)
	w := e.Begin(isoRepeatableRead)
	w.timeout = 3 * time.Second
	t0 := time.Now()
	err := e.lm.lock(w, tb, pk4(1), lockX)
	took := time.Since(t0)
	stop.Store(true)
	wg.Wait()
	if err != nil {
		t.Fatalf("writer starved by overlapping readers: %v after %v", err, took)
	}
	if took > time.Second {
		t.Fatalf("writer waited too long: %v", took)
	}
}

// 锁升级不排队：A 持有 S，B 等 X，A 升级成 X 必须立即成功（只被其它持有者阻塞），不能被人为地造成死锁。
func TestUpgradeDoesNotQueueBehindWaiters(t *testing.T) {
	e, tb := newTestEngine(t)
	a, b := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	a.timeout, b.timeout = 3*time.Second, 3*time.Second
	e.lm.lock(a, tb, pk4(1), lockS)
	chB := lockAsync(e, b, tb, 1, lockX)
	if _, done := waitDone(chB, 150*time.Millisecond); done {
		t.Fatal("B must wait")
	}
	up := make(chan error, 1)
	go func() { up <- e.lm.lock(a, tb, pk4(1), lockX) }()
	if err, done := waitDone(up, time.Second); !done || err != nil {
		t.Fatalf("upgrade should succeed immediately, got %v (done=%v)", err, done)
	}
	e.Commit(a)
	if err, done := waitDone(chB, 2*time.Second); !done || err != nil {
		t.Fatalf("B should proceed after A: %v %v", err, done)
	}
}

// 排在队列前面的等待者放弃（超时/被 KILL）之后，后面的等待者必须被唤醒并前进，而不是一直睡下去。
func TestAbandoningWaiterWakesTheOnesBehindIt(t *testing.T) {
	e, tb := newTestEngine(t)
	a, b, c := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	a.timeout, c.timeout = 5*time.Second, 5*time.Second
	b.timeout = 300 * time.Millisecond // B 很快超时
	e.lm.lock(a, tb, pk4(1), lockS)
	chB := lockAsync(e, b, tb, 1, lockX) // 排第一，被 A 的 S 阻塞，300ms 后超时
	time.Sleep(100 * time.Millisecond)
	chC := lockAsync(e, c, tb, 1, lockS) // 排在 B 后面，只因为 B 在前面才等
	if _, done := waitDone(chC, 100*time.Millisecond); done {
		t.Fatal("C should wait behind B")
	}
	if err, done := waitDone(chB, 2*time.Second); !done || !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("B should time out: %v %v", err, done)
	}
	if err, done := waitDone(chC, time.Second); !done || err != nil {
		t.Fatalf("C must be woken when B gives up (A only holds S, which is compatible): %v %v", err, done)
	}
}

func TestNoWaitAndSkipLocked(t *testing.T) {
	e, tb := gapFixture(t)
	holder := newTx(e, isoReadCommitted) // RC：不登记间隙锁，只看行锁
	if _, err := e.GetRow(holder, tb, pk4(20), lockX); err != nil {
		t.Fatal(err)
	}
	// NOWAIT：遇到被锁的行立即失败
	a := newTx(e, isoReadCommitted)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	c.SetWaitPolicy(waitNoWait)
	t0 := time.Now()
	_, err := c.Seek(pk4(10), seekGE)
	for err == nil {
		_, err = c.Step(1)
	}
	if !errors.Is(err, ErrNoWait) || time.Since(t0) > time.Second {
		t.Fatalf("NOWAIT should fail immediately with ErrNoWait, got %v after %v", err, time.Since(t0))
	}
	e.Rollback(a) // a 在碰到被锁的行之前已经锁住了 id=10，必须释放，否则会干扰下面的 SKIP LOCKED 断言
	// SKIP LOCKED：跳过被锁的行，返回其余的行
	b := newTx(e, isoReadCommitted)
	cs := e.OpenCursor(b, tb, 0, lockX, 4)
	cs.SetWaitPolicy(waitSkip)
	var ids []int
	r, err := cs.Seek(nil, seekFirst)
	for err == nil {
		ids = append(ids, rowVal(r)*10)
		r, err = cs.Step(1)
	}
	if !errors.Is(err, ErrNotFound) || len(ids) != 4 || ids[0] != 10 || ids[1] != 30 {
		t.Fatalf("SKIP LOCKED should skip id=20 only: %v err=%v", ids, err)
	}
	e.Rollback(b) // b 的 SKIP LOCKED 扫描已经锁住了其余的行，释放掉，下面单独验证按主键取行
	// 按主键取一行（rnd_pos）：NOWAIT 报错，SKIP LOCKED 当作"没有这一行"
	if _, err := e.GetRowPolicy(newTx(e, isoReadCommitted), tb, pk4(20), lockX, waitNoWait); !errors.Is(err, ErrNoWait) {
		t.Fatalf("GetRow NOWAIT: %v", err)
	}
	if _, err := e.GetRowPolicy(newTx(e, isoReadCommitted), tb, pk4(20), lockX, waitSkip); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRow SKIP LOCKED: %v", err)
	}
	// 没被锁的行不受策略影响
	if r, err := e.GetRowPolicy(newTx(e, isoReadCommitted), tb, pk4(30), lockX, waitNoWait); err != nil || rowVal(r) != 3 {
		t.Fatalf("unlocked row with NOWAIT: %v %v", r, err)
	}
}

// 一致性快照在建立的那一刻固定下来，之后别人的提交看不到。
func TestSnapshotIsTakenImmediately(t *testing.T) {
	e, tb := gapFixture(t)
	a := e.Begin(isoRepeatableRead)
	a.Snapshot()
	w := e.Begin(isoRepeatableRead)
	e.Update(w, tb, pk4(10), pk4(10), row8(99), secs(99, false))
	e.Commit(w)
	a.StmtBegin(isoRepeatableRead)
	if v, _ := get(e, a, tb, 10); v != 1 {
		t.Fatalf("snapshot must predate the later commit, saw %d", v)
	}
}

// 唯一键冲突而失败的插入不能在主键上留下行锁（InnoDB 回滚掉的插入不留锁）。
func TestFailedInsertLeavesNoRowLock(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	e.Insert(s, tb, pk4(1), row8(7), secs(7, true))
	e.Commit(s)
	a, b := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	var d DupError
	if err := e.Insert(a, tb, pk4(5), row8(7), secs(7, true)); !errors.As(err, &d) {
		t.Fatalf("want dup, got %v", err)
	}
	res := make(chan error, 1)
	go func() { res <- e.Insert(b, tb, pk4(5), row8(8), secs(8, true)) }()
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("b must succeed, got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("b blocked on a row lock left by a's failed insert")
	}
}

// 主键重复导致的失败插入只在已存在的行上留 S 锁：第二个同样重复的插入不应被阻塞，而要的是 X 的写者仍被挡住。
func TestDupInsertKeepsOnlySharedLock(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	mustInsert(t, e, s, tb, 10, 1)
	e.Commit(s)
	a, b, w := e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead), e.Begin(isoRepeatableRead)
	var d DupError
	if err := e.Insert(a, tb, pk4(10), row8(2), secs(2, false)); !errors.As(err, &d) {
		t.Fatalf("want dup, got %v", err)
	}
	res := make(chan error, 1)
	go func() { res <- e.Insert(b, tb, pk4(10), row8(3), secs(3, false)) }()
	select {
	case err := <-res:
		if !errors.As(err, &d) {
			t.Fatalf("b must get dup, got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("second duplicate insert blocked by an X lock")
	}
	go func() { res <- e.Update(w, tb, pk4(10), pk4(10), row8(9), secs(9, false)) }()
	select {
	case err := <-res:
		t.Fatalf("writer must wait for a's S lock, got %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	e.Commit(a)
	e.Commit(b)
	if err := <-res; err != nil {
		t.Fatal(err)
	}
}
