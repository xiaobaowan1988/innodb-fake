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
		for v := tb.head(pk4(id)).head; v != nil; v = v.prev {
			n++
		}
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
