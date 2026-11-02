package main

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 没有全局锁之后，几个最危险的并发场景的压力测试。都应配合 -race 运行。

//  1. purge 与读者并发：写者反复更新（产生大量旧版本），purge 以很短的间隔运行，
//     RR 读者在快照里求和——总额必须恒定。purge 误删仍被需要的版本、或与读者争用版本链都会让总额出错或触发 -race。
func TestStressPurgeVsSnapshotReaders(t *testing.T) {
	e, tb := newTestEngine(t)
	const accounts, initial = 30, 1000
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < accounts; i++ {
		mustInsert(t, e, s, tb, i, initial)
	}
	e.Commit(s)
	stopPurge := e.StartPurger(time.Millisecond)
	defer stopPurge()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for !stop.Load() {
				a, b := rng.Intn(accounts), rng.Intn(accounts)
				if a == b {
					continue
				}
				if a > b { // 固定加锁顺序，避免死锁干扰这个测试的重点
					a, b = b, a
				}
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				ok := true
				for _, id := range []int{a, b} {
					r, err := e.GetRow(tx, tb, pk4(id), lockX)
					if err != nil {
						ok = false
						break
					}
					d := 7
					if id == a {
						d = -7
					}
					nv := rowVal(r) + d
					if e.Update(tx, tb, pk4(id), pk4(id), row8(nv), secs(nv, false)) != nil {
						ok = false
						break
					}
				}
				if ok {
					e.Commit(tx)
				} else {
					e.Rollback(tx)
				}
			}
		}(w)
	}
	var bad atomic.Int64
	var reads atomic.Int64
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
				// 同一个事务里隔一会儿再读一次：RR 必须读到同样的结果（purge 不能把我还需要的版本删掉）
				time.Sleep(time.Millisecond)
				sum2 := 0
				for _, v := range scanAll(e, tx, tb) {
					sum2 += v
				}
				if sum != accounts*initial || n != accounts || sum2 != sum {
					bad.Add(1)
				}
				reads.Add(1)
				e.Commit(tx)
			}
		}()
	}
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d of %d snapshot reads were inconsistent", bad.Load(), reads.Load())
	}
	e.purgeAll()
}

//  2. 回滚与读者并发：很多事务插入新行/更新后回滚，读者同时扫描主索引和二级索引。
//     回滚弹出版本到删除键之间有"空链"的窗口，读者必须当作不存在；已提交的行任何时候都必须可见；回滚的行永远不可见。
func TestStressRollbackVsReaders(t *testing.T) {
	e, tb := newTestEngine(t)
	const stable = 200
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < stable; i++ {
		mustInsert(t, e, s, tb, i, i%50)
	}
	e.Commit(s)
	stopPurge := e.StartPurger(time.Millisecond)
	defer stopPurge()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 50))
			n := 0
			for !stop.Load() {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				id := 1000 + w*100000 + n // 只属于本写者的新行，永远回滚
				n++
				e.Insert(tx, tb, pk4(id), row8(7), secs(rng.Intn(50), false))
				i := rng.Intn(stable)
				e.Update(tx, tb, pk4(i), pk4(i), row8(i%50), secs(i%50, false)) // 同值更新，产生新版本和二级索引条目
				e.Rollback(tx)
			}
		}(w)
	}
	var bad atomic.Int64
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for !stop.Load() {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				idx := r % 2 // 0: 主索引, 1: 二级索引
				c := e.OpenCursor(tx, tb, idx, lockNone, 4)
				n := 0
				_, err := c.Seek(nil, seekFirst)
				for err == nil {
					n++
					_, err = c.Step(1)
				}
				if n != stable { // 回滚的行不能出现，已提交的行不能消失
					bad.Add(1)
				}
				e.Commit(tx)
			}
		}(r)
	}
	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d scans saw a wrong number of rows", bad.Load())
	}
}

// 3) 唯一二级索引：很多 goroutine 抢同一个唯一值，恰好一个赢（对应 SQL 层的 T2，引擎层的版本）。
func TestStressUniqueRaceExactlyOneWinner(t *testing.T) {
	e, tb := newTestEngine(t)
	const values, goroutines = 400, 8
	var wins [values]atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for _, v := range rand.New(rand.NewSource(int64(g))).Perm(values) {
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				err := e.Insert(tx, tb, pk4(g*10000+v), row8(v), secs(v, true)) // 唯一值 v
				if err == nil {
					wins[v].Add(1)
					e.Commit(tx)
				} else {
					var d DupError
					if !errors.As(err, &d) {
						t.Errorf("unexpected error %v", err)
					}
					e.Rollback(tx)
				}
			}
		}(g)
	}
	wg.Wait()
	for v := 0; v < values; v++ {
		if wins[v].Load() != 1 {
			t.Fatalf("unique value %d had %d winners", v, wins[v].Load())
		}
	}
}

//  4. 间隙锁与插入的原子性：A 对一个范围做加锁读后，等一会儿再读，行数必须不变——
//     即使很多插入者同时在往这个范围里插（它们必须等 A 提交）。登记间隙和"检查+发布"之间任何漏洞都会造成幻读。
func TestStressGapLockNoPhantoms(t *testing.T) {
	e, tb := gapFixture(t)
	for round := 0; round < 150; round++ {
		a := newTx(e, isoRepeatableRead)
		var inserters sync.WaitGroup
		errs := make(chan error, 8)
		start := make(chan struct{})
		for g := 0; g < 6; g++ { // 往 (10,40) 内（含各个间隙）插入的事务；A 提交之前它们都应该被挡住
			inserters.Add(1)
			go func(g int) {
				defer inserters.Done()
				<-start
				tx := newTx(e, isoRepeatableRead)
				id := 11 + round*7%9 + g*3 + (round/7)*100 // 保证每轮每个 goroutine 的键都是新的
				if id%10 == 0 {
					id++
				}
				errs <- e.Insert(tx, tb, pk4(id%40+1000*(round+1)+0), row8(0), secs(0, false))
				e.Commit(tx)
			}(g)
		}
		close(start)
		n1 := len(lockedRangeRead(t, e, a, tb, 1, 100000, lockX))
		time.Sleep(time.Millisecond)
		a.StmtBegin(isoRepeatableRead)
		n2 := len(lockedRangeRead(t, e, a, tb, 1, 100000, lockX))
		e.Commit(a)
		inserters.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if n1 != n2 {
			t.Fatalf("round %d: phantom — first read %d rows, second read %d rows", round, n1, n2)
		}
		// 清理这一轮插入的行，保持每轮起点一致
		cl := newTx(e, isoRepeatableRead)
		c := e.OpenCursor(cl, tb, 0, lockX, 4)
		var del [][]byte
		r, err := c.Seek(nil, seekFirst)
		for err == nil {
			if rowVal(r) == 0 {
				del = append(del, append([]byte(nil), c.raw.key...))
			}
			r, err = c.Step(1)
		}
		for _, k := range del {
			e.Delete(cl, tb, k)
		}
		e.Commit(cl)
	}
}

//  5. purge 与"二级索引键交替变化的已提交更新"并发：purge 判断某个二级索引条目不再被引用之后，写者可能恰好压入引用同一个键的
//     新版本。purge 必须先拿到该行的 X 锁，否则它随后删掉条目，这一行就会从二级索引里消失。
func TestStressPurgeVsSameKeyUpdates(t *testing.T) {
	e, tb := newTestEngine(t)
	const rows = 100
	// 二级索引键随行值的奇偶交替 A/B/A/B：purge 丢掉旧版本(键 A)、保留的版本是键 B 时会判定 A 的条目"未被引用"，
	// 而写者随后又压入键 A 的新版本——这正是需要 purge 先拿行锁来防止的竞争
	keyFor := func(id, val int) []secKey { return []secKey{{idx: 1, key: pk4(id%50 + (val%2)*50)}} }
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < rows; i++ {
		if err := e.Insert(s, tb, pk4(i), row8(0), keyFor(i, 0)); err != nil {
			t.Fatal(err)
		}
	}
	e.Commit(s)
	stopPurge := e.StartPurger(500 * time.Microsecond)
	defer stopPurge()
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 900))
			for !stop.Load() {
				id := rng.Intn(rows)
				tx := e.Begin(isoRepeatableRead)
				tx.StmtBegin(isoRepeatableRead)
				r, err := e.GetRow(tx, tb, pk4(id), lockX)
				if err != nil {
					e.Rollback(tx)
					continue
				}
				if e.Update(tx, tb, pk4(id), pk4(id), row8(rowVal(r)+1), keyFor(id, rowVal(r)+1)) == nil {
					e.Commit(tx)
				} else {
					e.Rollback(tx)
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
				tx := e.Begin(isoReadCommitted)
				tx.StmtBegin(isoReadCommitted)
				c := e.OpenCursor(tx, tb, 1, lockNone, 4)
				n := 0
				_, err := c.Seek(nil, seekFirst)
				for err == nil {
					n++
					_, err = c.Step(1)
				}
				if n != rows {
					bad.Add(1)
				}
				e.Commit(tx)
			}
		}()
	}
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d secondary-index scans lost rows (purge removed an entry that a live version still needed)", bad.Load())
	}
}
