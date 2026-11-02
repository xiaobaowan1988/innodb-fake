package main

import (
	"errors"
	"testing"
	"time"
)

// 表里预置 id = 10,20,30,40,50；v = id/10（二级索引 1 的键是 v）。
func gapFixture(t *testing.T) (*Engine, *table) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	for i := 1; i <= 5; i++ {
		mustInsert(t, e, s, tb, i*10, i)
	}
	e.Commit(s)
	return e, tb
}

func newTx(e *Engine, iso int) *Txn {
	tx := e.Begin(iso)
	tx.StmtBegin(iso)
	tx.timeout = 3 * time.Second
	return tx
}

func insertAsync(e *Engine, tx *Txn, tb *table, id int) chan error {
	ch := make(chan error, 1)
	go func() { ch <- e.Insert(tx, tb, pk4(id), row8(id/10), secs(id/10, false)) }()
	return ch
}

// blocked 判断一个异步操作是否仍在阻塞。
func blocked(ch chan error) bool {
	select {
	case err := <-ch:
		ch <- err // 放回去，之后还能取
		return false
	case <-time.After(150 * time.Millisecond):
		return true
	}
}

// 在 id 范围 [lo, hi] 上做一次加锁范围读（相当于 SELECT ... WHERE id BETWEEN lo AND hi FOR UPDATE），返回读到的 id 列表。
func lockedRangeRead(t *testing.T, e *Engine, tx *Txn, tb *table, lo, hi, lock int) []int {
	c := e.OpenCursor(tx, tb, 0, lock, 4)
	c.SetBound(boundEndFwd, pk4(hi), true)
	var ids []int
	r, err := c.Seek(pk4(lo), seekGE)
	for err == nil {
		ids = append(ids, rowVal(r)*10)
		r, err = c.Step(1)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("range read: %v", err)
	}
	return ids
}

func TestGapRangeBlocksInsert(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	got := lockedRangeRead(t, e, a, tb, 15, 35, lockX)
	if len(got) != 2 || got[0] != 20 || got[1] != 30 {
		t.Fatalf("range read returned %v", got)
	}
	b := newTx(e, isoRepeatableRead)
	// 被锁的间隙：(10,20) (20,30) (30,40)；不在其中：(-inf,10) (40,50) (50,+inf)
	want := map[int]bool{12: true, 25: true, 35: true, 5: false, 45: false, 60: false}
	pending := map[int]chan error{}
	for id := range want {
		pending[id] = insertAsync(e, b, tb, id)
	}
	// b 是同一个事务在多个 goroutine 里并发使用，仅用于测试阻塞与否；为避免干扰，逐个判断
	for id, shouldBlock := range want {
		if got := blocked(pending[id]); got != shouldBlock {
			t.Errorf("insert id=%d: blocked=%v want %v", id, got, shouldBlock)
		}
	}
	e.Commit(a)
	for id, ch := range pending {
		if err := <-ch; err != nil {
			t.Errorf("insert id=%d after commit: %v", id, err)
		}
	}
}

func TestGapReadCommittedHasNoGapLocks(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoReadCommitted)
	lockedRangeRead(t, e, a, tb, 15, 35, lockX)
	b := newTx(e, isoReadCommitted)
	for _, id := range []int{12, 25, 35} {
		if blocked(insertAsync(e, b, tb, id)) {
			t.Errorf("RC: insert id=%d must not be blocked", id)
		}
	}
}

func TestGapUniqueEqualityHitHasNoGap(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	if r, err := c.SeekFlags(pk4(30), seekExact, seekUniqueFull); err != nil || rowVal(r) != 3 {
		t.Fatalf("unique hit: %v %v", r, err)
	}
	b := newTx(e, isoRepeatableRead)
	for _, id := range []int{25, 35} {
		if blocked(insertAsync(e, b, tb, id)) {
			t.Errorf("unique hit must not lock gaps: insert %d blocked", id)
		}
	}
	// 但记录锁还在：改 id=30 要等
	b2 := newTx(e, isoRepeatableRead)
	ch := make(chan error, 1)
	go func() { ch <- e.Update(b2, tb, pk4(30), pk4(30), row8(9), secs(9, false)) }()
	if !blocked(ch) {
		t.Error("record lock on id=30 must block an update")
	}
	e.Commit(a)
	if err := <-ch; err != nil {
		t.Error(err)
	}
}

func TestGapUniqueEqualityMissLocksOnlyTheGap(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	if _, err := c.SeekFlags(pk4(25), seekExact, seekUniqueFull); !errors.Is(err, ErrNotFound) {
		t.Fatalf("miss: %v", err)
	}
	b := newTx(e, isoRepeatableRead)
	if !blocked(insertAsync(e, b, tb, 22)) {
		t.Error("insert into the missed gap must block")
	}
	if blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 35)) {
		t.Error("insert outside the gap must not block")
	}
	// 邻居记录 30 没有被锁：别人可以更新它
	b3 := newTx(e, isoRepeatableRead)
	ch := make(chan error, 1)
	go func() { ch <- e.Update(b3, tb, pk4(30), pk4(30), row8(3), secs(3, false)) }()
	if blocked(ch) {
		t.Error("gap-only lock must not lock the neighbouring record")
	}
}

func TestGapSecondaryIndex(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 1, lockX, 4)
	r, err := c.Seek(pk4(2), seekExact) // WHERE v = 2 FOR UPDATE：非唯一等值，前缀边界
	if err != nil || rowVal(r) != 2 {
		t.Fatalf("seek: %v %v", r, err)
	}
	if _, err := c.Step(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("step past prefix: %v", err)
	}
	// 二级索引间隙：(2|20, 3|30)
	b := newTx(e, isoRepeatableRead)
	if err := e.lm.lock(b, tb, pk4(21), lockX); err != nil { // 新行 id=21，v=2
		t.Fatal(err)
	}
	chIns := make(chan error, 1)
	go func() { chIns <- e.Insert(b, tb, pk4(21), row8(2), secs(2, false)) }()
	if !blocked(chIns) {
		t.Error("insert of a new secondary entry into the locked secondary gap must block")
	}
	// 把 id=40 的 v 改成 2，会在被锁的二级索引间隙里产生新条目
	b2 := newTx(e, isoRepeatableRead)
	chUpd := make(chan error, 1)
	go func() { chUpd <- e.Update(b2, tb, pk4(40), pk4(40), row8(2), secs(2, false)) }()
	if !blocked(chUpd) {
		t.Error("moving a row's secondary key into the locked gap must block")
	}
	// v 在间隙之外：不阻塞
	if blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 90)) { // v=9
		t.Error("insert outside the secondary gap must not block")
	}
	e.Commit(a)
	if err := <-chIns; err != nil {
		t.Error(err)
	}
	if err := <-chUpd; err != nil {
		t.Error(err)
	}
}

func TestGapClassicDeadlock(t *testing.T) {
	e, tb := gapFixture(t)
	a, b := newTx(e, isoRepeatableRead), newTx(e, isoRepeatableRead)
	for _, tx := range []*Txn{a, b} { // 两个事务先后对同一个空洞做 FOR UPDATE：间隙锁互不冲突
		c := e.OpenCursor(tx, tb, 0, lockX, 4)
		if _, err := c.SeekFlags(pk4(25), seekExact, seekUniqueFull); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	chA := insertAsync(e, a, tb, 25)
	if !blocked(chA) {
		t.Fatal("a's insert must wait for b's gap lock")
	}
	errB := e.Insert(b, tb, pk4(26), row8(2), secs(2, false)) // b 也要插入同一个空洞 -> 形成环
	errA := error(nil)
	if errors.Is(errB, ErrDeadlock) {
		e.Rollback(b)
		errA = <-chA
	} else {
		e.Commit(b)
		errA = <-chA
	}
	deadlocks := 0
	for _, err := range []error{errA, errB} {
		if errors.Is(err, ErrDeadlock) {
			deadlocks++
		} else if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
	}
	if deadlocks != 1 {
		t.Fatalf("exactly one deadlock victim expected, got errA=%v errB=%v", errA, errB)
	}
}

func TestGapFullScanLocksEverythingIncludingSupremum(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	n := 0
	_, err := c.Seek(nil, seekFirst)
	for err == nil {
		n++
		_, err = c.Step(1)
	}
	if n != 5 {
		t.Fatalf("scanned %d rows", n)
	}
	for _, id := range []int{5, 15, 45, 60} { // 所有间隙含最后的 (50,+inf)
		if !blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, id)) {
			t.Errorf("full scan: insert id=%d should be blocked", id)
		}
	}
}

func TestGapBackwardScan(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	c.SetBound(boundEndBwd, pk4(30), true) // id >= 30，从大到小
	var ids []int
	r, err := c.Seek(nil, seekLast)
	for err == nil {
		ids = append(ids, rowVal(r)*10)
		r, err = c.Step(-1)
	}
	if len(ids) != 3 || ids[0] != 50 || ids[2] != 30 {
		t.Fatalf("backward read %v", ids)
	}
	// 反向扫描：对每个访问到的记录（含停止记录 20）锁它前面的间隙，起点（50）上方的间隙也锁；(-inf,10) 和 (10,20) 之外不锁
	want := map[int]bool{60: true, 45: true, 35: true, 25: true, 15: true, 5: false}
	for id, shouldBlock := range want {
		if got := blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, id)); got != shouldBlock {
			t.Errorf("backward: insert id=%d blocked=%v want %v", id, got, shouldBlock)
		}
	}
}

func TestGapSerializablePlainReadIsSharedLocking(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoSerializable)
	lockedRangeRead(t, e, a, tb, 15, 35, lockS)
	if !blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 25)) {
		t.Error("SERIALIZABLE shared range read must block inserts into the range")
	}
}

func TestGapRepeatableCountStaysStable(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	n1 := len(lockedRangeRead(t, e, a, tb, 15, 45, lockX))
	ch := insertAsync(e, newTx(e, isoRepeatableRead), tb, 25)
	if !blocked(ch) {
		t.Fatal("insert must block")
	}
	a.StmtBegin(isoRepeatableRead)
	n2 := len(lockedRangeRead(t, e, a, tb, 15, 45, lockX))
	if n1 != n2 || n1 != 3 {
		t.Fatalf("phantom: first read %d rows, second read %d rows", n1, n2)
	}
	e.Commit(a)
	if err := <-ch; err != nil {
		t.Fatal(err)
	}
}

func TestGapIntervalsAreCoalesced(t *testing.T) {
	e, tb := newTestEngine(t)
	s := e.Begin(isoRepeatableRead)
	for i := 1; i <= 2000; i++ {
		mustInsert(t, e, s, tb, i*2, i)
	}
	e.Commit(s)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	_, err := c.Seek(nil, seekFirst)
	for err == nil {
		_, err = c.Step(1)
	}
	if n := len(tb.ix[0].gaps); n > 2 {
		t.Fatalf("a 2000-row scan left %d gap intervals; adjacent ones should be merged", n)
	}
	e.Commit(a)
	if len(tb.ix[0].gaps) != 0 {
		t.Fatal("gap locks must be released at commit")
	}
}

func TestGapInsertWaitTimesOut(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	lockedRangeRead(t, e, a, tb, 15, 35, lockX)
	b := newTx(e, isoRepeatableRead)
	b.timeout = 300 * time.Millisecond
	if err := e.Insert(b, tb, pk4(25), row8(2), secs(2, false)); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("want lock timeout, got %v", err)
	}
	// 超时只影响这条语句：a 提交后 b 再插入就成功了
	e.Commit(a)
	if err := e.Insert(b, tb, pk4(25), row8(2), secs(2, false)); err != nil {
		t.Fatal(err)
	}
}

func TestGapOwnLocksDoNotBlockSelf(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	lockedRangeRead(t, e, a, tb, 15, 35, lockX)
	if err := e.Insert(a, tb, pk4(25), row8(2), secs(2, false)); err != nil {
		t.Fatalf("a must be able to insert into its own gap: %v", err)
	}
}

// 同一个游标先做精确查找（自动设置前缀边界），再做范围查找：前缀边界不能残留。
func TestCursorReuseClearsAutoPrefixBound(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockNone, 4)
	if r, err := c.Seek(pk4(20), seekExact); err != nil || rowVal(r) != 2 {
		t.Fatalf("exact: %v %v", r, err)
	}
	r, err := c.Seek(pk4(35), seekGE)
	if err != nil || rowVal(r) != 4 {
		t.Fatalf("GE after exact must work, got %v %v", r, err)
	}
	// 外部设置的终点边界不会被清掉
	c.SetBound(boundEndFwd, pk4(40), true)
	c.Seek(pk4(20), seekExact)
	r, err = c.Seek(pk4(30), seekGE)
	if err != nil || rowVal(r) != 3 {
		t.Fatalf("%v %v", r, err)
	}
	c.Step(1) // 40
	if _, err := c.Step(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("external end bound must stay in force: %v", err)
	}
}

// 两次互不相邻的间隙登记不能被合并：中间没被锁的间隙必须仍然可以插入。
func TestGapDisjointIntervalsNotMerged(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	for _, miss := range []int{15, 45} { // 间隙 (10,20) 和 (40,50)
		c := e.OpenCursor(a, tb, 0, lockX, 4)
		if _, err := c.SeekFlags(pk4(miss), seekExact, seekUniqueFull); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	if blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 33)) { // 间隙 (30,40) 没被锁
		t.Error("the untouched gap between two disjoint locked gaps must not be locked")
	}
	for _, id := range []int{12, 47} {
		if !blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, id)) {
			t.Errorf("insert %d must be blocked", id)
		}
	}
}

// InnoDB 的起点规则：范围以唯一索引全键 >= K 开始且 K 存在 -> 只有记录锁，不锁它前面的间隙。
func TestGapStartAtExistingUniqueKeyHasNoGapBefore(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	r, err := c.SeekFlags(pk4(20), seekGE, seekUniqueFull)
	if err != nil || rowVal(r) != 2 {
		t.Fatalf("%v %v", r, err)
	}
	for ok := true; ok; { // 继续扫到末尾
		_, err := c.Step(1)
		ok = err == nil
	}
	if blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 15)) {
		t.Error("gap before an exactly-matched start key must not be locked")
	}
	if !blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 25)) {
		t.Error("gaps after the start key must be locked")
	}
	// 起点不存在时（25）则要锁住它所在的间隙（用全新的环境，避免读到上面事务已锁住的行）
	e2, tb2 := gapFixture(t)
	b := newTx(e2, isoRepeatableRead)
	cb := e2.OpenCursor(b, tb2, 0, lockX, 4)
	if r, err := cb.SeekFlags(pk4(25), seekGE, seekUniqueFull); err != nil || rowVal(r) != 3 {
		t.Fatalf("%v %v", r, err)
	}
	if !blocked(insertAsync(e2, newTx(e2, isoRepeatableRead), tb2, 22)) {
		t.Error("start key that does not exist: its gap must be locked")
	}
}

// InnoDB 的终点规则：唯一索引全键、包含端点的终点 <= K，K 存在 -> 扫到 K 就结束，不锁后面的间隙。
func TestGapEndAtExistingUniqueKeyStopsScan(t *testing.T) {
	e, tb := gapFixture(t)
	a := newTx(e, isoRepeatableRead)
	c := e.OpenCursor(a, tb, 0, lockX, 4)
	c.SetBoundU(boundEndFwd, pk4(30), true, true)
	var ids []int
	r, err := c.Seek(pk4(20), seekGE)
	for err == nil {
		ids = append(ids, rowVal(r)*10)
		r, err = c.Step(1)
	}
	if len(ids) != 2 || ids[1] != 30 {
		t.Fatalf("%v", ids)
	}
	if blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 35)) {
		t.Error("scan ended exactly at the unique end key: the gap after it must not be locked")
	}
	if !blocked(insertAsync(e, newTx(e, isoRepeatableRead), tb, 25)) {
		t.Error("gap inside the range must be locked")
	}
	// 终点不是唯一键：仍然要读下一条并锁住它前面的间隙（用全新的环境）
	e2, tb2 := gapFixture(t)
	b := newTx(e2, isoRepeatableRead)
	cb := e2.OpenCursor(b, tb2, 0, lockX, 4)
	cb.SetBoundU(boundEndFwd, pk4(30), true, false)
	_, err = cb.Seek(pk4(20), seekGE)
	for err == nil {
		_, err = cb.Step(1)
	}
	if !blocked(insertAsync(e2, newTx(e2, isoRepeatableRead), tb2, 36)) {
		t.Error("non-unique end bound: the gap after the last row must be locked")
	}
}
