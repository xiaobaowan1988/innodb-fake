package main

// 间隙锁（next-key 锁的"间隙"部分）。设计见 docs/plan-gap-latch.md。
//
// 间隙用"键区间"表示，而不是依附于某条物理记录：每个索引有一张区间表，区间是开区间 (lo, hi)，
// 键空间是索引条目键（主索引：主键编码；二级索引：二级索引键 + 主键）。
// 间隙锁之间互不冲突；它只阻塞"落在区间内的新条目的插入"（插入意向）。

import (
	"bytes"
	"sync"
	"time"
)

type gapLock struct {
	txn          *Txn
	lo, hi       []byte
	loInf, hiInf bool // 对应一侧为无穷
}

// idxState 是一个索引的并发控制状态。
//
// gapMu 的协议（物理并发改造后依然成立）：
//   - 发布者（插入新条目）持读锁：在其内"检查间隙冲突 + 发布条目"，多个发布者可并行；
//   - 登记者（游标登记间隙）持写锁：在其内"定位候选/前驱 + 登记区间"。
//
// 两者互斥，所以不会出现"我登记之前的检查通过、登记之后你才插入"的幻读漏洞。
type idxState struct {
	gapMu sync.RWMutex
	gaps  []*gapLock
}

func (g *gapLock) contains(key []byte) bool {
	return (g.loInf || bytes.Compare(g.lo, key) < 0) && (g.hiInf || bytes.Compare(key, g.hi) < 0)
}

// covers：g 是否完全覆盖开区间 (lo, hi)。
func (g *gapLock) covers(lo []byte, loInf bool, hi []byte, hiInf bool) bool {
	loOK := g.loInf || (!loInf && bytes.Compare(g.lo, lo) <= 0)
	hiOK := g.hiInf || (!hiInf && bytes.Compare(hi, g.hi) <= 0)
	return loOK && hiOK
}

// addGap 登记 (lo, hi)。调用者须持有 ix.gapMu 写锁。
// 已被自己的区间覆盖则跳过；与自己的区间相邻则合并（相邻处的单个条目由记录锁保护，所以合并是安全的）。
func (t *Txn) addGap(ix *idxState, lo []byte, loInf bool, hi []byte, hiInf bool) {
	own := t.gaps[ix]
	for _, g := range own {
		if g.covers(lo, loInf, hi, hiInf) {
			return
		}
	}
	for _, g := range own {
		if !g.hiInf && !loInf && bytes.Equal(g.hi, lo) { // 向上延伸
			g.hi, g.hiInf = append([]byte(nil), hi...), hiInf
			return
		}
		if !g.loInf && !hiInf && bytes.Equal(g.lo, hi) { // 向下延伸
			g.lo, g.loInf = append([]byte(nil), lo...), loInf
			return
		}
	}
	g := &gapLock{txn: t, lo: append([]byte(nil), lo...), loInf: loInf, hi: append([]byte(nil), hi...), hiInf: hiInf}
	ix.gaps = append(ix.gaps, g)
	if t.gaps == nil {
		t.gaps = map[*idxState][]*gapLock{}
	}
	t.gaps[ix] = append(own, g)
}

// conflict 返回第一个持有覆盖 key 的间隙锁的其他事务。调用者须持有 ix.gapMu（读锁或写锁）。
func (ix *idxState) conflict(t *Txn, key []byte) *Txn {
	for _, g := range ix.gaps {
		if g.txn != t && g.contains(key) {
			return g.txn
		}
	}
	return nil
}

// releaseGaps 在事务结束时移除它登记的全部区间。
func (t *Txn) releaseGaps() {
	for ix := range t.gaps {
		ix.gapMu.Lock()
		old := ix.gaps
		n := old[:0]
		for _, g := range old {
			if g.txn != t {
				n = append(n, g)
			}
		}
		for i := len(n); i < len(old); i++ {
			old[i] = nil
		}
		ix.gaps = n
		ix.gapMu.Unlock()
	}
	t.gaps = nil
}

// gapEntry 是即将新增的一个物理索引条目。
type gapEntry struct {
	idx int
	key []byte
}

// newEntries 列出这次写入将新增的物理索引条目（已存在的条目不会产生新的插入）。
func newEntries(tb *table, pk []byte, pkExists bool, sec []secKey) []gapEntry {
	var out []gapEntry
	if !pkExists {
		out = append(out, gapEntry{0, pk})
	}
	for _, s := range sec {
		ek := concat(s.key, pk)
		if _, ok := tb.sec[s.idx].Get(ek); !ok {
			out = append(out, gapEntry{s.idx, ek})
		}
	}
	return out
}

// lockIdxForPublish 对涉及的索引按索引号升序取 gapMu 读锁，返回释放函数。
func (tb *table) lockIdxForPublish(entries []gapEntry) func() {
	var idxs []int
	seen := map[int]bool{}
	for _, en := range entries {
		if !seen[en.idx] {
			seen[en.idx] = true
			idxs = append(idxs, en.idx)
		}
	}
	for i := 1; i < len(idxs); i++ { // 数量很少，插入排序
		for j := i; j > 0 && idxs[j] < idxs[j-1]; j-- {
			idxs[j], idxs[j-1] = idxs[j-1], idxs[j]
		}
	}
	for _, i := range idxs {
		tb.ix[i].gapMu.RLock()
	}
	return func() {
		for k := len(idxs) - 1; k >= 0; k-- {
			tb.ix[idxs[k]].gapMu.RUnlock()
		}
	}
}

// gapHolder 检查这些新条目是否落在别的事务的间隙锁里。调用者须已持有相应索引的 gapMu 读锁。
func (tb *table) gapHolder(t *Txn, entries []gapEntry) *Txn {
	for _, en := range entries {
		if h := tb.ix[en.idx].conflict(t, en.key); h != nil {
			return h
		}
	}
	return nil
}

// waitTxn 等待另一个事务结束（插入遇到间隙锁时用）。参与死锁检测，并受锁等待超时限制。
func (lm *lockMgr) waitTxn(t, holder *Txn) error {
	lm.mu.Lock()
	t.blockedOn = []*Txn{holder}
	if lm.deadlock(t) {
		t.blockedOn = nil
		lm.mu.Unlock()
		return ErrDeadlock
	}
	lm.mu.Unlock()
	var err error
	timer := time.NewTimer(t.timeout)
	select {
	case <-holder.doneCh:
	case <-timer.C:
		err = ErrLockTimeout
	}
	timer.Stop()
	lm.mu.Lock()
	t.blockedOn = nil
	lm.mu.Unlock()
	return err
}

// ---------------------------------------------------------------- 游标边界

const (
	boundNone   = 0
	boundPrefix = 1 // 条目必须仍以 key 为前缀（同键扫描 / index_next_same）
	boundEndFwd = 2 // 正向扫描的终点（MySQL 的 end_range）
	boundEndBwd = 3 // 反向扫描的终点
)

// SetBound 设置游标的扫描边界。越过边界的第一个条目只会让它前面的间隙被锁住，不会锁住该条目的行。
func (c *Cursor) SetBound(kind int, key []byte, inclusive bool) {
	c.SetBoundU(kind, key, inclusive, false)
}

// SetBoundU 同 SetBound；unique 表示终点是"唯一索引的全键"。此时扫到恰好等于终点的记录就结束，
// 不再去读后面的记录，也就不会锁后面的间隙（InnoDB 的做法，已用实验确认）。
func (c *Cursor) SetBoundU(kind int, key []byte, inclusive, unique bool) {
	c.bKind, c.bIncl, c.bUniq = kind, inclusive, unique
	c.bKey = append([]byte(nil), key...)
}

// noteReturned 在向调用者返回一行之后调用：如果这一行恰好等于唯一终点，标记扫描已经结束。
func (c *Cursor) noteReturned() {
	if !c.bUniq || !c.bIncl || (c.bKind != boundEndFwd && c.bKind != boundEndBwd) {
		return
	}
	k := c.raw.key
	if c.idx != 0 {
		k = k[:len(k)-c.pkWidth]
	}
	if bytes.Equal(k, c.bKey) {
		c.done = true
	}
}

func (c *Cursor) beyond(k []byte) bool {
	if c.autoPrefix != nil && !bytes.HasPrefix(k, c.autoPrefix) {
		return true
	}
	switch c.bKind {
	case boundPrefix:
		return !bytes.HasPrefix(k, c.bKey)
	case boundEndFwd, boundEndBwd:
		p := k
		if len(p) > len(c.bKey) {
			p = p[:len(c.bKey)]
		}
		cmp := bytes.Compare(p, c.bKey)
		if c.bKind == boundEndFwd {
			return cmp > 0 || (cmp == 0 && !c.bIncl)
		}
		return cmp < 0 || (cmp == 0 && !c.bIncl)
	}
	return false
}

// gapMode：只有 RR / SERIALIZABLE 下的加锁读才登记间隙锁（READ COMMITTED 不加，与 InnoDB 一致）。
func (c *Cursor) gapMode() bool {
	return c.lock != lockNone && (c.t.iso == isoRepeatableRead || c.t.iso == isoSerializable)
}

// registerGap 登记候选条目对应的间隙。调用者须持有 e.mu 与 ix.gapMu 写锁。
//
// 正向扫描：登记 (前驱, e)，即候选前面的间隙；越过索引末端则登记 (最后一个条目, +∞)。
//
// 反向扫描（已用 MySQL 8.0.40 的 InnoDB 做实验确认）：对每个访问到的记录（包括越过范围的那条停止记录）登记它
// **前面**的间隙 (前驱, e)；另外在起点（isFirst）再登记第一条记录**后面**的间隙 (e, 后继)，
// 这样从最大值往下扫时，起点上方的间隙（含 +∞）也被锁住。
func (c *Cursor) registerGap(dir int, hasCand, isFirst bool) {
	t := c.raw.t
	var lo, hi []byte
	loInf, hiInf := true, true
	before := func() { // 候选前面的间隙 (前驱, e)
		lo, loInf, hi, hiInf = nil, true, c.raw.key, false
		if k, ok := c.raw.prevKey(); ok {
			lo, loInf = k, false
		}
		c.t.addGap(c.ix, lo, loInf, hi, hiInf)
	}
	if dir > 0 {
		if hasCand {
			before()
		} else {
			if k, ok := keyAt(t, locLast, nil); ok {
				lo, loInf = k, false
			}
			c.t.addGap(c.ix, lo, loInf, nil, true)
		}
		return
	}
	if hasCand {
		before()
		if isFirst { // 起点上方的间隙 (e, 后继)
			lo, loInf, hi, hiInf = c.raw.key, false, nil, true
			if k, ok := c.raw.nextKey(); ok {
				hi, hiInf = k, false
			}
			c.t.addGap(c.ix, lo, loInf, hi, hiInf)
		}
		return
	}
	// 反向且没有候选：从索引开头越界，或者索引为空
	if k, ok := keyAt(t, locFirst, nil); ok {
		c.t.addGap(c.ix, nil, true, k, false)
	} else {
		c.t.addGap(c.ix, nil, true, nil, true)
	}
}
