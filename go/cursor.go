package main

import "bytes"

// 本文件不依赖 cgo，便于单元测试。

// Seek 模式（与 ha_myengine.cc 中的 SEEK_* 一致）。
const (
	seekExact      = 0 // 第一个以 key 为前缀的条目
	seekGE         = 1 // >= key
	seekAfter      = 2 // 第一个不以 key 为前缀且 > key 的条目
	seekLE         = 3 // 最后一个 <= key（含以 key 为前缀者）
	seekLT         = 4 // < key
	seekFirst      = 5
	seekLast       = 6
	seekPrefixLast = 7 // 最后一个以 key 为前缀的条目
)

// seekDir：定位到第一个候选后，如果候选不可见，应沿哪个方向继续找。
func seekDir(mode int) int {
	switch mode {
	case seekLE, seekLT, seekLast, seekPrefixLast:
		return -1
	}
	return 1
}

// seekNeedsPrefix：这类模式要求候选必须仍以 key 为前缀，否则视为没找到。
func seekNeedsPrefix(mode int) bool { return mode == seekExact || mode == seekPrefixLast }

// rawCursor 只管"在一棵 B+ 树里的位置"，不关心事务和可见性。
// 树未被修改时直接沿叶子链移动；树被修改过（version 变了）则按上次的 key 重新定位，
// 所以扫描期间增删不会使游标失效。
type rawCursor struct {
	t     *BTree
	p     pos // 定位时的叶子位置，仅在 ver 未变时可用
	ver   uint64
	key   []byte
	val   any
	valid bool
}

func (c *rawCursor) set(p pos) bool {
	if !p.valid() {
		c.valid = false
		return false
	}
	c.p, c.ver = p, c.t.version
	c.key, c.val, c.valid = p.key(), p.val(), true
	return true
}

// prefixSucc 返回比所有以 k 为前缀的 key 都大的最小 key；不存在返回 nil。
func prefixSucc(k []byte) []byte {
	s := append([]byte(nil), k...)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0xFF {
			s[i]++
			return s[:i+1]
		}
	}
	return nil
}

func (c *rawCursor) seek(k []byte, mode int) bool {
	t := c.t
	switch mode {
	case seekExact:
		p := t.LowerBound(k)
		return p.valid() && bytes.HasPrefix(p.key(), k) && c.set(p) || c.invalidate()
	case seekGE:
		return c.set(t.LowerBound(k))
	case seekAfter:
		if s := prefixSucc(k); s != nil {
			return c.set(t.LowerBound(s))
		}
		return c.invalidate()
	case seekLE:
		if s := prefixSucc(k); s != nil {
			return c.set(t.LastLess(s))
		}
		return c.set(t.Last())
	case seekLT:
		return c.set(t.LastLess(k))
	case seekFirst:
		return c.set(t.First())
	case seekLast:
		return c.set(t.Last())
	case seekPrefixLast:
		var p pos
		if s := prefixSucc(k); s != nil {
			p = t.LastLess(s)
		} else {
			p = t.Last()
		}
		return p.valid() && bytes.HasPrefix(p.key(), k) && c.set(p) || c.invalidate()
	}
	return c.invalidate()
}

func (c *rawCursor) invalidate() bool { c.valid = false; return false }

// step: dir>0 下一条，dir<0 上一条。
func (c *rawCursor) step(dir int) bool {
	if !c.valid {
		return false
	}
	t := c.t
	if t.version == c.ver { // 快路径：树没变，沿叶子链移动，O(1)
		if dir > 0 {
			return c.set(c.p.Next())
		}
		return c.set(c.p.Prev())
	}
	// 慢路径：树被改过，按 key 重新下探
	if dir > 0 {
		return c.set(t.UpperBound(c.key))
	}
	return c.set(t.LastLess(c.key))
}
