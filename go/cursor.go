package main

import "bytes"

// 本文件不依赖 cgo，便于单元测试。

// 返回码约定：0 成功；1 未找到/失败；2 主键重复；3 缓冲区不足。
const (
	rcOK         = 0
	rcNotFound   = 1
	rcDup        = 2
	rcTooSmall   = 3
	rcRowMissing = 4 // 二级索引条目存在，但主索引里找不到对应的行
)

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

type table struct {
	trees map[int]*BTree // idx -> B+ 树；0 为主索引(key=主键, val=整行)
}

// emptyTree 供只读路径使用：索引还没有任何写入时不能在读锁下创建树。绝不能被修改。
var emptyTree = NewBTree()

// tree 返回索引树，不存在则创建。只能在写锁下调用。
func (t *table) tree(idx int) *BTree {
	bt := t.trees[idx]
	if bt == nil {
		bt = NewBTree()
		t.trees[idx] = bt
	}
	return bt
}

// treeRO 只读查找，不修改 map，可在读锁下并发调用。
func (t *table) treeRO(idx int) *BTree {
	if bt := t.trees[idx]; bt != nil {
		return bt
	}
	return emptyTree
}

// cursor 记录当前位置。树未被修改时直接沿叶子链移动；
// 树被修改过（version 变了）则按上次的 key 重新定位，所以扫描期间增删不会使游标失效。
type cursor struct {
	tbl   *table
	idx   int
	t     *BTree // 定位时所在的树
	p     pos    // 定位时的叶子位置，仅在 ver 未变时可用
	ver   uint64
	key   []byte
	val   []byte
	valid bool
}

func (c *cursor) set(t *BTree, p pos) int {
	if !p.valid() {
		c.valid = false
		return rcNotFound
	}
	c.t, c.p, c.ver = t, p, t.version
	c.key, c.val, c.valid = p.key(), p.val(), true
	return rcOK
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

func (c *cursor) seek(k []byte, mode int) int {
	t := c.tbl.treeRO(c.idx)
	switch mode {
	case seekExact:
		p := t.LowerBound(k)
		if p.valid() && bytes.HasPrefix(p.key(), k) {
			return c.set(t, p)
		}
		return c.set(t, pos{})
	case seekGE:
		return c.set(t, t.LowerBound(k))
	case seekAfter:
		if s := prefixSucc(k); s != nil {
			return c.set(t, t.LowerBound(s))
		}
		return c.set(t, pos{})
	case seekLE:
		if s := prefixSucc(k); s != nil {
			return c.set(t, t.LastLess(s))
		}
		return c.set(t, t.Last())
	case seekLT:
		return c.set(t, t.LastLess(k))
	case seekFirst:
		return c.set(t, t.First())
	case seekLast:
		return c.set(t, t.Last())
	case seekPrefixLast:
		var p pos
		if s := prefixSucc(k); s != nil {
			p = t.LastLess(s)
		} else {
			p = t.Last()
		}
		if p.valid() && bytes.HasPrefix(p.key(), k) {
			return c.set(t, p)
		}
		return c.set(t, pos{})
	}
	return rcNotFound
}

// step: dir>0 下一条，dir<0 上一条。
func (c *cursor) step(dir int) int {
	if !c.valid {
		return rcNotFound
	}
	t := c.tbl.treeRO(c.idx)
	if t == c.t && t.version == c.ver { // 快路径：树没变，沿叶子链移动，O(1)
		if dir > 0 {
			return c.set(t, c.p.Next())
		}
		return c.set(t, c.p.Prev())
	}
	// 慢路径：树被改过，按 key 重新下探
	if dir > 0 {
		return c.set(t, t.UpperBound(c.key))
	}
	return c.set(t, t.LastLess(c.key))
}

// fetchRow 返回游标当前位置对应的整行：主索引直接取 val；二级索引用 key 末尾的主键回表。
func (c *cursor) fetchRow(pkWidth int) ([]byte, int) {
	if !c.valid {
		return nil, rcNotFound
	}
	if c.idx == 0 {
		return c.val, rcOK
	}
	if len(c.key) < pkWidth {
		return nil, rcRowMissing
	}
	v, ok := c.tbl.treeRO(0).Get(c.key[len(c.key)-pkWidth:])
	if !ok {
		return nil, rcRowMissing
	}
	return v, rcOK
}
