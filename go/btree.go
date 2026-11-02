package main

import (
	"bytes"
	"fmt"
	"sort"
)

// 内存 B+ 树：key/value 均为 []byte，按 bytes.Compare 排序。
// 叶子节点用双向链表串联，支持范围扫描；删除时做借位/合并，保持平衡。

const (
	maxKeys = 32 // 每个节点最多的 key 数
	minKeys = maxKeys / 2
)

type node struct {
	leaf     bool
	keys     [][]byte
	vals     []any   // 仅叶子
	children []*node // 仅内部节点，len = len(keys)+1
	next     *node
	prev     *node
}

type BTree struct {
	root    *node
	size    int
	version uint64 // 每次新增/删除键自增；游标据此判断缓存的位置是否仍有效
}

func NewBTree() *BTree { return &BTree{root: &node{leaf: true}} }

func (t *BTree) Len() int { return t.size }

// childIndex: 内部节点中 key 应该下探的孩子下标。
func (n *node) childIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
}

// leafIndex: 叶子中第一个 >= key 的位置。
func (n *node) leafIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
}

func (t *BTree) findLeaf(key []byte) *node {
	n := t.root
	for depth := 0; ; depth++ {
		if verbose { // 调试：打印从根到叶子每一层的选择（MYENGINE_VERBOSE=1）
			traceLevel(depth, n, key)
		}
		if n.leaf {
			return n
		}
		n = n.children[n.childIndex(key)]
	}
}

func traceLevel(depth int, n *node, key []byte) {
	if n.leaf {
		i := n.leafIndex(key)
		logf("  层%d 叶子   : %d 个键 [%x .. %x]，二分到第 %d 个位置", depth, len(n.keys), n.keys[0], n.keys[len(n.keys)-1], i)
		return
	}
	i := n.childIndex(key)
	logf("  层%d 内部节点: %d 个分隔键，%d 个孩子，二分选第 %d 个孩子 (分隔键 [%s .. %s])",
		depth, len(n.keys), len(n.children), i, sepHex(n.keys, i-1), sepHex(n.keys, i))
}

func sepHex(keys [][]byte, i int) string {
	if i < 0 {
		return "-∞"
	}
	if i >= len(keys) {
		return "+∞"
	}
	return fmt.Sprintf("%x", keys[i])
}

func (t *BTree) Get(key []byte) (any, bool) {
	l := t.findLeaf(key)
	i := l.leafIndex(key)
	if i < len(l.keys) && bytes.Equal(l.keys[i], key) {
		return l.vals[i], true
	}
	return nil, false
}

// Put 插入；overwrite=false 且 key 已存在时返回 false。
func (t *BTree) Put(key []byte, val any, overwrite bool) bool {
	key = append([]byte(nil), key...)
	exists, sk, sn := t.insert(t.root, key, val, overwrite)
	if exists && !overwrite {
		return false
	}
	if sn != nil {
		t.root = &node{keys: [][]byte{sk}, children: []*node{t.root, sn}}
	}
	if !exists {
		t.size++
		t.version++
	}
	return true
}

// insert 返回 (key 是否已存在, 分裂产生的分隔 key, 新右节点)。
func (t *BTree) insert(n *node, key []byte, val any, overwrite bool) (bool, []byte, *node) {
	if n.leaf {
		i := n.leafIndex(key)
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			if overwrite {
				n.vals[i] = val
			}
			return true, nil, nil
		}
		n.keys = insertAt(n.keys, i, key)
		n.vals = insertAnyAt(n.vals, i, val)
		if len(n.keys) <= maxKeys {
			return false, nil, nil
		}
		mid := len(n.keys) / 2
		r := &node{leaf: true,
			keys: append([][]byte(nil), n.keys[mid:]...),
			vals: append([]any(nil), n.vals[mid:]...),
			next: n.next, prev: n}
		if n.next != nil {
			n.next.prev = r
		}
		n.next = r
		n.keys, n.vals = n.keys[:mid], n.vals[:mid]
		return false, r.keys[0], r
	}
	ci := n.childIndex(key)
	exists, sk, sn := t.insert(n.children[ci], key, val, overwrite)
	if sn == nil {
		return exists, nil, nil
	}
	n.keys = insertAt(n.keys, ci, sk)
	n.children = insertNode(n.children, ci+1, sn)
	if len(n.keys) <= maxKeys {
		return exists, nil, nil
	}
	mid := len(n.keys) / 2
	up := n.keys[mid]
	r := &node{
		keys:     append([][]byte(nil), n.keys[mid+1:]...),
		children: append([]*node(nil), n.children[mid+1:]...),
	}
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	return exists, up, r
}

// Delete 删除 key，返回是否存在。
func (t *BTree) Delete(key []byte) bool {
	found := t.remove(t.root, key)
	if !found {
		return false
	}
	t.size--
	t.version++
	if !t.root.leaf && len(t.root.children) == 1 {
		t.root = t.root.children[0]
	}
	return true
}

func (t *BTree) remove(n *node, key []byte) bool {
	if n.leaf {
		i := n.leafIndex(key)
		if i >= len(n.keys) || !bytes.Equal(n.keys[i], key) {
			return false
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i], n.vals[i+1:]...)
		return true
	}
	ci := n.childIndex(key)
	if !t.remove(n.children[ci], key) {
		return false
	}
	if len(n.children[ci].keys) < minKeys {
		n.rebalance(ci)
	}
	return true
}

// rebalance 修复 children[i] 下溢：先向兄弟借，借不到则合并。
func (n *node) rebalance(i int) {
	c := n.children[i]
	var left, right *node
	if i > 0 {
		left = n.children[i-1]
	}
	if i < len(n.children)-1 {
		right = n.children[i+1]
	}
	switch {
	case left != nil && len(left.keys) > minKeys:
		last := len(left.keys) - 1
		if c.leaf {
			c.keys = insertAt(c.keys, 0, left.keys[last])
			c.vals = insertAnyAt(c.vals, 0, left.vals[last])
			left.keys, left.vals = left.keys[:last], left.vals[:last]
			n.keys[i-1] = c.keys[0]
		} else {
			c.keys = insertAt(c.keys, 0, n.keys[i-1])
			c.children = insertNode(c.children, 0, left.children[len(left.children)-1])
			n.keys[i-1] = left.keys[last]
			left.keys = left.keys[:last]
			left.children = left.children[:len(left.children)-1]
		}
	case right != nil && len(right.keys) > minKeys:
		if c.leaf {
			c.keys = append(c.keys, right.keys[0])
			c.vals = append(c.vals, right.vals[0])
			right.keys, right.vals = right.keys[1:], right.vals[1:]
			n.keys[i] = right.keys[0]
		} else {
			c.keys = append(c.keys, n.keys[i])
			c.children = append(c.children, right.children[0])
			n.keys[i] = right.keys[0]
			right.keys = right.keys[1:]
			right.children = right.children[1:]
		}
	case left != nil:
		n.merge(i - 1) // 把 children[i] 并入 children[i-1]
	default:
		n.merge(i) // 把 children[i+1] 并入 children[i]
	}
}

// merge 把 children[i+1] 并入 children[i]，并删除分隔 key。
func (n *node) merge(i int) {
	l, r := n.children[i], n.children[i+1]
	if l.leaf {
		l.keys = append(l.keys, r.keys...)
		l.vals = append(l.vals, r.vals...)
		l.next = r.next
		if r.next != nil {
			r.next.prev = l
		}
	} else {
		l.keys = append(append(l.keys, n.keys[i]), r.keys...)
		l.children = append(l.children, r.children...)
	}
	n.keys = append(n.keys[:i], n.keys[i+1:]...)
	n.children = append(n.children[:i+1], n.children[i+2:]...)
}

// ---- 位置与游标辅助 ----

type pos struct {
	n *node
	i int
}

func (p pos) valid() bool { return p.n != nil && p.i >= 0 && p.i < len(p.n.keys) }
func (p pos) key() []byte { return p.n.keys[p.i] }
func (p pos) val() any    { return p.n.vals[p.i] }

// normalize: 越过叶子末尾时进入下一个叶子。
func (p pos) normalize() pos {
	for p.n != nil && p.i >= len(p.n.keys) {
		p = pos{p.n.next, 0}
	}
	return p
}

func (t *BTree) First() pos {
	n := t.root
	for !n.leaf {
		n = n.children[0]
	}
	return pos{n, 0}.normalize()
}

func (t *BTree) Last() pos {
	n := t.root
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return pos{n, len(n.keys) - 1}.back()
}

// back: 空叶子时向前找到上一个有效位置。
func (p pos) back() pos {
	for p.n != nil && p.i < 0 {
		p.n = p.n.prev
		if p.n != nil {
			p.i = len(p.n.keys) - 1
		}
	}
	return p
}

// LowerBound: 第一个 >= key 的位置。
func (t *BTree) LowerBound(key []byte) pos {
	l := t.findLeaf(key)
	return pos{l, l.leafIndex(key)}.normalize()
}

// UpperBound: 第一个 > key 的位置。
func (t *BTree) UpperBound(key []byte) pos {
	p := t.LowerBound(key)
	if p.valid() && bytes.Equal(p.key(), key) {
		return pos{p.n, p.i + 1}.normalize()
	}
	return p
}

// LastLess: 最后一个 < key 的位置。
func (t *BTree) LastLess(key []byte) pos {
	l := t.findLeaf(key)
	return pos{l, l.leafIndex(key) - 1}.back()
}

func (p pos) Next() pos { return pos{p.n, p.i + 1}.normalize() }
func (p pos) Prev() pos { return pos{p.n, p.i - 1}.back() }

func insertAt(s [][]byte, i int, v []byte) [][]byte {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

func insertAnyAt(s []any, i int, v any) []any {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

func insertNode(s []*node, i int, v *node) []*node {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

// check 校验树的结构不变量（测试用）。
func (t *BTree) check() string {
	var depth = -1
	var walk func(n *node, lo, hi []byte, d int, root bool) string
	walk = func(n *node, lo, hi []byte, d int, root bool) string {
		if !root && len(n.keys) < minKeys {
			return "underflow"
		}
		if len(n.keys) > maxKeys {
			return "overflow"
		}
		for i, k := range n.keys {
			if i > 0 && bytes.Compare(n.keys[i-1], k) >= 0 {
				return "unsorted"
			}
			if lo != nil && bytes.Compare(k, lo) < 0 {
				return "below lo"
			}
			if hi != nil && bytes.Compare(k, hi) >= 0 {
				return "above hi"
			}
		}
		if n.leaf {
			if depth == -1 {
				depth = d
			} else if depth != d {
				return "uneven depth"
			}
			return ""
		}
		if len(n.children) != len(n.keys)+1 {
			return "child count"
		}
		for i, c := range n.children {
			l, h := lo, hi
			if i > 0 {
				l = n.keys[i-1]
			}
			if i < len(n.keys) {
				h = n.keys[i]
			}
			if s := walk(c, l, h, d+1, false); s != "" {
				return s
			}
		}
		return ""
	}
	return walk(t.root, nil, nil, 0, true)
}
