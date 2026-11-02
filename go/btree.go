package main

// 内存 B+ 树：key 为 []byte，按 bytes.Compare 排序；value 为 any。叶子用双向链表串联。
//
// 并发控制（两级锁，设计见 docs/plan-gap-latch.md 2.5）：
//
//	smo   读写锁。共享：任何普通操作；独占：节点分裂/合并/借位/根变化（SMO）。
//	node.mu 只在叶子上：保护该叶子的 keys/vals（以及叶子里 value 所指对象的内容，由调用者约定）。
//
// 内部节点只在 SMO 时被修改，所以持有 smo 读锁时下降到叶子不需要任何内部节点锁。
// 普通插入/删除在叶子能容纳时只拿叶子写锁；否则释放后改拿 smo 写锁，用递归的分裂/合并代码重做一遍。
// 任何时刻，一个 goroutine 最多持有一个叶子锁（跨叶子时先放后取），所以读写交叉不会死锁。
// 非根叶子永远不会为空（删除的快速路径要求删后仍 >= minK）。
//
// 带回调的方法（GetFn / Modify / Upsert / DeleteIf / Locate / StepFrom 的 visit）在叶子锁内执行回调，
// 回调必须很短且不得阻塞，也不得再去取其它树的锁。

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxKeys = 32 // 每个节点最多的 key 数
)

type node struct {
	mu       sync.RWMutex // 仅叶子使用
	ver      uint64       // 仅叶子：每次增/删键自增（在 mu 写锁或 smo 写锁内）
	leaf     bool
	keys     [][]byte
	vals     []any // 仅叶子
	children []*node
	next     *node
	prev     *node
}

type BTree struct {
	smo   sync.RWMutex
	root  *node
	size  atomic.Int64
	epoch uint64 // 每次 SMO 自增（在 smo 写锁内）；游标据此判断缓存的位置是否仍有效
	maxK  int
	minK  int
}

func NewBTree() *BTree { return newBTreeSized(defaultMaxKeys) }

// newBTreeSized 用于测试：小扇出能让分裂/合并频繁发生。
func newBTreeSized(maxK int) *BTree {
	return &BTree{root: &node{leaf: true}, maxK: maxK, minK: maxK / 2}
}

func (t *BTree) Len() int { return int(t.size.Load()) }

// childIndex: 内部节点中 key 应该下探的孩子下标。
func (n *node) childIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
}

// leafIndex: 叶子中第一个 >= key 的位置。
func (n *node) leafIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
}

// descend 下降到应包含 key 的叶子。调用者持有 smo（读或写锁）；内部节点此时不会被修改，所以不需要节点锁。
func (t *BTree) descend(key []byte) *node {
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

func (t *BTree) leftmost() *node {
	n := t.root
	for !n.leaf {
		n = n.children[0]
	}
	return n
}

func (t *BTree) rightmost() *node {
	n := t.root
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return n
}

func traceLevel(depth int, n *node, key []byte) {
	if n.leaf {
		if len(n.keys) == 0 {
			logf("  层%d 叶子   : 空", depth)
			return
		}
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

// ---------------------------------------------------------------- 点操作

// GetFn 在叶子读锁内对 key 对应的 value 执行 fn。返回 key 是否存在。
func (t *BTree) GetFn(key []byte, fn func(val any)) bool {
	t.smo.RLock()
	n := t.descend(key)
	n.mu.RLock()
	i := n.leafIndex(key)
	found := i < len(n.keys) && bytes.Equal(n.keys[i], key)
	if found && fn != nil {
		fn(n.vals[i])
	}
	n.mu.RUnlock()
	t.smo.RUnlock()
	return found
}

// Get 返回 value 本身。注意：如果 value 是指针，指针所指对象的内容受叶子锁保护，请用 GetFn。
func (t *BTree) Get(key []byte) (any, bool) {
	var v any
	ok := t.GetFn(key, func(x any) { v = x })
	return v, ok
}

// Modify 在叶子写锁内对已存在的 key 的 value 执行 fn（不改变树结构）。返回 key 是否存在。
func (t *BTree) Modify(key []byte, fn func(val any)) bool {
	t.smo.RLock()
	n := t.descend(key)
	n.mu.Lock()
	i := n.leafIndex(key)
	found := i < len(n.keys) && bytes.Equal(n.keys[i], key)
	if found {
		fn(n.vals[i])
	}
	n.mu.Unlock()
	t.smo.RUnlock()
	return found
}

// Upsert 在叶子写锁内原子地"查找并决定"：key 已存在则 fn(old, true)，不存在则 fn(nil, false)。
// fn 返回 (nv, store)：store 为真则存入 nv（已存在时替换，不存在时插入）。返回 key 之前是否存在。
// fn 只会被调用一次。
func (t *BTree) Upsert(key []byte, fn func(old any, exists bool) (nv any, store bool)) bool {
	t.smo.RLock()
	n := t.descend(key)
	n.mu.Lock()
	i := n.leafIndex(key)
	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		if nv, store := fn(n.vals[i], true); store {
			n.vals[i] = nv
		}
		n.mu.Unlock()
		t.smo.RUnlock()
		return true
	}
	if len(n.keys) < t.maxK { // 叶子放得下：就地插入，不需要 SMO
		if nv, store := fn(nil, false); store {
			n.keys = insertAt(n.keys, i, append([]byte(nil), key...))
			n.vals = insertAnyAt(n.vals, i, nv)
			n.ver++
			t.size.Add(1)
		}
		n.mu.Unlock()
		t.smo.RUnlock()
		return false
	}
	n.mu.Unlock()
	t.smo.RUnlock()

	// 慢路径：可能要分裂，独占整棵树后重做。此时没有任何其它 goroutine在树里，叶子不需要再加锁。
	t.smo.Lock()
	defer t.smo.Unlock()
	leaf := t.descend(key)
	j := leaf.leafIndex(key)
	if j < len(leaf.keys) && bytes.Equal(leaf.keys[j], key) { // 在我们放锁的间隙里别人插入了它
		if nv, store := fn(leaf.vals[j], true); store {
			leaf.vals[j] = nv
		}
		return true
	}
	nv, store := fn(nil, false)
	if !store {
		return false
	}
	_, sk, sn := t.insert(t.root, append([]byte(nil), key...), nv)
	if sn != nil {
		t.root = &node{keys: [][]byte{sk}, children: []*node{t.root, sn}}
	}
	t.size.Add(1)
	t.epoch++
	return false
}

// Put 插入；overwrite=false 且 key 已存在时返回 false（不修改）。
func (t *BTree) Put(key []byte, val any, overwrite bool) bool {
	ok := true
	t.Upsert(key, func(old any, exists bool) (any, bool) {
		if exists {
			ok = overwrite
			return val, overwrite
		}
		return val, true
	})
	return ok
}

// DeleteIf 删除 key（如果存在且 pred 为 nil 或返回真；pred 在持有叶子/树锁时执行）。返回是否删除了。
func (t *BTree) DeleteIf(key []byte, pred func(val any) bool) bool {
	t.smo.RLock()
	n := t.descend(key)
	n.mu.Lock()
	i := n.leafIndex(key)
	if i >= len(n.keys) || !bytes.Equal(n.keys[i], key) {
		n.mu.Unlock()
		t.smo.RUnlock()
		return false
	}
	if pred != nil && !pred(n.vals[i]) {
		n.mu.Unlock()
		t.smo.RUnlock()
		return false
	}
	if len(n.keys) > t.minK || n == t.root { // 删后不会下溢：就地删除
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i], n.vals[i+1:]...)
		n.ver++
		t.size.Add(-1)
		n.mu.Unlock()
		t.smo.RUnlock()
		return true
	}
	n.mu.Unlock()
	t.smo.RUnlock()

	t.smo.Lock() // 慢路径：删除会引起借位/合并
	defer t.smo.Unlock()
	leaf := t.descend(key)
	j := leaf.leafIndex(key)
	if j >= len(leaf.keys) || !bytes.Equal(leaf.keys[j], key) {
		return false
	}
	if pred != nil && !pred(leaf.vals[j]) {
		return false
	}
	t.remove(t.root, key)
	if !t.root.leaf && len(t.root.children) == 1 {
		t.root = t.root.children[0]
	}
	t.size.Add(-1)
	t.epoch++
	return true
}

func (t *BTree) Delete(key []byte) bool { return t.DeleteIf(key, nil) }

// ---------------------------------------------------------------- SMO（调用者持有 smo 写锁）

// insert 递归插入；返回 (key 是否已存在, 分裂产生的分隔 key, 新右节点)。
func (t *BTree) insert(n *node, key []byte, val any) (bool, []byte, *node) {
	if n.leaf {
		i := n.leafIndex(key)
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			n.vals[i] = val
			return true, nil, nil
		}
		n.keys = insertAt(n.keys, i, key)
		n.vals = insertAnyAt(n.vals, i, val)
		n.ver++
		if len(n.keys) <= t.maxK {
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
	exists, sk, sn := t.insert(n.children[ci], key, val)
	if sn == nil {
		return exists, nil, nil
	}
	n.keys = insertAt(n.keys, ci, sk)
	n.children = insertNode(n.children, ci+1, sn)
	if len(n.keys) <= t.maxK {
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

func (t *BTree) remove(n *node, key []byte) bool {
	if n.leaf {
		i := n.leafIndex(key)
		if i >= len(n.keys) || !bytes.Equal(n.keys[i], key) {
			return false
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i], n.vals[i+1:]...)
		n.ver++
		return true
	}
	ci := n.childIndex(key)
	if !t.remove(n.children[ci], key) {
		return false
	}
	if len(n.children[ci].keys) < t.minK {
		t.rebalance(n, ci)
	}
	return true
}

// rebalance 修复 children[i] 下溢：先向兄弟借，借不到则合并。
func (t *BTree) rebalance(n *node, i int) {
	c := n.children[i]
	var left, right *node
	if i > 0 {
		left = n.children[i-1]
	}
	if i < len(n.children)-1 {
		right = n.children[i+1]
	}
	switch {
	case left != nil && len(left.keys) > t.minK:
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
	case right != nil && len(right.keys) > t.minK:
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

// ---------------------------------------------------------------- 定位与遍历

// Loc 是游标在树里的位置：叶子、下标，以及当时的叶子 ver 和树 epoch。
// 只有 epoch 和叶子 ver 都没变，位置才仍然有效（见 StepFrom）。
type Loc struct {
	n     *node
	i     int
	ver   uint64
	epoch uint64
}

const (
	locGE    = iota // 第一个 >= key
	locGT           // 第一个 > key
	locLT           // 最后一个 < key
	locFirst        // 第一个
	locLast         // 最后一个
)

// Locate 按 op 定位到一个条目，并在叶子读锁内对它调用 visit（可为 nil）。
func (t *BTree) Locate(op int, key []byte, visit func(key []byte, val any)) (Loc, bool) {
	t.smo.RLock()
	defer t.smo.RUnlock()
	return t.locateLocked(op, key, visit)
}

// locateLocked：调用者持有 smo 读锁。
func (t *BTree) locateLocked(op int, key []byte, visit func(key []byte, val any)) (Loc, bool) {
	var n *node
	switch op {
	case locFirst:
		n = t.leftmost()
	case locLast:
		n = t.rightmost()
	default:
		n = t.descend(key)
	}
	n.mu.RLock()
	var i int
	switch op {
	case locGE:
		i = n.leafIndex(key)
	case locGT:
		i = n.leafIndex(key)
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			i++
		}
	case locLT:
		i = n.leafIndex(key) - 1
	case locFirst:
		i = 0
	case locLast:
		i = len(n.keys) - 1
	}
	// 越过叶子边界：先放当前叶子锁再取邻居叶子锁（任何时刻最多持有一个叶子锁）
	for i < 0 || i >= len(n.keys) {
		forward := i >= len(n.keys)
		nb := n.prev
		if forward {
			nb = n.next
		}
		n.mu.RUnlock()
		if nb == nil {
			return Loc{}, false
		}
		n = nb
		n.mu.RLock()
		if forward {
			i = 0
		} else {
			i = len(n.keys) - 1
		}
	}
	if visit != nil {
		visit(n.keys[i], n.vals[i])
	}
	loc := Loc{n, i, n.ver, t.epoch}
	n.mu.RUnlock()
	return loc, true
}

// StepFrom 从 loc 沿 dir(+1/-1) 走一步。epoch 和叶子 ver 都没变时 O(1) 沿叶子链移动；
// 否则按上一个条目的键 lastKey 重新下探（所以扫描期间并发增删不会使游标失效）。
func (t *BTree) StepFrom(loc Loc, dir int, lastKey []byte, visit func(key []byte, val any)) (Loc, bool) {
	t.smo.RLock()
	defer t.smo.RUnlock()
	if loc.n != nil && loc.epoch == t.epoch {
		n := loc.n
		n.mu.RLock()
		if n.ver == loc.ver {
			j := loc.i + dir
			if j >= 0 && j < len(n.keys) {
				if visit != nil {
					visit(n.keys[j], n.vals[j])
				}
				l := Loc{n, j, n.ver, t.epoch}
				n.mu.RUnlock()
				return l, true
			}
			nb := n.prev
			if dir > 0 {
				nb = n.next
			}
			n.mu.RUnlock()
			if nb == nil {
				return Loc{}, false
			}
			nb.mu.RLock()
			jj := len(nb.keys) - 1
			if dir > 0 {
				jj = 0
			}
			if jj >= 0 && jj < len(nb.keys) {
				if visit != nil {
					visit(nb.keys[jj], nb.vals[jj])
				}
				l := Loc{nb, jj, nb.ver, t.epoch}
				nb.mu.RUnlock()
				return l, true
			}
			nb.mu.RUnlock()
		} else {
			n.mu.RUnlock()
		}
	}
	if dir > 0 {
		return t.locateLocked(locGT, lastKey, visit)
	}
	return t.locateLocked(locLT, lastKey, visit)
}

// ScanPrefix 依次对所有以 prefix 开头的条目的键调用 fn，fn 返回 false 则停止。
// fn 在没有任何树锁的情况下执行，所以可以去访问别的树。
func (t *BTree) ScanPrefix(prefix []byte, fn func(key []byte) bool) {
	var key []byte
	capture := func(k []byte, _ any) { key = k }
	loc, ok := t.Locate(locGE, prefix, capture)
	for ok && bytes.HasPrefix(key, prefix) {
		if !fn(key) {
			return
		}
		loc, ok = t.StepFrom(loc, 1, key, capture)
	}
}

// ---------------------------------------------------------------- 辅助

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

// check 校验树的结构不变量（测试用，要求此刻没有并发操作）。
func (t *BTree) check() string {
	depth := -1
	var walk func(n *node, lo, hi []byte, d int, root bool) string
	walk = func(n *node, lo, hi []byte, d int, root bool) string {
		if !root && len(n.keys) < t.minK {
			return "underflow"
		}
		if len(n.keys) > t.maxK {
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
	if s := walk(t.root, nil, nil, 0, true); s != "" {
		return s
	}
	// 叶子链：从左到右恰好覆盖所有叶子，且 prev/next 互相一致
	var prev *node
	cnt := 0
	for n := t.leftmost(); n != nil; n = n.next {
		if n.prev != prev {
			return "leaf chain prev mismatch"
		}
		cnt += len(n.keys)
		prev = n
	}
	if cnt != t.Len() {
		return fmt.Sprintf("leaf chain holds %d keys but size is %d", cnt, t.Len())
	}
	return ""
}
