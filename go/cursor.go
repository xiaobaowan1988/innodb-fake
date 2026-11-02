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
// 树未被修改（epoch 与叶子 ver 都没变）时沿叶子链 O(1) 移动；否则按上次的 key 重新下探，
// 所以扫描期间增删不会使游标失效。所有对树的访问都由 BTree 自己加锁。
type rawCursor struct {
	t     *BTree
	loc   Loc
	key   []byte
	val   any
	valid bool

	vk    []byte // visit 回调捕获的结果
	vv    any
	visit func(k []byte, v any)
	// hook 在叶子读锁内、对每个被访问到的条目调用：引擎用它在锁内解析版本链（链内容受叶子锁保护）。
	hook func(k []byte, v any)
}

func (c *rawCursor) capture(k []byte, v any) {
	c.vk, c.vv = k, v
	if c.hook != nil {
		c.hook(k, v)
	}
}

func (c *rawCursor) visitFn() func(k []byte, v any) {
	if c.visit == nil {
		c.visit = c.capture
	}
	return c.visit
}

func (c *rawCursor) setFrom(loc Loc, ok bool) bool {
	if !ok {
		c.valid = false
		return false
	}
	c.loc, c.key, c.val, c.valid = loc, c.vk, c.vv, true
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
	t, v := c.t, c.visitFn()
	switch mode {
	case seekExact:
		if c.setFrom(t.Locate(locGE, k, v)) && bytes.HasPrefix(c.key, k) {
			return true
		}
		return c.invalidate()
	case seekGE:
		return c.setFrom(t.Locate(locGE, k, v))
	case seekAfter:
		if s := prefixSucc(k); s != nil {
			return c.setFrom(t.Locate(locGE, s, v))
		}
		return c.invalidate()
	case seekLE:
		if s := prefixSucc(k); s != nil {
			return c.setFrom(t.Locate(locLT, s, v))
		}
		return c.setFrom(t.Locate(locLast, nil, v))
	case seekLT:
		return c.setFrom(t.Locate(locLT, k, v))
	case seekFirst:
		return c.setFrom(t.Locate(locFirst, nil, v))
	case seekLast:
		return c.setFrom(t.Locate(locLast, nil, v))
	case seekPrefixLast:
		var ok bool
		if s := prefixSucc(k); s != nil {
			ok = c.setFrom(t.Locate(locLT, s, v))
		} else {
			ok = c.setFrom(t.Locate(locLast, nil, v))
		}
		if ok && bytes.HasPrefix(c.key, k) {
			return true
		}
		return c.invalidate()
	}
	return c.invalidate()
}

func (c *rawCursor) invalidate() bool { c.valid = false; return false }

// step: dir>0 下一条，dir<0 上一条。
func (c *rawCursor) step(dir int) bool {
	if !c.valid {
		return false
	}
	return c.setFrom(c.t.StepFrom(c.loc, dir, c.key, c.visitFn()))
}

// stepBatch 批量预取：沿 dir 在同一个叶子里最多取 max 个条目，每个条目在叶子读锁内调用 visit。
// 返回取到的条目数；游标位置移到批末（loc），调用者负责维护 c.key。
func (c *rawCursor) stepBatch(dir, max int, visit func(k []byte, v any)) int {
	if !c.valid {
		return 0
	}
	loc, n := c.t.StepBatch(c.loc, dir, c.key, max, visit)
	if n == 0 {
		c.valid = false
		return 0
	}
	c.loc = loc
	return n
}

// 物理邻居的键（用于登记间隙）。每次都是一次独立的下探，只在加锁范围扫描时使用。
func (c *rawCursor) prevKey() ([]byte, bool) { return keyAt(c.t, locLT, c.key) }
func (c *rawCursor) nextKey() ([]byte, bool) { return keyAt(c.t, locGT, c.key) }

func keyAt(t *BTree, op int, key []byte) ([]byte, bool) {
	var k []byte
	_, ok := t.Locate(op, key, func(kk []byte, _ any) { k = kk })
	return k, ok
}
