package main

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

func newTree() *BTree { return newBTreeSized(4) } // 小扇出：让借位/合并/分裂频繁发生

// 键 = 2 字节"前缀"部分 + 2 字节后缀，便于测试前缀匹配类模式。
func k2(a, b int) []byte { return []byte{byte(a >> 8), byte(a), byte(b >> 8), byte(b)} }

func sortedKeys(m map[string]bool) [][]byte {
	var r [][]byte
	for k := range m {
		r = append(r, []byte(k))
	}
	sort.Slice(r, func(i, j int) bool { return bytes.Compare(r[i], r[j]) < 0 })
	return r
}

// 所有 seek 模式（含前缀）对照暴力实现。
func TestSeekModesAgainstBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	tb := newTree()
	set := map[string]bool{}
	for i := 0; i < 600; i++ {
		k := k2(rng.Intn(40), rng.Intn(40))
		tb.Put(k, k, true)
		set[string(k)] = true
	}
	keys := sortedKeys(set)
	c := &rawCursor{t: tb}
	for trial := 0; trial < 3000; trial++ {
		var q []byte
		if rng.Intn(2) == 0 {
			q = k2(rng.Intn(45), rng.Intn(45)) // 完整键
		} else {
			a := rng.Intn(45)
			q = []byte{byte(a >> 8), byte(a)} // 仅前缀
		}
		for mode := seekExact; mode <= seekPrefixLast; mode++ {
			var want []byte
			switch mode {
			case seekExact:
				for _, k := range keys {
					if bytes.HasPrefix(k, q) {
						want = k
						break
					}
				}
			case seekGE:
				for _, k := range keys {
					if bytes.Compare(k, q) >= 0 {
						want = k
						break
					}
				}
			case seekAfter:
				for _, k := range keys {
					if bytes.Compare(k, q) > 0 && !bytes.HasPrefix(k, q) {
						want = k
						break
					}
				}
			case seekLE:
				for i := len(keys) - 1; i >= 0; i-- {
					if bytes.Compare(keys[i], q) <= 0 || bytes.HasPrefix(keys[i], q) {
						want = keys[i]
						break
					}
				}
			case seekLT:
				for i := len(keys) - 1; i >= 0; i-- {
					if bytes.Compare(keys[i], q) < 0 {
						want = keys[i]
						break
					}
				}
			case seekFirst:
				want = keys[0]
			case seekLast:
				want = keys[len(keys)-1]
			case seekPrefixLast:
				for i := len(keys) - 1; i >= 0; i-- {
					if bytes.HasPrefix(keys[i], q) {
						want = keys[i]
						break
					}
				}
			}
			ok := c.seek(q, mode)
			if (want == nil) != !ok || (want != nil && !bytes.Equal(c.key, want)) {
				t.Fatalf("mode %d q=%x: got ok=%v key=%x want %x", mode, q, ok, c.key, want)
			}
		}
	}
}

// 全量正向/反向扫描：树未变时走快路径，必须与排序结果一致。
func TestFullScanFastPath(t *testing.T) {
	tb := newTree()
	n := 5000
	for i := 0; i < n; i++ {
		tb.Put(k(i), k(i), false)
	}
	c := &rawCursor{t: tb}
	i := 0
	for ok := c.seek(nil, seekFirst); ok; ok = c.step(1) {
		if !bytes.Equal(c.key, k(i)) {
			t.Fatalf("forward %d: got %x", i, c.key)
		}
		i++
	}
	if i != n {
		t.Fatalf("forward count %d", i)
	}
	for ok := c.seek(nil, seekLast); ok; ok = c.step(-1) {
		i--
		if !bytes.Equal(c.key, k(i)) {
			t.Fatalf("backward %d: got %x", i, c.key)
		}
	}
	if i != 0 {
		t.Fatalf("backward count, i=%d", i)
	}
}

// 扫描过程中随机插入/删除（含删除游标当前所在的键），游标必须始终返回"当前键之后最近的存活键"。
func TestScanWithConcurrentModification(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	tb := newTree()
	live := map[int]bool{}
	for i := 0; i < 3000; i++ {
		tb.Put(k(i*2), nil, false)
		live[i*2] = true
	}
	next := func(after int) (int, bool) { // 暴力：> after 的最小存活键
		best, ok := 0, false
		for v := range live {
			if v > after && (!ok || v < best) {
				best, ok = v, true
			}
		}
		return best, ok
	}
	c := &rawCursor{t: tb}
	c.seek(nil, seekFirst)
	cur := 0
	for steps := 0; steps < 1500; steps++ {
		// 随机改动树
		for m := rng.Intn(4); m > 0; m-- {
			v := rng.Intn(6200)
			switch rng.Intn(3) {
			case 0:
				if !live[v] {
					tb.Put(k(v), nil, false)
					live[v] = true
				}
			case 1:
				if live[v] {
					tb.Delete(k(v))
					delete(live, v)
				}
			case 2: // 删除游标当前所在的键
				if live[cur] {
					tb.Delete(k(cur))
					delete(live, cur)
				}
			}
		}
		want, ok := next(cur)
		got1 := c.step(1)
		if ok != got1 {
			t.Fatalf("step %d: want ok=%v got %v", steps, ok, got1)
		}
		if !ok {
			return
		}
		got := int(k2int(c.key))
		if got != want {
			t.Fatalf("step %d: after %d got %d want %d", steps, cur, got, want)
		}
		cur = got
	}
}

func k2int(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}

// 游标定位之后，树发生了结构修改（分裂/借位/合并），游标的下一步必须仍然是"上一个键之后最近的存活键"。
// 靠 epoch 和叶子 ver 让位置失效；缺了任何一个，都会在借位/合并后重复返回或跳过某个键。
func TestCursorSurvivesStructureModifications(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for iter := 0; iter < 30000; iter++ {
		tb := newTree()
		live := map[int]bool{}
		for i := 0; i < 20+rng.Intn(30); i++ {
			v := rng.Intn(200)
			if !live[v] {
				tb.Put(k(v), nil, false)
				live[v] = true
			}
		}
		keys := func() []int {
			var r []int
			for v := range live {
				r = append(r, v)
			}
			sort.Ints(r)
			return r
		}
		ks := keys()
		c := &rawCursor{t: tb}
		start := ks[rng.Intn(len(ks))]
		if !c.seek(k(start), seekGE) {
			t.Fatal("seek")
		}
		cur := kint(c.key)
		for m := 1 + rng.Intn(6); m > 0; m-- { // 对树做若干次随机修改
			v := rng.Intn(200)
			if live[v] && rng.Intn(2) == 0 {
				tb.Delete(k(v))
				delete(live, v)
			} else if !live[v] {
				tb.Put(k(v), nil, false)
				live[v] = true
			}
		}
		dir := 1
		if rng.Intn(2) == 0 {
			dir = -1
		}
		want, wantOK := 0, false
		for _, v := range keys() {
			if dir > 0 && v > cur && (!wantOK || v < want) {
				want, wantOK = v, true
			}
			if dir < 0 && v < cur && (!wantOK || v > want) {
				want, wantOK = v, true
			}
		}
		ok := c.step(dir)
		if ok != wantOK || (ok && kint(c.key) != want) {
			t.Fatalf("iter %d: from %d dir %d: got (%v, %d) want (%v, %d)", iter, cur, dir, ok, kint(c.key), wantOK, want)
		}
	}
}
