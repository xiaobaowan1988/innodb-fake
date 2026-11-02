package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func k(i int) []byte    { b := make([]byte, 8); binary.BigEndian.PutUint64(b, uint64(i)); return b }
func kint(b []byte) int { return int(binary.BigEndian.Uint64(b)) }

// 按正向遍历整棵树，返回所有键对应的整数。
func collect(t *BTree) []int {
	var key []byte
	capture := func(kk []byte, _ any) { key = kk }
	var r []int
	loc, ok := t.Locate(locFirst, nil, capture)
	for ok {
		r = append(r, kint(key))
		loc, ok = t.StepFrom(loc, 1, key, capture)
	}
	return r
}

func locKey(t *BTree, op int, q int) (int, bool) {
	var key []byte
	_, ok := t.Locate(op, k(q), func(kk []byte, _ any) { key = kk })
	if !ok {
		return 0, false
	}
	return kint(key), true
}

// 与排序切片做对照的随机测试：插入/覆盖/删除/边界查找/正反向遍历。扇出分别取默认值和很小的值（小扇出让分裂/合并频繁发生）。
func TestRandomAgainstOracle(t *testing.T) {
	for _, maxK := range []int{defaultMaxKeys, 4} {
		rng := rand.New(rand.NewSource(1))
		tr := newBTreeSized(maxK)
		oracle := map[int]bool{}
		for step := 0; step < 40000; step++ {
			i := rng.Intn(3000)
			switch rng.Intn(3) {
			case 0, 1:
				ok := tr.Put(k(i), k(i*2), false)
				if ok == oracle[i] {
					t.Fatalf("maxK=%d put %d: ok=%v existed=%v", maxK, i, ok, oracle[i])
				}
				oracle[i] = true
			case 2:
				if tr.Delete(k(i)) != oracle[i] {
					t.Fatalf("maxK=%d delete %d mismatch", maxK, i)
				}
				delete(oracle, i)
			}
			if step%2000 == 0 {
				if s := tr.check(); s != "" {
					t.Fatalf("maxK=%d step %d: invariant: %s", maxK, step, s)
				}
			}
		}
		var want []int
		for i := range oracle {
			want = append(want, i)
		}
		sort.Ints(want)
		got := collect(tr)
		if len(got) != len(want) || tr.Len() != len(want) {
			t.Fatalf("maxK=%d len got %d want %d (Len=%d)", maxK, len(got), len(want), tr.Len())
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("maxK=%d idx %d got %d want %d", maxK, i, got[i], want[i])
			}
		}
		// 反向遍历
		var key []byte
		capture := func(kk []byte, _ any) { key = kk }
		idx := len(want) - 1
		loc, ok := tr.Locate(locLast, nil, capture)
		for ok {
			if kint(key) != want[idx] {
				t.Fatalf("maxK=%d reverse mismatch at %d", maxK, idx)
			}
			idx--
			loc, ok = tr.StepFrom(loc, -1, key, capture)
		}
		if idx != -1 {
			t.Fatalf("maxK=%d reverse incomplete, idx=%d", maxK, idx)
		}
		// 边界查找
		for q := 0; q < 3002; q += 7 {
			lb := sort.SearchInts(want, q)
			ub := sort.SearchInts(want, q+1)
			if v, ok := locKey(tr, locGE, q); (lb < len(want)) != ok || (ok && v != want[lb]) {
				t.Fatalf("GE %d", q)
			}
			if v, ok := locKey(tr, locGT, q); (ub < len(want)) != ok || (ok && v != want[ub]) {
				t.Fatalf("GT %d", q)
			}
			if v, ok := locKey(tr, locLT, q); (lb > 0) != ok || (ok && v != want[lb-1]) {
				t.Fatalf("LT %d", q)
			}
		}
	}
}

func TestOverwriteAndEmpty(t *testing.T) {
	tr := NewBTree()
	if _, ok := tr.Locate(locFirst, nil, nil); ok {
		t.Fatal("empty tree has a first entry")
	}
	if _, ok := tr.Locate(locLast, nil, nil); ok {
		t.Fatal("empty tree has a last entry")
	}
	tr.Put([]byte("a"), []byte("1"), false)
	if tr.Put([]byte("a"), []byte("2"), false) {
		t.Fatal("dup accepted")
	}
	tr.Put([]byte("a"), []byte("3"), true)
	if v, _ := tr.Get([]byte("a")); !bytes.Equal(v.([]byte), []byte("3")) || tr.Len() != 1 {
		t.Fatal("overwrite failed")
	}
	tr.Delete([]byte("a"))
	if tr.Len() != 0 {
		t.Fatal("not empty after delete")
	}
	if _, ok := tr.Locate(locFirst, nil, nil); ok {
		t.Fatal("entry left after delete")
	}
}

func TestSequentialFillDrain(t *testing.T) {
	for _, maxK := range []int{defaultMaxKeys, 4} {
		tr := newBTreeSized(maxK)
		for i := 0; i < 20000; i++ {
			tr.Put(k(i), nil, false)
		}
		if s := tr.check(); s != "" {
			t.Fatal(s)
		}
		for i := 0; i < 20000; i++ {
			if !tr.Delete(k(i)) {
				t.Fatalf("missing %d", i)
			}
			if i%1000 == 0 {
				if s := tr.check(); s != "" {
					t.Fatalf("after delete %d: %s", i, s)
				}
			}
		}
		if tr.Len() != 0 || !tr.root.leaf {
			t.Fatal("tree should collapse to empty leaf")
		}
	}
}

func TestUpsertModifyDeleteIf(t *testing.T) {
	tr := newBTreeSized(4)
	type cell struct{ n int }
	for i := 0; i < 500; i++ { // 多次触发分裂
		exists := tr.Upsert(k(i), func(old any, ex bool) (any, bool) {
			if ex {
				t.Fatal("unexpected existing")
			}
			return &cell{i}, true
		})
		if exists {
			t.Fatal("exists")
		}
	}
	if !tr.Modify(k(7), func(v any) { v.(*cell).n = 777 }) || tr.Modify(k(9999), func(any) {}) {
		t.Fatal("Modify result")
	}
	var got int
	tr.GetFn(k(7), func(v any) { got = v.(*cell).n })
	if got != 777 {
		t.Fatal("Modify not visible")
	}
	// DeleteIf：谓词为假不删，为真才删（含需要借位/合并的路径）
	if tr.DeleteIf(k(7), func(v any) bool { return v.(*cell).n == 1 }) {
		t.Fatal("pred false must not delete")
	}
	for i := 0; i < 500; i++ {
		if !tr.DeleteIf(k(i), func(v any) bool { return true }) {
			t.Fatalf("delete %d", i)
		}
	}
	if tr.Len() != 0 || tr.check() != "" {
		t.Fatalf("after drain: len=%d check=%q", tr.Len(), tr.check())
	}
}

func TestScanPrefix(t *testing.T) {
	tr := newBTreeSized(4)
	for a := 0; a < 20; a++ {
		for b := 0; b < 5; b++ {
			tr.Put(append(k(a), byte(b)), nil, false)
		}
	}
	n := 0
	tr.ScanPrefix(k(7), func(key []byte) bool { n++; return true })
	if n != 5 {
		t.Fatalf("prefix scan found %d", n)
	}
	n = 0
	tr.ScanPrefix(k(7), func(key []byte) bool { n++; return n < 3 })
	if n != 3 {
		t.Fatalf("early stop: %d", n)
	}
}

// ---------------------------------------------------------------- 并发

// 多个写者各自拥有一组键并发增删，读者并发做点查和正反向全量扫描。
// "稳定键"从不被删除，每次扫描必须按序、不重复地看到全部稳定键；"变动键"并发插入/删除。
// 小扇出让 SMO（分裂/合并/借位）非常频繁。配合 -race 检查加锁是否完整。
func TestConcurrentTorture(t *testing.T) {
	const writers, readers, space = 4, 3, 4000
	tr := newBTreeSized(4)
	stable := map[int]bool{}
	for i := 0; i < space; i += 2 { // 偶数键是稳定键
		tr.Put(k(i), nil, false)
		stable[i] = true
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	owned := make([]map[int]bool, writers)
	for w := 0; w < writers; w++ {
		owned[w] = map[int]bool{}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 100))
			for !stop.Load() {
				// 写者 w 只操作 (奇数键 且 (i/2)%writers==w)
				i := (rng.Intn(space/2)*2 + 1)
				if (i/2)%writers != w {
					continue
				}
				if owned[w][i] {
					if !tr.Delete(k(i)) {
						t.Errorf("delete of owned key %d failed", i)
						return
					}
					delete(owned[w], i)
				} else {
					if !tr.Put(k(i), nil, false) {
						t.Errorf("put of unowned key %d reported duplicate", i)
						return
					}
					owned[w][i] = true
				}
			}
		}(w)
	}
	var scans atomic.Int64
	var bad atomic.Int64
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(r) + 7))
			var key []byte
			capture := func(kk []byte, _ any) { key = kk }
			for !stop.Load() {
				dir, op := 1, locFirst
				if r%2 == 1 {
					dir, op = -1, locLast
				}
				seenStable, prev := 0, -1
				loc, ok := tr.Locate(op, nil, capture)
				for ok {
					cur := kint(key)
					if prev != -1 && ((dir > 0 && cur <= prev) || (dir < 0 && cur >= prev)) {
						bad.Add(1) // 乱序或重复
						break
					}
					prev = cur
					if stable[cur] {
						seenStable++
					}
					loc, ok = tr.StepFrom(loc, dir, key, capture)
				}
				if seenStable != len(stable) {
					bad.Add(1) // 漏掉了稳定键
				}
				scans.Add(1)
				// 点查稳定键必须总能找到
				for j := 0; j < 50; j++ {
					i := rng.Intn(space/2) * 2
					if !tr.GetFn(k(i), nil) {
						bad.Add(1)
					}
				}
			}
		}(r)
	}
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d inconsistent observations during %d scans", bad.Load(), scans.Load())
	}
	if s := tr.check(); s != "" {
		t.Fatalf("invariant after torture: %s", s)
	}
	want := len(stable)
	for w := range owned {
		want += len(owned[w])
	}
	if tr.Len() != want || len(collect(tr)) != want {
		t.Fatalf("final size %d, collect %d, want %d", tr.Len(), len(collect(tr)), want)
	}
	t.Logf("%d full scans completed concurrently with writers", scans.Load())
}

// Upsert 的"查找并决定"是原子的：N 个 goroutine 抢同一批键，每个键恰好只有一个赢家。
func TestConcurrentUpsertExactlyOneWinner(t *testing.T) {
	tr := newBTreeSized(4)
	const keys, goroutines = 2000, 8
	var wins [keys]atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			order := rand.New(rand.NewSource(int64(g))).Perm(keys)
			for _, i := range order {
				tr.Upsert(k(i), func(old any, exists bool) (any, bool) {
					if !exists {
						wins[i].Add(1)
						return g, true
					}
					return nil, false
				})
			}
		}(g)
	}
	wg.Wait()
	for i := 0; i < keys; i++ {
		if wins[i].Load() != 1 {
			t.Fatalf("key %d had %d winners", i, wins[i].Load())
		}
	}
	if tr.Len() != keys || tr.check() != "" {
		t.Fatalf("len=%d check=%q", tr.Len(), tr.check())
	}
}
