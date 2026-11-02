package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
)

func k(i int) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, uint64(i)); return b }

func collect(t *BTree) []int {
	var r []int
	for p := t.First(); p.valid(); p = p.Next() {
		r = append(r, int(binary.BigEndian.Uint64(p.key())))
	}
	return r
}

// 与排序切片做对照的随机测试：插入/覆盖/删除/边界查找/正反向遍历。
func TestRandomAgainstOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	tr := NewBTree()
	oracle := map[int]bool{}
	for step := 0; step < 60000; step++ {
		i := rng.Intn(3000)
		switch rng.Intn(3) {
		case 0, 1:
			ok := tr.Put(k(i), k(i*2), false)
			if ok == oracle[i] {
				t.Fatalf("put %d: ok=%v existed=%v", i, ok, oracle[i])
			}
			oracle[i] = true
		case 2:
			if tr.Delete(k(i)) != oracle[i] {
				t.Fatalf("delete %d mismatch", i)
			}
			delete(oracle, i)
		}
		if step%2000 == 0 {
			if s := tr.check(); s != "" {
				t.Fatalf("step %d: invariant: %s", step, s)
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
		t.Fatalf("len got %d want %d (Len=%d)", len(got), len(want), tr.Len())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("idx %d got %d want %d", i, got[i], want[i])
		}
	}
	// 反向遍历
	idx := len(want) - 1
	for p := tr.Last(); p.valid(); p = p.Prev() {
		if int(binary.BigEndian.Uint64(p.key())) != want[idx] {
			t.Fatalf("reverse mismatch at %d", idx)
		}
		idx--
	}
	if idx != -1 {
		t.Fatalf("reverse incomplete, idx=%d", idx)
	}
	// 边界查找
	for q := 0; q < 3002; q += 7 {
		lb := sort.SearchInts(want, q)
		ub := sort.SearchInts(want, q+1)
		if p := tr.LowerBound(k(q)); (lb < len(want)) != p.valid() || (p.valid() && int(binary.BigEndian.Uint64(p.key())) != want[lb]) {
			t.Fatalf("lowerbound %d", q)
		}
		if p := tr.UpperBound(k(q)); (ub < len(want)) != p.valid() || (p.valid() && int(binary.BigEndian.Uint64(p.key())) != want[ub]) {
			t.Fatalf("upperbound %d", q)
		}
		if p := tr.LastLess(k(q)); (lb > 0) != p.valid() || (p.valid() && int(binary.BigEndian.Uint64(p.key())) != want[lb-1]) {
			t.Fatalf("lastless %d", q)
		}
	}
}

func TestOverwriteAndEmpty(t *testing.T) {
	tr := NewBTree()
	if tr.First().valid() || tr.Last().valid() {
		t.Fatal("empty tree has positions")
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
	if tr.Len() != 0 || tr.First().valid() {
		t.Fatal("not empty after delete")
	}
}

func TestSequentialFillDrain(t *testing.T) {
	tr := NewBTree()
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
