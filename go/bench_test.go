package main

import (
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// 引擎核心的并行基准：绕开 MySQL 与 cgo，只测引擎内部的锁竞争。
// 用法: go test -run xxx -bench Engine -cpu 1,2,4 -benchtime 2s

const benchRows = 100000

func benchEngine(b *testing.B) (*Engine, *table) {
	e := NewEngine()
	e.LockTimeout = 5 * time.Second
	tb := e.CreateTable("bench", 2) // 主索引 + 1 个二级索引
	s := e.Begin(isoRepeatableRead)
	for i := 0; i < benchRows; i++ {
		if err := e.Insert(s, tb, pk4(i), row8(i), []secKey{{idx: 1, key: pk4(i % 1000)}}); err != nil {
			b.Fatal(err)
		}
		if i%5000 == 4999 { // 分批提交，避免一个巨大的事务
			e.Commit(s)
			s = e.Begin(isoRepeatableRead)
		}
	}
	e.Commit(s)
	stop := e.StartPurger(20 * time.Millisecond)
	b.Cleanup(stop)
	return e, tb
}

func BenchmarkEnginePointRead(b *testing.B) {
	e, tb := benchEngine(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		tx := e.Begin(isoRepeatableRead)
		tx.StmtBegin(isoRepeatableRead)
		for pb.Next() {
			if _, err := e.GetRow(tx, tb, pk4(rng.Intn(benchRows)), lockNone); err != nil {
				b.Error(err)
				return
			}
		}
		e.Commit(tx)
	})
}

func BenchmarkEngineRangeScan100(b *testing.B) {
	e, tb := benchEngine(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		tx := e.Begin(isoRepeatableRead)
		tx.StmtBegin(isoRepeatableRead)
		for pb.Next() {
			c := e.OpenCursor(tx, tb, 0, lockNone, 4)
			_, err := c.Seek(pk4(rng.Intn(benchRows-200)), seekGE)
			for n := 1; err == nil && n < 100; n++ {
				_, err = c.Step(1)
			}
		}
		e.Commit(tx)
	})
}

var insertSeq atomic.Uint32

func BenchmarkEngineInsert(b *testing.B) {
	e, tb := benchEngine(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := int(insertSeq.Add(1)) << 22 // 每个 goroutine 独立的主键区间
		i := 0
		for pb.Next() {
			tx := e.Begin(isoRepeatableRead)
			tx.StmtBegin(isoRepeatableRead)
			k := benchRows + base + i
			if err := e.Insert(tx, tb, pk4(k), row8(k), []secKey{{idx: 1, key: pk4(k % 1000)}}); err != nil {
				b.Error(err)
				return
			}
			e.Commit(tx)
			i++
		}
	})
}

func BenchmarkEngineUpdateDifferentRows(b *testing.B) {
	e, tb := benchEngine(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			id := rng.Intn(benchRows)
			tx := e.Begin(isoRepeatableRead)
			tx.StmtBegin(isoRepeatableRead)
			c := e.OpenCursor(tx, tb, 0, lockX, 4)
			r, err := c.SeekFlags(pk4(id), seekExact, seekUniqueFull)
			if err != nil {
				b.Error(err)
				return
			}
			nv := int(binary.BigEndian.Uint64(r)) + 1
			if err := e.Update(tx, tb, pk4(id), pk4(id), row8(nv), []secKey{{idx: 1, key: pk4(id % 1000)}}); err != nil {
				b.Error(err)
				return
			}
			e.Commit(tx)
		}
	})
}

func BenchmarkEngineMixed90_10(b *testing.B) {
	e, tb := benchEngine(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			id := rng.Intn(benchRows)
			tx := e.Begin(isoRepeatableRead)
			tx.StmtBegin(isoRepeatableRead)
			if rng.Intn(10) == 0 {
				c := e.OpenCursor(tx, tb, 0, lockX, 4)
				r, err := c.SeekFlags(pk4(id), seekExact, seekUniqueFull)
				if err == nil {
					e.Update(tx, tb, pk4(id), pk4(id), row8(int(binary.BigEndian.Uint64(r))+1), []secKey{{idx: 1, key: pk4(id % 1000)}})
				}
			} else {
				e.GetRow(tx, tb, pk4(id), lockNone)
			}
			e.Commit(tx)
		}
	})
}
