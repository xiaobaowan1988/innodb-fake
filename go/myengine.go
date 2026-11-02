package main

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"unsafe"
)

// 返回码约定：0 成功；1 未找到/失败；2 主键重复；3 缓冲区不足。
const (
	rcOK       = 0
	rcNotFound = 1
	rcDup      = 2
	rcTooSmall = 3
)

// Seek 模式（与 C++ 侧 ha_rkey_function 的映射在 ha_myengine.cc 中完成）。
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

var verbose = os.Getenv("MYENGINE_VERBOSE") != ""

func logf(format string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, "[go] "+format+"\n", args...)
	}
}

type table struct {
	trees map[int]*BTree // idx -> B+ 树；0 为主索引(key=主键, val=整行)
}

func (t *table) tree(idx int) *BTree {
	bt := t.trees[idx]
	if bt == nil {
		bt = NewBTree()
		t.trees[idx] = bt
	}
	return bt
}

type cursor struct {
	tbl   *table
	idx   int
	key   []byte
	val   []byte
	valid bool
}

var (
	mu        sync.Mutex // 全局大锁：简单优先，后续可细化
	tables    = map[string]*table{}
	cursors   = map[uint64]*cursor{}
	nextCurID uint64
)

func getTable(name string) *table {
	t := tables[name]
	if t == nil { // mysqld 重启后数据字典里有表而内存里没有，按空表处理
		t = &table{trees: map[int]*BTree{}}
		tables[name] = t
	}
	return t
}

func bytesOf(p unsafe.Pointer, n C.size_t) []byte {
	if p == nil || n == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(p), int(n))
}

//export MyEngine_Create
func MyEngine_Create(name *C.char) C.int {
	mu.Lock()
	defer mu.Unlock()
	tables[C.GoString(name)] = &table{trees: map[int]*BTree{}}
	logf("Create(%s)", C.GoString(name))
	return rcOK
}

//export MyEngine_Open
func MyEngine_Open(name *C.char) C.int {
	mu.Lock()
	defer mu.Unlock()
	getTable(C.GoString(name))
	logf("Open(%s)", C.GoString(name))
	return rcOK
}

//export MyEngine_Close
func MyEngine_Close(name *C.char) C.int { return rcOK }

//export MyEngine_Drop
func MyEngine_Drop(name *C.char) C.int {
	mu.Lock()
	defer mu.Unlock()
	delete(tables, C.GoString(name))
	logf("Drop(%s)", C.GoString(name))
	return rcOK
}

//export MyEngine_Rename
func MyEngine_Rename(from, to *C.char) C.int {
	mu.Lock()
	defer mu.Unlock()
	f, t := C.GoString(from), C.GoString(to)
	if tb, ok := tables[f]; ok {
		tables[t] = tb
		delete(tables, f)
	}
	logf("Rename(%s -> %s)", f, t)
	return rcOK
}

//export MyEngine_Count
func MyEngine_Count(name *C.char) C.longlong {
	mu.Lock()
	defer mu.Unlock()
	return C.longlong(getTable(C.GoString(name)).tree(0).Len())
}

//export MyEngine_Put
func MyEngine_Put(name *C.char, idx C.int, key unsafe.Pointer, klen C.size_t, val unsafe.Pointer, vlen C.size_t, overwrite C.int) C.int {
	mu.Lock()
	defer mu.Unlock()
	t := getTable(C.GoString(name)).tree(int(idx))
	if !t.Put(bytesOf(key, klen), bytesOf(val, vlen), overwrite != 0) {
		return rcDup
	}
	return rcOK
}

//export MyEngine_Delete
func MyEngine_Delete(name *C.char, idx C.int, key unsafe.Pointer, klen C.size_t) C.int {
	mu.Lock()
	defer mu.Unlock()
	if !getTable(C.GoString(name)).tree(int(idx)).Delete(bytesOf(key, klen)) {
		return rcNotFound
	}
	return rcOK
}

func copyOut(src []byte, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	*outlen = C.size_t(len(src))
	if len(src) > int(capacity) {
		return rcTooSmall
	}
	copy(unsafe.Slice((*byte)(out), int(capacity)), src)
	return rcOK
}

//export MyEngine_Get
func MyEngine_Get(name *C.char, idx C.int, key unsafe.Pointer, klen C.size_t, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	mu.Lock()
	defer mu.Unlock()
	v, ok := getTable(C.GoString(name)).tree(int(idx)).Get(bytesOf(key, klen))
	if !ok {
		return rcNotFound
	}
	return copyOut(v, out, capacity, outlen)
}

//export MyEngine_CursorOpen
func MyEngine_CursorOpen(name *C.char, idx C.int) C.ulonglong {
	mu.Lock()
	defer mu.Unlock()
	nextCurID++
	cursors[nextCurID] = &cursor{tbl: getTable(C.GoString(name)), idx: int(idx)}
	return C.ulonglong(nextCurID)
}

//export MyEngine_CursorClose
func MyEngine_CursorClose(cur C.ulonglong) C.int {
	mu.Lock()
	defer mu.Unlock()
	delete(cursors, uint64(cur))
	return rcOK
}

func (c *cursor) set(p pos) C.int {
	if !p.valid() {
		c.valid = false
		return rcNotFound
	}
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

//export MyEngine_CursorSeek
func MyEngine_CursorSeek(cur C.ulonglong, key unsafe.Pointer, klen C.size_t, mode C.int) C.int {
	mu.Lock()
	defer mu.Unlock()
	c := cursors[uint64(cur)]
	if c == nil {
		return rcNotFound
	}
	t := c.tbl.tree(c.idx)
	k := append([]byte(nil), bytesOf(key, klen)...)
	switch int(mode) {
	case seekExact:
		p := t.LowerBound(k)
		if p.valid() && bytes.HasPrefix(p.key(), k) {
			return c.set(p)
		}
		return c.set(pos{})
	case seekGE:
		return c.set(t.LowerBound(k))
	case seekAfter:
		if s := prefixSucc(k); s != nil {
			return c.set(t.LowerBound(s))
		}
		return c.set(pos{})
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
		if p.valid() && bytes.HasPrefix(p.key(), k) {
			return c.set(p)
		}
		return c.set(pos{})
	}
	return rcNotFound
}

// CursorStep: dir>0 下一条，dir<0 上一条。按 key 重新定位，扫描期间并发增删不会失效。
//
//export MyEngine_CursorStep
func MyEngine_CursorStep(cur C.ulonglong, dir C.int) C.int {
	mu.Lock()
	defer mu.Unlock()
	c := cursors[uint64(cur)]
	if c == nil || !c.valid {
		return rcNotFound
	}
	t := c.tbl.tree(c.idx)
	if dir > 0 {
		return c.set(t.UpperBound(c.key))
	}
	return c.set(t.LastLess(c.key))
}

//export MyEngine_CursorKey
func MyEngine_CursorKey(cur C.ulonglong, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	mu.Lock()
	defer mu.Unlock()
	c := cursors[uint64(cur)]
	if c == nil || !c.valid {
		return rcNotFound
	}
	return copyOut(c.key, out, capacity, outlen)
}

//export MyEngine_CursorVal
func MyEngine_CursorVal(cur C.ulonglong, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	mu.Lock()
	defer mu.Unlock()
	c := cursors[uint64(cur)]
	if c == nil || !c.valid {
		return rcNotFound
	}
	return copyOut(c.val, out, capacity, outlen)
}

func main() {}
