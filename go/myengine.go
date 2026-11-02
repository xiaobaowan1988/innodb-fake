package main

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"os"
	"runtime/cgo"
	"runtime/pprof"
	"sync"
	"unsafe"
)

var verbose = os.Getenv("MYENGINE_VERBOSE") != ""

func logf(format string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, "[go] "+format+"\n", args...)
	}
}

// 锁规则：
//   - mu 是读写锁：Get/Count/Cursor{Seek,Step,Key,Val} 拿读锁，可并行；
//     Create/Drop/Rename/Put/Delete 拿写锁，与一切读写互斥。
//   - 读锁下禁止修改任何 map 或树（所以只读路径用 treeRO，不用 tree）。
//   - 一个游标同一时刻只被一个 MySQL 线程使用，游标内部状态无需额外加锁。
var (
	mu     sync.RWMutex
	tables = map[string]*table{}
)

// getTable 不存在则创建（mysqld 重启后数据字典里有表而内存里没有，按空表处理）。写锁下调用。
func getTable(name string) *table {
	t := tables[name]
	if t == nil {
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

// lookup 在读锁下查表；调用者必须持有读锁。表不存在时返回空表（只读，不写入 map）。
func lookup(name string) *table {
	if t := tables[name]; t != nil {
		return t
	}
	return &table{trees: map[int]*BTree{}}
}

//export MyEngine_Count
func MyEngine_Count(name *C.char) C.longlong {
	mu.RLock()
	defer mu.RUnlock()
	return C.longlong(lookup(C.GoString(name)).treeRO(0).Len())
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
	mu.RLock()
	defer mu.RUnlock()
	v, ok := lookup(C.GoString(name)).treeRO(int(idx)).Get(bytesOf(key, klen))
	if !ok {
		return rcNotFound
	}
	return copyOut(v, out, capacity, outlen)
}

// 游标句柄用 runtime/cgo.Handle：查找无锁，不再经过全局 map。

//export MyEngine_CursorOpen
func MyEngine_CursorOpen(name *C.char, idx C.int) C.ulonglong {
	n := C.GoString(name)
	mu.RLock()
	t := tables[n]
	mu.RUnlock()
	if t == nil { // 正常情况下 handler.open 已创建；兜底
		mu.Lock()
		t = getTable(n)
		mu.Unlock()
	}
	return C.ulonglong(cgo.NewHandle(&cursor{tbl: t, idx: int(idx)}))
}

//export MyEngine_CursorClose
func MyEngine_CursorClose(cur C.ulonglong) C.int {
	if cur != 0 {
		cgo.Handle(cur).Delete()
	}
	return rcOK
}

func cur(h C.ulonglong) *cursor {
	if h == 0 {
		return nil
	}
	return cgo.Handle(h).Value().(*cursor)
}

//export MyEngine_CursorSeek
func MyEngine_CursorSeek(h C.ulonglong, key unsafe.Pointer, klen C.size_t, mode C.int) C.int {
	c := cur(h)
	if c == nil {
		return rcNotFound
	}
	mu.RLock()
	defer mu.RUnlock()
	return C.int(c.seek(bytesOf(key, klen), int(mode)))
}

// CursorStep: dir>0 下一条，dir<0 上一条。
//
//export MyEngine_CursorStep
func MyEngine_CursorStep(h C.ulonglong, dir C.int) C.int {
	c := cur(h)
	if c == nil {
		return rcNotFound
	}
	mu.RLock()
	defer mu.RUnlock()
	return C.int(c.step(int(dir)))
}

// CursorFetch 一次跨语言调用完成：移动游标(step: +1 下一条, -1 上一条, 0 不动) -> 必要时回表 -> 拷出整行。
// 返回 0 成功；1 已到尽头；4 二级索引条目对应的行不存在。
//
//export MyEngine_CursorFetch
func MyEngine_CursorFetch(h C.ulonglong, step C.int, pkWidth C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := cur(h)
	if c == nil {
		return rcNotFound
	}
	mu.RLock()
	defer mu.RUnlock()
	if step != 0 {
		if rc := c.step(int(step)); rc != rcOK {
			return C.int(rc)
		}
	}
	row, rc := c.fetchRow(int(pkWidth))
	if rc != rcOK {
		return C.int(rc)
	}
	return copyOut(row, out, capacity, outlen)
}

// CursorSeekFetch = CursorSeek + 取整行，同样只跨一次语言边界。
//
//export MyEngine_CursorSeekFetch
func MyEngine_CursorSeekFetch(h C.ulonglong, key unsafe.Pointer, klen C.size_t, mode C.int, pkWidth C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := cur(h)
	if c == nil {
		return rcNotFound
	}
	mu.RLock()
	defer mu.RUnlock()
	if rc := c.seek(bytesOf(key, klen), int(mode)); rc != rcOK {
		return C.int(rc)
	}
	row, rc := c.fetchRow(int(pkWidth))
	if rc != rcOK {
		return C.int(rc)
	}
	return copyOut(row, out, capacity, outlen)
}

//export MyEngine_CursorKey
func MyEngine_CursorKey(h C.ulonglong, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := cur(h)
	if c == nil || !c.valid {
		return rcNotFound
	}
	return copyOut(c.key, out, capacity, outlen) // key/val 已是游标自己持有的切片，无需加锁
}

//export MyEngine_CursorVal
func MyEngine_CursorVal(h C.ulonglong, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := cur(h)
	if c == nil || !c.valid {
		return rcNotFound
	}
	return copyOut(c.val, out, capacity, outlen)
}

// 性能剖析：设置 MYENGINE_CPUPROFILE=/path/cpu.prof 后，Go 库加载时开始采样；
// 调用 MyEngine_ProfileStop() 写出文件（c-shared 库没有退出钩子，需显式停止）。
var profFile *os.File

func init() {
	if p := os.Getenv("MYENGINE_CPUPROFILE"); p != "" {
		if f, err := os.Create(p); err == nil {
			profFile = f
			pprof.StartCPUProfile(f)
		}
	}
}

//export MyEngine_ProfileStop
func MyEngine_ProfileStop() {
	if profFile != nil {
		pprof.StopCPUProfile()
		profFile.Close()
		profFile = nil
	}
}

func main() {}
