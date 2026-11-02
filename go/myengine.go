package main

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime/cgo"
	"runtime/pprof"
	"strconv"
	"time"
	"unsafe"
)

// 本文件是 C++ handler 与 engine.go 之间的薄封装：只做类型转换和错误码映射。
// 句柄（事务、表、游标）都是 runtime/cgo.Handle，C++ 侧当作不透明的 64 位整数使用。

// 返回码
const (
	rcOK       = 0
	rcNotFound = 1 // 没有更多行 / 没找到
	rcDup      = 2 // 唯一键冲突，*dupIdx 为冲突的索引号
	rcTooSmall = 3 // 输出缓冲区不足
	rcDeadlock = 5
	rcTimeout  = 6
)

var eng = NewEngine()

func init() {
	eng.LockTimeout = 50 * time.Second // 与 InnoDB 默认值一致
	if v := os.Getenv("MYENGINE_LOCK_WAIT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			eng.LockTimeout = time.Duration(n) * time.Second
		}
	}
	eng.StartPurger(50 * time.Millisecond)
	if p := os.Getenv("MYENGINE_CPUPROFILE"); p != "" {
		if f, err := os.Create(p); err == nil {
			profFile = f
			pprof.StartCPUProfile(f)
		}
	}
}

// 性能剖析：MYENGINE_CPUPROFILE=/path/cpu.prof 时加载即开始采样，MyEngine_ProfileStop() 写出文件。
var profFile *os.File

//export MyEngine_ProfileStop
func MyEngine_ProfileStop() {
	if profFile != nil {
		pprof.StopCPUProfile()
		profFile.Close()
		profFile = nil
	}
}

func bytesOf(p unsafe.Pointer, n C.size_t) []byte {
	if p == nil || n == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(p), int(n))
}

func rcOf(err error, dupIdx *C.int) C.int {
	var d DupError
	switch {
	case err == nil:
		return rcOK
	case errors.As(err, &d):
		if dupIdx != nil {
			*dupIdx = C.int(d.Idx)
		}
		return rcDup
	case errors.Is(err, ErrDeadlock):
		return rcDeadlock
	case errors.Is(err, ErrLockTimeout):
		return rcTimeout
	}
	return rcNotFound
}

func txnOf(h C.ulonglong) *Txn   { return cgo.Handle(h).Value().(*Txn) }
func tblOf(h C.ulonglong) *table { return cgo.Handle(h).Value().(*table) }
func curOf(h C.ulonglong) *Cursor {
	if h == 0 {
		return nil
	}
	return cgo.Handle(h).Value().(*Cursor)
}

// ---------------------------------------------------------------- 表

//export MyEngine_Create
func MyEngine_Create(name *C.char, nkeys C.int) C.int {
	eng.CreateTable(C.GoString(name), int(nkeys))
	logf("Create(%s)", C.GoString(name))
	return rcOK
}

//export MyEngine_TableOpen
func MyEngine_TableOpen(name *C.char, nkeys C.int) C.ulonglong {
	tb := eng.OpenTable(C.GoString(name), int(nkeys))
	logf("Open(%s)", C.GoString(name))
	return C.ulonglong(cgo.NewHandle(tb))
}

//export MyEngine_TableClose
func MyEngine_TableClose(h C.ulonglong) {
	if h != 0 {
		cgo.Handle(h).Delete()
	}
}

//export MyEngine_Drop
func MyEngine_Drop(name *C.char) C.int {
	eng.DropTable(C.GoString(name))
	return rcOK
}

//export MyEngine_Rename
func MyEngine_Rename(from, to *C.char) C.int {
	eng.RenameTable(C.GoString(from), C.GoString(to))
	return rcOK
}

//export MyEngine_Count
func MyEngine_Count(h C.ulonglong) C.longlong { return C.longlong(eng.Count(tblOf(h))) }

// ---------------------------------------------------------------- 事务

//export MyEngine_TxnBegin
func MyEngine_TxnBegin(iso C.int) C.ulonglong {
	return C.ulonglong(cgo.NewHandle(eng.Begin(int(iso))))
}

//export MyEngine_TxnStmtBegin
func MyEngine_TxnStmtBegin(h C.ulonglong, iso C.int) { txnOf(h).StmtBegin(int(iso)) }

// TxnEnd 结束事务并释放句柄：commit != 0 提交，否则回滚。
//
//export MyEngine_TxnEnd
func MyEngine_TxnEnd(h C.ulonglong, commit C.int) {
	t := txnOf(h)
	if commit != 0 {
		eng.Commit(t)
	} else {
		eng.Rollback(t)
	}
	cgo.Handle(h).Delete()
}

//export MyEngine_TxnRollbackStmt
func MyEngine_TxnRollbackStmt(h C.ulonglong) { eng.RollbackStmt(txnOf(h)) }

//export MyEngine_TxnSavepoint
func MyEngine_TxnSavepoint(h C.ulonglong) C.ulonglong { return C.ulonglong(txnOf(h).Savepoint()) }

//export MyEngine_TxnRollbackTo
func MyEngine_TxnRollbackTo(h C.ulonglong, sp C.ulonglong) {
	eng.RollbackToSavepoint(txnOf(h), int(sp))
}

// ---------------------------------------------------------------- 行操作

// 二级索引键打包格式：重复 [idx u32][flags u32 (bit0=唯一)][len u32][key bytes]，小端。
func parseSec(p unsafe.Pointer, n C.size_t) []secKey {
	b := bytesOf(p, n)
	var out []secKey
	for len(b) >= 12 {
		idx := binary.LittleEndian.Uint32(b)
		flags := binary.LittleEndian.Uint32(b[4:])
		kl := int(binary.LittleEndian.Uint32(b[8:]))
		out = append(out, secKey{idx: int(idx), key: b[12 : 12+kl], unique: flags&1 != 0})
		b = b[12+kl:]
	}
	return out
}

//export MyEngine_RowInsert
func MyEngine_RowInsert(th, tbh C.ulonglong, pk unsafe.Pointer, pklen C.size_t, row unsafe.Pointer, rowlen C.size_t, sec unsafe.Pointer, seclen C.size_t, dupIdx *C.int) C.int {
	return rcOf(eng.Insert(txnOf(th), tblOf(tbh), bytesOf(pk, pklen), bytesOf(row, rowlen), parseSec(sec, seclen)), dupIdx)
}

//export MyEngine_RowUpdate
func MyEngine_RowUpdate(th, tbh C.ulonglong, oldpk unsafe.Pointer, oldlen C.size_t, newpk unsafe.Pointer, newlen C.size_t, row unsafe.Pointer, rowlen C.size_t, sec unsafe.Pointer, seclen C.size_t, dupIdx *C.int) C.int {
	return rcOf(eng.Update(txnOf(th), tblOf(tbh), bytesOf(oldpk, oldlen), bytesOf(newpk, newlen), bytesOf(row, rowlen), parseSec(sec, seclen)), dupIdx)
}

//export MyEngine_RowDelete
func MyEngine_RowDelete(th, tbh C.ulonglong, pk unsafe.Pointer, pklen C.size_t) C.int {
	return rcOf(eng.Delete(txnOf(th), tblOf(tbh), bytesOf(pk, pklen)), nil)
}

func copyOut(src []byte, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	*outlen = C.size_t(len(src))
	if len(src) > int(capacity) {
		return rcTooSmall
	}
	copy(unsafe.Slice((*byte)(out), int(capacity)), src)
	return rcOK
}

// GetRow 按主键读一行；lockMode: 0 一致性读，1 S 锁，2 X 锁。
//
//export MyEngine_GetRow
func MyEngine_GetRow(th, tbh C.ulonglong, pk unsafe.Pointer, pklen C.size_t, lockMode C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	row, err := eng.GetRow(txnOf(th), tblOf(tbh), bytesOf(pk, pklen), int(lockMode))
	if err != nil {
		return rcOf(err, nil)
	}
	return copyOut(row, out, capacity, outlen)
}

// ---------------------------------------------------------------- 游标

//export MyEngine_CursorOpen
func MyEngine_CursorOpen(th, tbh C.ulonglong, idx C.int, lockMode C.int, pkWidth C.int) C.ulonglong {
	c := eng.OpenCursor(txnOf(th), tblOf(tbh), int(idx), int(lockMode), int(pkWidth))
	return C.ulonglong(cgo.NewHandle(c))
}

//export MyEngine_CursorClose
func MyEngine_CursorClose(h C.ulonglong) {
	if h != 0 {
		cgo.Handle(h).Delete()
	}
}

func finishFetch(row []byte, err error, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	if err != nil {
		return rcOf(err, nil)
	}
	return copyOut(row, out, capacity, outlen)
}

// CursorSeekFetch 定位并取一行整行；一次跨语言调用。
// flags 位 0：唯一索引全键等值（命中且行存在时只加记录锁，不登记间隙锁）。
//
//export MyEngine_CursorSeekFetch
func MyEngine_CursorSeekFetch(h C.ulonglong, key unsafe.Pointer, klen C.size_t, mode C.int, flags C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := curOf(h)
	if c == nil {
		return rcNotFound
	}
	row, err := c.SeekFlags(bytesOf(key, klen), int(mode), int(flags))
	return finishFetch(row, err, out, capacity, outlen)
}

// CursorBound 设置扫描边界：kind 0 清除，1 同键前缀，2 正向终点，3 反向终点；unique 表示终点是唯一索引的全键。
// 越过边界的第一个条目只会让它前面的间隙被锁住（RR/SERIALIZABLE 的加锁读），不会锁它的行。
//
//export MyEngine_CursorBound
func MyEngine_CursorBound(h C.ulonglong, kind C.int, key unsafe.Pointer, klen C.size_t, inclusive C.int, unique C.int) {
	if c := curOf(h); c != nil {
		c.SetBoundU(int(kind), bytesOf(key, klen), inclusive != 0, unique != 0)
	}
}

// CursorFetch 沿 step 方向(+1/-1)移动并取一行整行；一次跨语言调用。
//
//export MyEngine_CursorFetch
func MyEngine_CursorFetch(h C.ulonglong, step C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	c := curOf(h)
	if c == nil {
		return rcNotFound
	}
	row, err := c.Step(int(step))
	return finishFetch(row, err, out, capacity, outlen)
}

// ---------------------------------------------------------------- 自增

// AutoReserve 预留 n 个自增值，返回 0 并写出第一个值；返回 1 表示计数器还没初始化。
//
//export MyEngine_AutoReserve
func MyEngine_AutoReserve(tbh C.ulonglong, offset, increment, n C.ulonglong, first *C.ulonglong) C.int {
	f, ok := tblOf(tbh).AutoReserve(uint64(offset), uint64(increment), uint64(n))
	if !ok {
		return 1
	}
	*first = C.ulonglong(f)
	return rcOK
}

//export MyEngine_AutoInit
func MyEngine_AutoInit(tbh C.ulonglong, max C.ulonglong) { tblOf(tbh).AutoInit(uint64(max)) }

//export MyEngine_AutoBump
func MyEngine_AutoBump(tbh C.ulonglong, v C.ulonglong) { tblOf(tbh).AutoBump(uint64(v)) }

// IndexLastLatest 返回索引 idx 上"最大键对应的、最新且未删除的行"（不管事务可见性），用于初始化自增计数器。
//
//export MyEngine_IndexLastLatest
func MyEngine_IndexLastLatest(tbh C.ulonglong, idx C.int, pkWidth C.int, out unsafe.Pointer, capacity C.size_t, outlen *C.size_t) C.int {
	row, ok := eng.LastLatest(tblOf(tbh), int(idx), int(pkWidth))
	if !ok {
		return rcNotFound
	}
	return copyOut(row, out, capacity, outlen)
}

// RecordsInRange 估算范围内的条目数，供优化器选择执行计划；最多数 limit 个。
//
//export MyEngine_RecordsInRange
func MyEngine_RecordsInRange(tbh C.ulonglong, idx C.int, lo unsafe.Pointer, lolen C.size_t, hasLo, loIncl C.int, hi unsafe.Pointer, hilen C.size_t, hasHi, hiIncl C.int, limit C.int) C.longlong {
	n, _ := eng.RecordsInRange(tblOf(tbh), int(idx), bytesOf(lo, lolen), hasLo != 0, loIncl != 0, bytesOf(hi, hilen), hasHi != 0, hiIncl != 0, int(limit))
	return C.longlong(n)
}

func main() {}
