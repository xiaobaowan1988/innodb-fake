package main

/*
#include <stddef.h>
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// 全量 Mock：不做任何真实存储，只打印日志并返回 0(成功) / 1(失败)。

func logf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[go] "+format+"\n", args...)
}

//export MyEngine_Open
func MyEngine_Open(name *C.char) C.int {
	logf("Open(%s)", C.GoString(name))
	return 0
}

//export MyEngine_Close
func MyEngine_Close(name *C.char) C.int {
	logf("Close(%s)", C.GoString(name))
	return 0
}

//export MyEngine_Put
func MyEngine_Put(key unsafe.Pointer, klen C.size_t, val unsafe.Pointer, vlen C.size_t) C.int {
	logf("Put(klen=%d, vlen=%d) -> swallowed", int(klen), int(vlen))
	return 0
}

//export MyEngine_Seek
func MyEngine_Seek(key unsafe.Pointer, klen C.size_t) C.int {
	logf("Seek(klen=%d) -> mock success, no row", int(klen))
	return 0
}

func main() {}
