package main

import (
	"fmt"
	"os"
)

var verbose = os.Getenv("MYENGINE_VERBOSE") != ""

func logf(format string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, "[go] "+format+"\n", args...)
	}
}
