package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.824/mr"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		fmt.Fprintf(os.Stderr, "没有参与呢...\n")
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	fmt.Fprintf(os.Stderr, "coor退出...\n")

}
