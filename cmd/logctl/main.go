// Command logctl inspects a batch-commit-log directory.
package main

import (
	"flag"
	"fmt"
	"os"

	batchlog "github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func main() {
	fs := flag.NewFlagSet("logctl", flag.ExitOnError)
	dir := fs.String("dir", "", "log directory")
	fs.Parse(os.Args[1:])

	args := fs.Args()
	if *dir == "" || len(args) != 1 || args[0] != "stat" {
		fmt.Fprintln(os.Stderr, "usage: logctl --dir <path> stat")
		os.Exit(2)
	}

	l, err := batchlog.Open(*dir, batchlog.Options{SegmentBytes: 64 << 20})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer l.Close()

	segments := l.Segments()
	var first, last uint64
	for i, s := range segments {
		if i == 0 || s.FirstSeq < first {
			first = s.FirstSeq
		}
		if s.LastSeq > last {
			last = s.LastSeq
		}
	}
	fmt.Printf("{\"segments\":%d,\"firstSeq\":%d,\"lastSeq\":%d}\n", len(segments), first, last)
}
