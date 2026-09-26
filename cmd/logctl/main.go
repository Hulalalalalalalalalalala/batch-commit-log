// Command logctl inspects a batch-commit log directory.
//
//	logctl --dir <path> stat
//
// stat prints one compact JSON line summarising the directory:
//
//	{"segments":2,"firstSeq":1,"lastSeq":7}
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	bcl "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func main() {
	fs := flag.NewFlagSet("logctl", flag.ExitOnError)
	dir := fs.String("dir", "", "path to the log directory")
	fs.Parse(os.Args[1:])
	if *dir == "" || fs.NArg() != 1 || fs.Arg(0) != "stat" {
		fmt.Fprintln(os.Stderr, "usage: logctl --dir <path> stat")
		os.Exit(2)
	}

	l, err := bcl.Open(*dir, bcl.Options{SegmentBytes: 64 << 20})
	if err != nil {
		fmt.Fprintln(os.Stderr, "logctl:", err)
		os.Exit(1)
	}
	defer l.Close()

	segs := l.Segments()
	var first, last uint64
	for i, s := range segs {
		if i == 0 || s.FirstSeq < first {
			first = s.FirstSeq
		}
		if i == 0 || s.LastSeq > last {
			last = s.LastSeq
		}
	}
	out := struct {
		Segments int    `json:"segments"`
		FirstSeq uint64 `json:"firstSeq"`
		LastSeq  uint64 `json:"lastSeq"`
	}{Segments: len(segs), FirstSeq: first, LastSeq: last}
	line, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logctl:", err)
		os.Exit(1)
	}
	fmt.Println(string(line))
}
