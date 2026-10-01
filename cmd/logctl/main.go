// Command logctl inspects a batch commit log directory.
//
// Usage:
//
//	logctl --dir <path> stat
//
// stat prints a single line of JSON with the directory, segment count,
// last committed sequence and total on-disk byte size.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("logctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: logctl --dir <path> stat")
		fs.PrintDefaults()
	}
	dir := fs.String("dir", "", "log directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 || fs.Arg(0) != "stat" {
		fmt.Fprintln(stderr, "logctl: expected exactly one command: stat")
		fs.Usage()
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "logctl: --dir is required")
		return 2
	}

	l, err := log.Open(*dir, log.Options{Sync: true})
	if err != nil {
		fmt.Fprintf(stderr, "logctl: open %s: %v\n", *dir, err)
		return 1
	}
	defer l.Close()

	var lastSeq uint64
	var totalBytes int64
	segs := l.Segments()
	for _, s := range segs {
		if s.LastSeq > lastSeq {
			lastSeq = s.LastSeq
		}
		totalBytes += s.Bytes
	}

	out := struct {
		Dir        string `json:"dir"`
		Segments   int    `json:"segments"`
		LastSeq    uint64 `json:"last_seq"`
		TotalBytes int64  `json:"total_bytes"`
	}{
		Dir:        *dir,
		Segments:   len(segs),
		LastSeq:    lastSeq,
		TotalBytes: totalBytes,
	}
	if err := json.NewEncoder(stdout).Encode(out); err != nil {
		fmt.Fprintf(stderr, "logctl: %v\n", err)
		return 1
	}
	return 0
}
