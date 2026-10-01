// Command logctl inspects a batch commit log.
//
// Usage:
//
//	logctl --dir <path> stat
//
// stat prints a single JSON object with the log directory, segment count,
// last committed sequence and total byte size.
package main

import (
	"encoding/json"
	"errors"
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
	dir := fs.String("dir", "", "log directory")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: logctl --dir <path> stat\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if fs.NArg() != 1 || fs.Arg(0) != "stat" {
		fmt.Fprintln(stderr, "error: expected a single command: stat")
		fs.Usage()
		return 2
	}
	if *dir == "" {
		fmt.Fprintln(stderr, "error: --dir is required")
		return 2
	}

	l, err := log.Open(*dir, log.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "error: open %s: %v\n", *dir, err)
		return 1
	}
	defer l.Close()

	var totalBytes int64
	var lastSeq uint64
	segs := l.Segments()
	for _, s := range segs {
		totalBytes += s.Bytes
		if s.LastSeq > lastSeq {
			lastSeq = s.LastSeq
		}
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
	enc := json.NewEncoder(stdout)
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(stderr, "error: write stat: %v\n", err)
		return 1
	}
	return 0
}
