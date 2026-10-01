package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func TestStatEmpty(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"--dir", dir, "stat"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stat output not JSON: %v (%q)", err, stdout.String())
	}
	if strings.Count(strings.TrimRight(stdout.String(), "\n"), "\n") != 0 {
		t.Fatalf("stat output must be one line: %q", stdout.String())
	}
	if out["dir"] != dir {
		t.Errorf("dir = %v", out["dir"])
	}
	if out["segments"].(float64) != 0 {
		t.Errorf("segments = %v", out["segments"])
	}
	if out["last_seq"].(float64) != 0 {
		t.Errorf("empty last_seq = %v, want 0", out["last_seq"])
	}
	if out["total_bytes"].(float64) != 0 {
		t.Errorf("total_bytes = %v", out["total_bytes"])
	}
}

func TestStatPopulated(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 100, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b, err := l.Append([][]byte{bytes.Repeat([]byte("y"), 50)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"--dir", dir, "stat"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	var out struct {
		Dir        string `json:"dir"`
		Segments   int    `json:"segments"`
		LastSeq    uint64 `json:"last_seq"`
		TotalBytes int64  `json:"total_bytes"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.LastSeq != 3 {
		t.Errorf("last_seq = %d, want 3", out.LastSeq)
	}
	if out.Segments == 0 {
		t.Error("segments = 0, want > 0")
	}
	var sum int64
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".seg") {
			info, err := f.Info()
			if err != nil {
				t.Fatal(err)
			}
			sum += info.Size()
		}
	}
	if out.TotalBytes != sum {
		t.Errorf("total_bytes = %d, want %d", out.TotalBytes, sum)
	}
}

func TestStatUsageErrors(t *testing.T) {
	dir := t.TempDir()
	cases := [][]string{
		nil,
		{"--dir", dir},
		{"--dir", dir, "bogus"},
		{"--dir", dir, "stat", "extra"},
		{"stat"},
		{"--bogus-flag", "x", "stat"},
	}
	for i, args := range cases {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != 2 {
			t.Errorf("case %d args=%v: exit = %d, want 2; stderr=%s", i, args, code, stderr.String())
		}
	}
}

func TestStatOpenError(t *testing.T) {
	// A path that is an existing regular file cannot be a log dir.
	dir := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(dir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"--dir", dir, "stat"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected reason on stderr")
	}
}
