package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	bcl "github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestStatEmpty(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--dir", t.TempDir(), "stat"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stat output is not JSON: %v (%q)", err, stdout.String())
	}
	if out["segments"] != float64(0) || out["last_seq"] != float64(0) || out["total_bytes"] != float64(0) {
		t.Fatalf("unexpected empty-log stat: %v", out)
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Fatalf("stat should be a single line, got %q", stdout.String())
	}
}

func TestStatPopulated(t *testing.T) {
	dir := t.TempDir()
	l, err := bcl.Open(dir, bcl.Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b, err := l.Append([][]byte{[]byte("payload-data"), []byte("x")})
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
	if out.Dir != dir || out.LastSeq != 3 || out.Segments < 2 || out.TotalBytes <= 0 {
		t.Fatalf("unexpected stat: %+v", out)
	}
}

func TestUsageErrorsExit2(t *testing.T) {
	cases := [][]string{
		{"--dir", t.TempDir()},              // missing command
		{"--dir", t.TempDir(), "bogus"},     // unknown command
		{"--dir", t.TempDir(), "stat", "x"}, // too many args
		{"stat"},                            // missing --dir
		{"--bogus", "stat"},                 // bad flag
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Fatalf("run(%v) exit = %d, want 2; stderr=%s", args, code, stderr.String())
		}
	}
}

func TestOpenErrorExit1(t *testing.T) {
	path := t.TempDir() + "/is-a-file"
	writeFile(t, path)
	var stdout, stderr bytes.Buffer
	code := run([]string{"--dir", path, "stat"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected a reason on stderr")
	}
}
