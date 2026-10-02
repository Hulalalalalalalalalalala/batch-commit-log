package main

import (
	"os/exec"
	"path/filepath"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func runStat(t *testing.T, dir string) string {
	t.Helper()
	out, err := exec.Command("go", "run", ".", "--dir", dir, "stat").CombinedOutput()
	if err != nil {
		t.Fatalf("logctl stat: %v\n%s", err, out)
	}
	return string(out)
}

func TestStatEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")
	got := runStat(t, dir)
	want := "{\"segments\":0,\"firstSeq\":0,\"lastSeq\":0}\n"
	if got != want {
		t.Fatalf("stat = %q, want %q", got, want)
	}
}

func TestStatAfterCommits(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, r := range []string{"a", "b", "c"} {
		b, err := l.Append([][]byte{[]byte(r)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	// A staged batch must not show up in stat.
	if _, err := l.Append([][]byte{[]byte("pending")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()

	got := runStat(t, dir)
	want := "{\"segments\":3,\"firstSeq\":1,\"lastSeq\":3}\n"
	if got != want {
		t.Fatalf("stat = %q, want %q", got, want)
	}
}

func TestStatAfterPrefixTruncation(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, r := range []string{"a", "b", "c"} {
		b, err := l.Append([][]byte{[]byte(r)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	// Reclaim every segment: stat then reports the fully-truncated shape.
	if n, err := l.DeleteThrough(3); err != nil || n != 3 {
		t.Fatalf("DeleteThrough = %d, %v; want 3, nil", n, err)
	}
	l.Close()

	got := runStat(t, dir)
	want := "{\"segments\":0,\"firstSeq\":0,\"lastSeq\":0}\n"
	if got != want {
		t.Fatalf("stat after truncation = %q, want %q", got, want)
	}

	// A partial truncation lists only retained segments.
	dir2 := t.TempDir()
	l, err = log.Open(dir2, log.Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, r := range []string{"a", "b", "c"} {
		b, _ := l.Append([][]byte{[]byte(r)})
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	if _, err := l.DeleteThrough(1); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	l.Close()
	got = runStat(t, dir2)
	want = "{\"segments\":2,\"firstSeq\":2,\"lastSeq\":3}\n"
	if got != want {
		t.Fatalf("partial stat = %q, want %q", got, want)
	}
}

func TestStatWithAbortedBatch(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := l.Append([][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// An aborted batch is a permanent hole: invisible to stat, and its
	// hole-only segment is not counted.
	ab, err := l.Append([][]byte{[]byte("dropped")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Abort(ab); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	l.Close()

	got := runStat(t, dir)
	want := "{\"segments\":1,\"firstSeq\":1,\"lastSeq\":1}\n"
	if got != want {
		t.Fatalf("stat = %q, want %q", got, want)
	}
}
