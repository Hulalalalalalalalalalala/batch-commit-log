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
