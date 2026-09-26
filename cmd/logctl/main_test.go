package main_test

import (
	"os/exec"
	"testing"

	batchlog "github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func runStat(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("go", "run", ".", "--dir", dir, "stat")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("logctl stat: %v\n%s", err, out)
	}
	return string(out)
}

func TestStatEmptyDirectory(t *testing.T) {
	got := runStat(t, t.TempDir())
	want := "{\"segments\":0,\"firstSeq\":0,\"lastSeq\":0}\n"
	if got != want {
		t.Fatalf("stat = %q, want %q", got, want)
	}
}

func TestStatWithCommittedBatches(t *testing.T) {
	dir := t.TempDir()
	// A 5-byte record encodes to a 29-byte entry; capacity 40 rolls
	// every batch into its own segment.
	l, err := batchlog.Open(dir, batchlog.Options{SegmentBytes: 40})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 2; i++ {
		b, err := l.Append([][]byte{[]byte("hello")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	// A staged-but-uncommitted batch must not appear in stat.
	if _, err := l.Append([][]byte{[]byte("world")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := runStat(t, dir)
	want := "{\"segments\":2,\"firstSeq\":1,\"lastSeq\":2}\n"
	if got != want {
		t.Fatalf("stat = %q, want %q", got, want)
	}
}
