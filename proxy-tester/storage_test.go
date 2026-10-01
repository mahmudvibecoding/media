package main

import (
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustAddress(value string) netip.Addr { return netip.MustParseAddr(value) }

func TestJournalRecoveryAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.jsonl")
	result := Result{ID: 1, Key: strings.Repeat("a", 64), Status: "not_responding"}
	line, _ := json.Marshal(result)
	line = append(line, '\n')
	os.WriteFile(path, append(append([]byte{}, line...), []byte(`{"id":2`)...), 0600)
	count := 0
	done, err := loadJournal(path, func(Result) { count++ })
	if err != nil || len(done) != 1 || count != 1 {
		t.Fatalf("recovery failed: %v", err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != string(line) {
		t.Fatal("did not preserve complete record and discard incomplete tail")
	}
	os.WriteFile(path, append(line, []byte("broken\n")...), 0600)
	if _, err := loadJournal(path, func(Result) {}); err == nil {
		t.Fatal("accepted corrupt complete record")
	}
}

func TestResumeSkipsCompletedAndRejectsDifferentProbe(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.jsonl")
	output := filepath.Join(dir, "results.jsonl")
	c := Candidate{ID: 1, Key: strings.Repeat("a", 64), Address: "example.com", Port: 443, Protocol: "mtproto"}
	data, _ := json.Marshal(c)
	os.WriteFile(input, append(data, '\n'), 0600)
	args := []string{"--input", input, "--output", output, "--run-id", "test", "--concurrency", "2"}
	for range 2 {
		if code, err := run(args); err != nil || code != 0 {
			t.Fatalf("run failed: %d %v", code, err)
		}
	}
	journal, _ := os.ReadFile(output)
	if strings.Count(string(journal), "\n") != 1 {
		t.Fatal("resume duplicated a completed result")
	}
	if _, err := run(append(args, "--video-id", "aaaaaaaaaaa")); err == nil {
		t.Fatal("accepted changed probe on resume")
	}
}
