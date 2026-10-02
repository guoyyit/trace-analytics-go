package store

import (
	"os"
	"testing"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
)

func appendLine(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(data, '\n'))
	return err
}

func testCfg(dir string) *config.Config {
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.SyncWAL = false
	cfg.SnapshotHistory = 4
	return cfg
}

func TestCommitOutOfOrderDuplicateAndRead(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	child := model.Span{TraceID: "t1", SpanID: "c", ParentSpanID: "p", Service: "svcB",
		Operation: "op", StartUnixNano: 100, DurationNano: 20, Status: model.StatusOK}
	parent := model.Span{TraceID: "t1", SpanID: "p", Service: "svcA",
		Operation: "root", StartUnixNano: 0, DurationNano: 100, Status: model.StatusError}

	acc, rej, err := s.Commit(&Batch{ID: "b1", Spans: []model.Span{child}})
	if err != nil || acc != 1 {
		t.Fatalf("child commit: acc=%d err=%v", acc, err)
	}
	// Late-arriving parent.
	if _, _, err := s.Commit(&Batch{ID: "b2", Spans: []model.Span{parent}}); err != nil {
		t.Fatal(err)
	}
	// Exact duplicate is isolated as DUPLICATE.
	acc, rej, err = s.Commit(&Batch{ID: "b3", Spans: []model.Span{parent}})
	if err != nil || acc != 0 || len(rej) != 1 || rej[0].Reason != model.RejectDuplicate {
		t.Fatalf("dup: acc=%d rej=%v err=%v", acc, rej, err)
	}

	snap := s.Current()
	tr, ok := snap.Trace("t1")
	if !ok || len(tr.Spans) != 2 || tr.MissingParents != 0 || !tr.HasError {
		t.Fatalf("trace assemble: %+v ok=%v", tr, ok)
	}
	if got := snap.TraceIDsByService("svcA"); len(got) != 1 || got[0] != "t1" {
		t.Fatalf("service index: %v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: data survives and duplicates still detected.
	s2, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	snap2 := s2.Current()
	if snap2.TotalSpans() != 2 {
		t.Fatalf("total spans after restart: %d", snap2.TotalSpans())
	}
	_, rej2, _ := s2.Commit(&Batch{ID: "b4", Spans: []model.Span{parent}})
	if len(rej2) != 1 || rej2[0].Reason != model.RejectDuplicate {
		t.Fatalf("dup after restart: %v", rej2)
	}
}

func TestCorruptTailDoesNotBlockStartup(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	sp := model.Span{TraceID: "t", SpanID: "s", Service: "x", Operation: "y",
		StartUnixNano: 1, DurationNano: 1, Status: model.StatusOK}
	if _, _, err := s.Commit(&Batch{ID: "ok", Spans: []model.Span{sp}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Append garbage to the log (simulates a torn tail write).
	if err := appendLine(dir+"/wal.log", []byte("{not-json-torn")); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatalf("startup must survive corrupt tail: %v", err)
	}
	defer s2.Close()
	if s2.ReplayDropped() < 1 {
		t.Fatalf("expected dropped corrupt line, got %d", s2.ReplayDropped())
	}
	if s2.Current().TotalSpans() != 1 {
		t.Fatal("committed span must survive")
	}
}

func TestCrashRecoveryWithoutGracefulClose(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(dir)
	cfg.SyncWAL = true

	// First "process": sync every batch, then exit without Close.
	s1, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		sp := model.Span{
			TraceID: traceID(i), SpanID: "s", Service: "svc", Operation: "op",
			StartUnixNano: int64(i), DurationNano: 1, Status: model.StatusOK,
		}
		if _, _, err := s1.Commit(&Batch{ID: "b", Spans: []model.Span{sp}}); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a crash: drop the handle without closing (fsync already done).

	s2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s2.Current().TotalSpans() != 25 {
		t.Fatalf("recovered spans: %d", s2.Current().TotalSpans())
	}
	if len(s2.Current().AllTraceIDs()) != 25 {
		t.Fatal("not all traces recovered")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

func traceID(i int) string {
	return "trace-" + string(rune('a'+i/10)) + string(rune('a'+i%10))
}
