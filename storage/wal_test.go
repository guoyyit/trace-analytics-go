package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
)

func testSpan(traceID, spanID string, startMs int64) model.Span {
	return model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		ServiceName:   "svc",
		OperationName: "op",
		StartTime:     time.Unix(0, startMs*int64(time.Millisecond)).UTC(),
		Duration:      time.Millisecond,
		Status:        model.StatusOK,
	}
}

func TestWALAppendReplayAndTornTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ctx, model.Batch{Seq: 1, Spans: []model.Span{testSpan("t1", "a", 0)}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRejected(ctx, model.RejectedSpan{Index: 0, Reason: "bad"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ctx, model.Batch{Seq: 2, Spans: []model.Span{testSpan("t2", "b", 1)}}); err != nil {
		t.Fatal(err)
	}
	if w.LastSeq() != 2 {
		t.Fatalf("lastSeq = %d", w.LastSeq())
	}
	w.Close()

	// Append garbage to simulate a torn (uncommitted) trailing write.
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{frameMagic, 99, 0, 0, 0, 5, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	w2, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.LastSeq() != 2 {
		t.Fatalf("recovered lastSeq = %d, want 2 (torn tail dropped)", w2.LastSeq())
	}

	var seqs []uint64
	rejCount := 0
	err = w2.Replay(ctx, func(b model.Batch) error {
		seqs = append(seqs, b.Seq)
		return nil
	}, func(r model.RejectedSpan) {
		rejCount++
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs = %v", seqs)
	}
	if rejCount != 1 {
		t.Fatalf("rejected frames = %d", rejCount)
	}

	info, _ := os.Stat(filepath.Join(dir, walFileName))
	if info.Size() == 0 {
		t.Fatal("wal truncated too far")
	}
}

func TestOpenStoreRecoversCommittedBatches(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := OpenStore(ctx, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Commit(ctx, []model.Span{
		testSpan("t1", "root", 0),
		testSpan("t1", "child", 1),
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Commit(ctx, []model.Span{
		{}, // malformed: isolated, not committed
		testSpan("t2", "root2", 5),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(ctx, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v := s2.Current()
	if v.SpanCount() != 3 {
		t.Fatalf("recovered span count = %d, want 3", v.SpanCount())
	}
	if tr, ok := v.Trace("t1"); !ok || len(tr.Spans) != 2 || !tr.Complete {
		t.Fatalf("trace t1 not assembled: %+v", tr)
	}
	rejected, total := v.Rejected()
	if total != 1 || len(rejected) != 1 {
		t.Fatalf("rejected = %v total=%d", rejected, total)
	}
}

func TestOpenStoreCorruptSnapshotStillStarts(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := OpenStore(ctx, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Commit(ctx, []model.Span{testSpan("t1", "a", 0)}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Force a snapshot write directly then corrupt it.
	if err := atomicWriteSnapshot(dir, snapshotData{LastSeq: 1, Traces: []persistedTrace{{TraceID: "t1", Spans: []model.Span{testSpan("t1", "a", 0)}}}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotName), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(ctx, testCfg(dir))
	if err != nil {
		t.Fatalf("corrupt snapshot must not block startup: %v", err)
	}
	defer s2.Close()
	if s2.Current().SpanCount() != 1 {
		t.Fatalf("data recovered from WAL despite bad snapshot: %d", s2.Current().SpanCount())
	}
}

func TestRecoveryAfterSnapshotRollover(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg := testCfg(dir)
	cfg.SnapshotAfterBatches = 3

	s, err := OpenStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, _, err := s.Commit(ctx, []model.Span{testSpan("tr", string(rune('a'+i)), int64(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if s.Current().SpanCount() != 7 {
		t.Fatalf("pre-restart count = %d", s.Current().SpanCount())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	walInfo, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if walInfo.Size() == 0 {
		// Batches 3 and 6 were snapshotted; batch 7 must still be in the WAL.
		t.Fatal("post-rollover batch 7 missing from WAL")
	}

	s2, err := OpenStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v := s2.Current()
	if v.Seq() != 7 {
		t.Fatalf("recovered seq = %d, want 7", v.Seq())
	}
	if v.SpanCount() != 7 {
		t.Fatalf("recovered span count = %d, want 7", v.SpanCount())
	}
	tr, ok := v.Trace("tr")
	if !ok || !tr.Complete || len(tr.Spans) != 7 {
		t.Fatalf("recovered trace = %+v ok=%v", tr, ok)
	}

	// New writes continue with correct sequence after snapshot recovery.
	if _, _, err := s2.Commit(ctx, []model.Span{testSpan("tr2", "z", 50)}); err != nil {
		t.Fatal(err)
	}
	if s2.Current().Seq() != 8 {
		t.Fatalf("seq after post-restart commit = %d", s2.Current().Seq())
	}
}
