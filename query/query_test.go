package query

import (
	"context"
	"errors"
	"testing"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/store"
)

func newEngine(t *testing.T, history int) (*store.Store, *Engine) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.SyncWAL = false
	cfg.SnapshotHistory = history
	s, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, New(s, cfg)
}

func ingestSpan(t *testing.T, g *ingest.Ingester, sp model.Span) {
	t.Helper()
	if _, err := g.Ingest("batch", []model.Span{sp}); err != nil {
		t.Fatal(err)
	}
}

func TestPaginationStableAcrossWrites(t *testing.T) {
	s, e := newEngine(t, 100)
	g := ingest.New(s, config.Default())
	ctx := context.Background()

	// 5 traces at distinct start times.
	for i := 0; i < 5; i++ {
		ingestSpan(t, g, model.Span{
			TraceID: "t" + string(rune('0'+i)), SpanID: "s", Service: "svc",
			Operation: "op", StartUnixNano: int64(i * 10), DurationNano: 1,
			Status: model.StatusOK,
		})
	}

	f := model.TraceFilter{Limit: 2}
	p1, err := e.SearchTraces(ctx, f)
	if err != nil || len(p1.Traces) != 2 {
		t.Fatalf("page1: %v %d", err, len(p1.Traces))
	}

	// Writes happen during paging.
	for i := 5; i < 10; i++ {
		ingestSpan(t, g, model.Span{
			TraceID: "t" + string(rune('0'+i)), SpanID: "s", Service: "svc",
			Operation: "op", StartUnixNano: int64(i * 10), DurationNano: 1,
			Status: model.StatusOK,
		})
	}

	var seen []string
	seen = append(seen, p1.Traces[0].TraceID, p1.Traces[1].TraceID)
	cur := p1.NextCursor
	for cur != "" {
		page, err := e.SearchTraces(ctx, model.TraceFilter{Limit: 2, Cursor: cur})
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range page.Traces {
			for _, existing := range seen {
				if existing == tr.TraceID {
					t.Fatalf("duplicate trace across pages: %s", tr.TraceID)
				}
			}
			seen = append(seen, tr.TraceID)
		}
		cur = page.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("pinned snapshot must still return exactly 5 traces, got %d (%v)", len(seen), seen)
	}
}

func TestExpiredCursor(t *testing.T) {
	s, e := newEngine(t, 2)
	g := ingest.New(s, config.Default())
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ingestSpan(t, g, model.Span{
			TraceID: "t" + string(rune('0'+i)), SpanID: "s", Service: "svc",
			Operation: "op", StartUnixNano: int64(i), DurationNano: 1,
			Status: model.StatusOK,
		})
	}
	p1, err := e.SearchTraces(ctx, model.TraceFilter{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Enough further commits to prune the pinned snapshot.
	for i := 3; i < 20; i++ {
		ingestSpan(t, g, model.Span{
			TraceID: "t" + string(rune('0'+i)), SpanID: "s", Service: "svc",
			Operation: "op", StartUnixNano: int64(i), DurationNano: 1,
			Status: model.StatusOK,
		})
	}
	_, err = e.SearchTraces(ctx, model.TraceFilter{Limit: 1, Cursor: p1.NextCursor})
	if !errors.Is(err, model.ErrCursorExpired) {
		t.Fatalf("expected ErrCursorExpired, got %v", err)
	}
}

func TestCriticalPath(t *testing.T) {
	_, e := newEngine(t, 10)
	s := e.store
	g := ingest.New(s, config.Default())
	spans := []model.Span{
		{TraceID: "t", SpanID: "root", Service: "a", Operation: "root",
			StartUnixNano: 0, DurationNano: 100, Status: model.StatusOK},
		{TraceID: "t", SpanID: "slow", ParentSpanID: "root", Service: "b", Operation: "slow",
			StartUnixNano: 10, DurationNano: 80, Status: model.StatusOK},
		{TraceID: "t", SpanID: "fast", ParentSpanID: "root", Service: "c", Operation: "fast",
			StartUnixNano: 10, DurationNano: 20, Status: model.StatusOK},
	}
	for _, sp := range spans {
		ingestSpan(t, g, sp)
	}
	cp, err := e.CriticalPath(context.Background(), "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Spans) != 2 || cp.Spans[0].SpanID != "root" || cp.Spans[1].SpanID != "slow" {
		t.Fatalf("critical path must choose slow branch, got %+v", cp.Spans)
	}
}

func TestStatsAndDependencies(t *testing.T) {
	s, e := newEngine(t, 10)
	g := ingest.New(s, config.Default())
	spans := []model.Span{
		{TraceID: "t", SpanID: "root", Service: "a", Operation: "root",
			StartUnixNano: 0, DurationNano: 100, Status: model.StatusError},
		{TraceID: "t", SpanID: "c", ParentSpanID: "root", Service: "b", Operation: "call",
			StartUnixNano: 10, DurationNano: 50, Status: model.StatusOK},
	}
	for _, sp := range spans {
		ingestSpan(t, g, sp)
	}
	stats, err := e.Stats(context.Background(), model.StatsFilter{}, 1000, []float64{0.5})
	if err != nil || len(stats) != 2 {
		t.Fatalf("stats: %v %d", err, len(stats))
	}
	var rootStats *model.OperationStats
	for i := range stats {
		if stats[i].Service == "a" {
			rootStats = &stats[i]
		}
	}
	if rootStats == nil || rootStats.Buckets[0].ErrorRate != 1 {
		t.Fatalf("error rate: %+v", rootStats)
	}
	edges, err := e.Dependencies(context.Background(), model.StatsFilter{})
	if err != nil || len(edges) != 1 || edges[0].FromService != "a" || edges[0].ToService != "b" {
		t.Fatalf("edges: %v %+v", err, edges)
	}
}
