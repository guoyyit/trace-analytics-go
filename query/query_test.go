package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/service"
)

func spanAt(traceID, id, svc, op string, startMs int64, dur time.Duration, tags map[string]string, status model.Status) model.Span {
	return model.Span{
		TraceID:       traceID,
		SpanID:        id,
		ServiceName:   svc,
		OperationName: op,
		StartTime:     time.Unix(0, startMs*int64(time.Millisecond)).UTC(),
		Duration:      dur,
		Tags:          tags,
		Status:        status,
	}
}

func newService(t *testing.T) *service.Service {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SyncWAL = false
	cfg.MaxPinnedViews = 64
	svc, err := service.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc
}

func TestSearchFilterCombinations(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	var spans []model.Span
	// trace A: svcX/op1, 100ms, one error span, tagged
	spans = append(spans,
		spanAt("A", "A1", "svcX", "op1", 0, 100*time.Millisecond, map[string]string{"env": "prod", "user": "u1"}, model.StatusError),
		spanAt("B", "B1", "svcY", "op2", 1000, 20*time.Millisecond, map[string]string{"env": "test"}, model.StatusOK),
		spanAt("C", "C1", "svcX", "op3", 2000, 5*time.Millisecond, nil, model.StatusOK),
	)
	if _, err := svc.Ingest(ctx, spans); err != nil {
		t.Fatal(err)
	}

	ids := func(f query.Filter) []string {
		res, err := svc.Search(ctx, f, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(res.Traces))
		for i, tr := range res.Traces {
			out[i] = tr.TraceID
		}
		return out
	}

	if got := ids(query.Filter{ServiceName: "svcX"}); len(got) != 2 {
		t.Fatalf("service filter = %v", got)
	}
	if got := ids(query.Filter{ServiceName: "svcX", OperationName: "op1"}); len(got) != 1 || got[0] != "A" {
		t.Fatalf("service+operation = %v", got)
	}
	if got := ids(query.Filter{TagKey: "env", TagValue: "prod"}); len(got) != 1 || got[0] != "A" {
		t.Fatalf("tag filter = %v", got)
	}
	if got := ids(query.Filter{OnlyErrors: true}); len(got) != 1 || got[0] != "A" {
		t.Fatalf("error filter = %v", got)
	}
	if got := ids(query.Filter{MinDuration: 50 * time.Millisecond}); len(got) != 1 || got[0] != "A" {
		t.Fatalf("minDuration = %v", got)
	}
	if got := ids(query.Filter{MaxDuration: 10 * time.Millisecond}); len(got) != 1 || got[0] != "C" {
		t.Fatalf("maxDuration = %v", got)
	}
	if got := ids(query.Filter{StartTime: time.Unix(0, 500*int64(time.Millisecond)).UTC(), EndTime: time.Unix(0, 1500*int64(time.Millisecond)).UTC()}); len(got) != 1 || got[0] != "B" {
		t.Fatalf("time window = %v", got)
	}
	if got := ids(query.Filter{ServiceName: "svcX", OnlyErrors: true}); len(got) != 1 || got[0] != "A" {
		t.Fatalf("AND combination = %v", got)
	}
	if got := ids(query.Filter{ServiceName: "svcY", OnlyErrors: true}); len(got) != 0 {
		t.Fatalf("no match expected = %v", got)
	}
}

func TestSearchPaginationStableAcrossWrites(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	// Page 1 over traces 0..9.
	var first []model.Span
	for i := 0; i < 10; i++ {
		first = append(first, spanAt(string(rune('A'+i)), "s", "svc", "op", int64(i), time.Millisecond, nil, model.StatusOK))
	}
	if _, err := svc.Ingest(ctx, first); err != nil {
		t.Fatal(err)
	}

	page1, err := svc.Search(ctx, query.Filter{}, "", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Traces) != 4 || !page1.HasMore || page1.NextCursor == "" {
		t.Fatalf("page1 = %+v", page1)
	}

	// Concurrent writes during paging: 10 new traces, some starting earlier.
	var second []model.Span
	for i := 0; i < 10; i++ {
		second = append(second, spanAt(string(rune('a'+i)), "s", "svc", "op", int64(100+i), time.Millisecond, nil, model.StatusOK))
	}
	if _, err := svc.Ingest(ctx, second); err != nil {
		t.Fatal(err)
	}

	var seen []string
	for _, tr := range page1.Traces {
		seen = append(seen, tr.TraceID)
	}
	cursor := page1.NextCursor
	pages := 1
	for {
		page, err := svc.Search(ctx, query.Filter{}, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range page.Traces {
			if contains(seen, tr.TraceID) {
				t.Fatalf("trace %s returned twice during stable paging", tr.TraceID)
			}
			seen = append(seen, tr.TraceID)
		}
		pages++
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("pagination never terminates")
		}
	}
	if len(seen) != 10 {
		t.Fatalf("snapshot paging saw %d of the original 10 traces: %v", len(seen), seen)
	}

	// A fresh query sees all 20.
	fresh, err := svc.Search(ctx, query.Filter{}, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.Traces) != 20 {
		t.Fatalf("fresh search saw %d traces, want 20", len(fresh.Traces))
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
