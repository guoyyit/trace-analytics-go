package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/trace-analytics-go/analytics"
	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/query"
	"github.com/guoyyit/trace-analytics-go/service"
)

func cfgFor(t *testing.T, mutate func(*config.Config)) config.Config {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SyncWAL = false
	cfg.MaxPinnedViews = 32
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func mkSpan(traceID, spanID, parent, svc, op string, startMs int64, dur time.Duration, status model.Status) model.Span {
	return model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parent,
		ServiceName:   svc,
		OperationName: op,
		StartTime:     time.Unix(0, startMs*int64(time.Millisecond)).UTC(),
		Duration:      dur,
		Status:        status,
	}
}

func TestIngestDuplicatesAndMalformedIsolation(t *testing.T) {
	svc, err := service.New(context.Background(), cfgFor(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()

	root := mkSpan("t1", "root", "", "gw", "req", 0, 100*time.Millisecond, model.StatusOK)
	child := mkSpan("t1", "c", "root", "db", "q", 10, 10*time.Millisecond, model.StatusOK)
	res, err := svc.Ingest(ctx, []model.Span{child, root, root, model.Span{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 {
		t.Fatalf("accepted = %d, want 2", res.Accepted)
	}
	if len(res.Rejected) != 1 || res.Rejected[0].Reason == "" {
		t.Fatalf("rejected = %+v", res.Rejected)
	}

	// Same span again: idempotent duplicate, no growth.
	res2, err := svc.Ingest(ctx, []model.Span{root})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Accepted != 0 {
		t.Fatalf("duplicate accepted again: %+v", res2)
	}

	// Conflicting duplicate is isolated.
	conflict := root
	conflict.Duration = 999 * time.Millisecond
	res3, err := svc.Ingest(ctx, []model.Span{conflict})
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Rejected) != 1 || res3.Rejected[0].Reason != "conflicting duplicate span" {
		t.Fatalf("conflict not isolated: %+v", res3.Rejected)
	}

	tr, err := svc.GetTrace(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Complete || len(tr.Spans) != 2 {
		t.Fatalf("trace = %+v", tr)
	}
}

func TestLateParentCompletesTrace(t *testing.T) {
	svc, _ := service.New(context.Background(), cfgFor(t, nil))
	defer svc.Close()
	ctx := context.Background()

	svc.Ingest(ctx, []model.Span{mkSpan("t", "child", "root", "svc", "op", 5, 5*time.Millisecond, model.StatusOK)})
	tr, _ := svc.GetTrace(ctx, "t")
	if tr.Complete {
		t.Fatal("orphan trace must be incomplete")
	}

	svc.Ingest(ctx, []model.Span{mkSpan("t", "root", "", "svc", "root", 0, 50*time.Millisecond, model.StatusOK)})
	tr, _ = svc.GetTrace(ctx, "t")
	if !tr.Complete || len(tr.Spans) != 2 {
		t.Fatalf("late parent should complete trace: %+v", tr)
	}

	// Drill-down all the way to a single span.
	sp, err := svc.GetSpan(ctx, "t", "child")
	if err != nil || sp.SpanID != "child" {
		t.Fatalf("span drill-down: %v %+v", err, sp)
	}
}

func TestConcurrentReadersAndWritersSeeCommittedSnapshots(t *testing.T) {
	svc, _ := service.New(context.Background(), cfgFor(t, nil))
	defer svc.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				traceID := fmt.Sprintf("w%d-t%d", w, i)
				spans := []model.Span{
					mkSpan(traceID, "root", "", "svc", "op", int64(i), time.Millisecond, model.StatusOK),
				}
				if _, err := svc.Ingest(ctx, spans); err != nil {
					t.Errorf("ingest: %v", err)
					return
				}
			}
		}(w)
	}

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				res, err := svc.Search(ctx, query.Filter{}, "", 100)
				if err != nil {
					t.Errorf("search: %v", err)
					return
				}
				// Every returned trace must be a fully assembled, committed one.
				for _, tr := range res.Traces {
					if len(tr.Spans) == 0 {
						t.Errorf("half-visible trace %s", tr.TraceID)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	res, err := svc.Search(ctx, query.Filter{}, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != 200 {
		t.Fatalf("final count = %d, want 200", len(res.Traces))
	}
}

func TestStorageLimitIsExplict(t *testing.T) {
	cfg := cfgFor(t, func(c *config.Config) {
		c.MaxSpans = 2
		c.MaxSpansPerTrace = 10
	})
	svc, _ := service.New(context.Background(), cfg)
	defer svc.Close()
	ctx := context.Background()

	r1, _ := svc.Ingest(ctx, []model.Span{
		mkSpan("a", "1", "", "s", "o", 0, time.Millisecond, model.StatusOK),
		mkSpan("b", "1", "", "s", "o", 1, time.Millisecond, model.StatusOK),
	})
	if r1.Accepted != 2 {
		t.Fatalf("accepted = %d", r1.Accepted)
	}
	r2, err := svc.Ingest(ctx, []model.Span{
		mkSpan("c", "1", "", "s", "o", 2, time.Millisecond, model.StatusOK),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Rejected) != 1 || r2.Rejected[0].Reason != "global span limit exceeded" {
		t.Fatalf("limit rejection = %+v", r2.Rejected)
	}
}

func TestStatsDependenciesAndCriticalPathEndToEnd(t *testing.T) {
	svc, _ := service.New(context.Background(), cfgFor(t, nil))
	defer svc.Close()
	ctx := context.Background()

	var spans []model.Span
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("t%d", i)
		status := model.StatusOK
		if i%10 == 0 {
			status = model.StatusError
		}
		spans = append(spans,
			mkSpan(id, "root", "", "gateway", "request", int64(i), 100*time.Millisecond, status),
			mkSpan(id, "child", "root", "billing", "charge", int64(i)+1, time.Duration(10+i)*time.Millisecond, status),
		)
	}
	if _, err := svc.Ingest(ctx, spans); err != nil {
		t.Fatal(err)
	}

	buckets, err := svc.Stats(ctx, true, analytics.StatsOptions{
		Bucket:    time.Hour,
		Quantiles: []float64{0.5, 0.99},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 {
		t.Fatalf("buckets = %d", len(buckets))
	}
	var total int64
	for _, sb := range buckets {
		for _, g := range sb.Groups {
			total += g.Count
			if g.ServiceName == "billing" && g.Quantiles[0.5] <= 0 {
				t.Fatal("quantiles not computed")
			}
		}
	}
	if total != 200 {
		t.Fatalf("stats count = %d", total)
	}

	deps, err := svc.Dependencies(ctx, query.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(deps.Edges) != 1 {
		t.Fatalf("edges = %+v", deps.Edges)
	}
	e := deps.Edges[0]
	if e.ParentService != "gateway" || e.ChildService != "billing" || e.CallCount != 100 || e.ErrorCount != 10 {
		t.Fatalf("edge = %+v", e)
	}
	if got := e.ErrorRate(); got != 0.1 {
		t.Fatalf("edge error rate = %v", got)
	}

	cp, err := svc.TraceCriticalPath(ctx, "t0")
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Chain) != 2 {
		t.Fatalf("critical path = %+v", cp.Chain)
	}

	if _, err := svc.GetTrace(ctx, "missing"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
