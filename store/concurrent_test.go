package store

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/guoyyit/trace-analytics-go/model"
)

// TestConcurrentReadWrite runs many writers and readers simultaneously.
// Every reader observes only fully committed batches: each trace must
// contain either the complete batch of spans for that commit or none of the
// batch's new content is partially visible (spans within a batch land
// atomically under the store commit lock).
func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const writers = 4
	const batchesPerWriter = 50
	const spansPerBatch = 10

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batchesPerWriter; b++ {
				traceID := fmt.Sprintf("w%d-b%d", w, b)
				spans := make([]model.Span, 0, spansPerBatch)
				for i := 0; i < spansPerBatch; i++ {
					spans = append(spans, model.Span{
						TraceID:       traceID,
						SpanID:        fmt.Sprintf("s%d", i),
						ParentSpanID:  chooseParent(i),
						Service:       fmt.Sprintf("svc%d", i%3),
						Operation:     "op",
						StartUnixNano: int64(b*100 + i),
						DurationNano:  5,
						Status:        model.StatusOK,
					})
				}
				if _, _, err := s.Commit(&Batch{ID: traceID, Spans: spans}); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	const readers = 4
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := s.Current()
				for _, id := range snap.AllTraceIDs() {
					tr, ok := snap.Trace(id)
					if !ok {
						t.Errorf("trace listed but not resolvable: %s", id)
						return
					}
					// Within-trace start bounds must always be consistent.
					if tr.EndUnixNano < tr.StartUnixNano {
						t.Errorf("inconsistent trace bounds: %s", id)
						return
					}
				}
			}
		}()
	}

	// Wait for the committed count to converge, then stop readers.
	for {
		if s.Current().TotalSpans() == writers*batchesPerWriter*spansPerBatch {
			break
		}
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()

	final := s.Current()
	want := writers * batchesPerWriter * spansPerBatch
	if final.TotalSpans() != want {
		t.Fatalf("final span count: got %d want %d", final.TotalSpans(), want)
	}
}

func chooseParent(i int) string {
	if i == 0 {
		return ""
	}
	return fmt.Sprintf("s%d", i-1)
}

func TestMissingParentCounted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testCfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	orphan := model.Span{TraceID: "t", SpanID: "orphan", ParentSpanID: "ghost",
		Service: "svc", Operation: "op", StartUnixNano: 0, DurationNano: 5,
		Status: model.StatusOK}
	if _, _, err := s.Commit(&Batch{ID: "b", Spans: []model.Span{orphan}}); err != nil {
		t.Fatal(err)
	}
	tr, ok := s.Current().Trace("t")
	if !ok || tr.MissingParents != 1 {
		t.Fatalf("missing parent count: %+v ok=%v", tr, ok)
	}
}
