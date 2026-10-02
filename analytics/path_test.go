package analytics

import (
	"testing"
	"time"

	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/model"
)

func sp(id, parent, svc, op string, startMs int64, dur time.Duration, status model.Status) model.Span {
	return model.Span{
		TraceID:       "t",
		SpanID:        id,
		ParentSpanID:  parent,
		ServiceName:   svc,
		OperationName: op,
		StartTime:     time.Unix(0, startMs*int64(time.Millisecond)).UTC(),
		Duration:      dur,
		Status:        status,
	}
}

func TestCriticalPathPicksLongestChain(t *testing.T) {
	// root [0,100] -> slow [10,80] -> slowdb [20,50]
	//             -> fast [10,5]
	root := sp("root", "", "gw", "req", 0, 100*time.Millisecond, model.StatusOK)
	slow := sp("slow", "root", "svc-a", "call", 10, 80*time.Millisecond, model.StatusOK)
	slowdb := sp("slowdb", "slow", "db", "q", 20, 50*time.Millisecond, model.StatusError)
	fast := sp("fast", "root", "svc-b", "ping", 10, 5*time.Millisecond, model.StatusOK)

	tr := ingest.BuildTrace("t", []model.Span{fast, slowdb, slow, root})
	cp := CriticalPath(tr)
	if len(cp.Chain) != 3 {
		t.Fatalf("chain len = %d: %+v", len(cp.Chain), cp.Chain)
	}
	want := []string{"root", "slow", "slowdb"}
	for i, hop := range cp.Chain {
		if hop.SpanID != want[i] {
			t.Fatalf("hop %d = %s, want %s", i, hop.SpanID, want[i])
		}
	}
	if cp.Chain[2].Status != model.StatusError {
		t.Fatal("critical path should carry span status")
	}
}

func TestCriticalPathEmpty(t *testing.T) {
	cp := CriticalPath(&model.Trace{TraceID: "x"})
	if len(cp.Chain) != 0 {
		t.Fatal("empty trace should yield empty chain")
	}
}
