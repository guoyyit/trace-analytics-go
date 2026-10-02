package ingest

import (
	"testing"
	"time"

	"github.com/guoyyit/trace-analytics-go/model"
)

func ts(offsetMillis int64) time.Time {
	return time.Unix(0, offsetMillis*int64(time.Millisecond)).UTC()
}

func validSpan(traceID, spanID, parent, service, op string, start int64, dur time.Duration) model.Span {
	return model.Span{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parent,
		ServiceName:   service,
		OperationName: op,
		StartTime:     ts(start),
		Duration:      dur,
		Status:        model.StatusOK,
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		span model.Span
		want string
	}{
		{"ok", validSpan("t", "a", "", "svc", "op", 0, time.Millisecond), ""},
		{"missing trace", model.Span{SpanID: "a", ServiceName: "s", OperationName: "o", StartTime: ts(0)}, "missing traceId"},
		{"missing span", model.Span{TraceID: "t", ServiceName: "s", OperationName: "o", StartTime: ts(0)}, "missing spanId"},
		{"missing service", model.Span{TraceID: "t", SpanID: "a", OperationName: "o", StartTime: ts(0)}, "missing serviceName"},
		{"missing operation", model.Span{TraceID: "t", SpanID: "a", ServiceName: "s", StartTime: ts(0)}, "missing operationName"},
		{"missing start", model.Span{TraceID: "t", SpanID: "a", ServiceName: "s", OperationName: "o"}, "missing startTime"},
		{"negative duration", func() model.Span {
			s := validSpan("t", "a", "", "s", "o", 0, 0)
			s.Duration = -1
			return s
		}(), "negative duration"},
		{"self parent", func() model.Span {
			s := validSpan("t", "a", "a", "s", "o", 0, 0)
			return s
		}(), "span is its own parent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Validate(tc.span); got != tc.want {
				t.Fatalf("Validate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildTraceOutOfOrder(t *testing.T) {
	root := validSpan("t", "root", "", "gateway", "request", 0, 100*time.Millisecond)
	child := validSpan("t", "c1", "root", "auth", "check", 10, 20*time.Millisecond)
	grand := validSpan("t", "c2", "c1", "db", "query", 15, 5*time.Millisecond)

	tr := BuildTrace("t", []model.Span{grand, child, root})
	if !tr.Complete {
		t.Fatal("expected complete trace")
	}
	if len(tr.Roots) != 1 || tr.Roots[0].Span.SpanID != "root" {
		t.Fatalf("roots = %v", tr.Roots)
	}
	if len(tr.Roots[0].Children) != 1 || tr.Roots[0].Children[0].Span.SpanID != "c1" {
		t.Fatalf("tree link broken: %+v", tr.Roots[0].Children)
	}
	if len(tr.Spans) != 3 {
		t.Fatalf("span count = %d", len(tr.Spans))
	}
	if !tr.StartTime.Equal(ts(0)) || !tr.EndTime.Equal(ts(100)) {
		t.Fatalf("window = %v..%v", tr.StartTime, tr.EndTime)
	}
}

func TestBuildTraceMissingParentBecomesCompleteLate(t *testing.T) {
	orphan := validSpan("t", "child", "root", "svc", "op", 5, 10*time.Millisecond)

	tr := BuildTrace("t", []model.Span{orphan})
	if tr.Complete {
		t.Fatal("expected incomplete trace with missing parent")
	}
	if len(tr.MissingParentID) != 1 || tr.MissingParentID[0] != "root" {
		t.Fatalf("missing = %v", tr.MissingParentID)
	}

	root := validSpan("t", "root", "", "svc", "root-op", 0, 50*time.Millisecond)
	tr = BuildTrace("t", []model.Span{orphan, root})
	if !tr.Complete {
		t.Fatal("late-arriving parent should complete the trace")
	}
}

func TestBuildTraceCycle(t *testing.T) {
	a := validSpan("t", "a", "b", "svc", "a", 0, time.Millisecond)
	b := validSpan("t", "b", "a", "svc", "b", 0, time.Millisecond)
	tr := BuildTrace("t", []model.Span{a, b})
	if tr.Complete {
		t.Fatal("cyclic parent links must be flagged incomplete")
	}
	if len(tr.Spans) != 2 {
		t.Fatalf("spans = %d", len(tr.Spans))
	}
	if len(tr.Roots) == 0 {
		t.Fatal("cycle nodes must still be reachable as roots")
	}
}

func TestBuildTraceDeduplicatesIDs(t *testing.T) {
	sp := validSpan("t", "a", "", "svc", "op", 0, time.Millisecond)
	tr := BuildTrace("t", []model.Span{sp, sp})
	if len(tr.Spans) != 1 {
		t.Fatalf("expected dedup, got %d", len(tr.Spans))
	}
}
