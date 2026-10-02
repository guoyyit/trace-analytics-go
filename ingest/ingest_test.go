package ingest

import (
	"testing"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
	"github.com/guoyyit/trace-analytics-go/store"
)

func validSpan() model.Span {
	return model.Span{
		TraceID: "t", SpanID: "s", Service: "svc", Operation: "op",
		StartUnixNano: 1, DurationNano: 2, Status: model.StatusOK,
	}
}

func TestValidateMalformedCases(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	s, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := New(s, cfg)

	cases := []func(model.Span) model.Span{
		func(sp model.Span) model.Span { sp.TraceID = ""; return sp },
		func(sp model.Span) model.Span { sp.SpanID = "  "; return sp },
		func(sp model.Span) model.Span { sp.Service = ""; return sp },
		func(sp model.Span) model.Span { sp.Operation = ""; return sp },
		func(sp model.Span) model.Span { sp.DurationNano = -1; return sp },
		func(sp model.Span) model.Span { sp.ParentSpanID = sp.SpanID; return sp },
		func(sp model.Span) model.Span { sp.Status = "BOGUS"; return sp },
	}
	for i, mutate := range cases {
		if r := g.Validate(mutate(validSpan())); r == nil || r.Reason != model.RejectMalformed {
			t.Fatalf("case %d: expected MALFORMED, got %+v", i, r)
		}
	}
	if r := g.Validate(validSpan()); r != nil {
		t.Fatalf("valid span rejected: %+v", r)
	}
}

func TestIngestIsolatesMalformedButCommitsValid(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	s, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := New(s, cfg)

	bad := validSpan()
	bad.SpanID = ""
	good := validSpan()
	good.SpanID = "good"

	report, err := g.Ingest("batch-1", []model.Span{bad, good})
	if err != nil {
		t.Fatal(err)
	}
	if report.Accepted != 1 || len(report.Rejected) != 1 {
		t.Fatalf("report=%+v", report)
	}
	if report.Rejected[0].Reason != model.RejectMalformed {
		t.Fatalf("expected MALFORMED isolated, got %v", report.Rejected[0].Reason)
	}

	// Reingest the good span: exact duplicate isolated distinctly.
	report2, err := g.Ingest("batch-2", []model.Span{good})
	if err != nil {
		t.Fatal(err)
	}
	if report2.Accepted != 0 || report2.Duplicate != 1 {
		t.Fatalf("expected duplicate, report=%+v", report2)
	}
}

func TestAttributeTruncation(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.MaxAttributesPerSpan = 1
	cfg.MaxAttributeValueLen = 3
	s, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := New(s, cfg)

	sp := validSpan()
	sp.Attributes = map[string]string{"k": "abcdef", "drop": "v"}
	if _, err := g.Ingest("b", []model.Span{sp}); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Current().Span("t", "s")
	if !ok {
		t.Fatal("span missing")
	}
	// Keys are sorted before truncation, so "drop" is the kept attribute.
	if len(got.Attributes) != 1 || got.Attributes["drop"] != "v" {
		t.Fatalf("attrs not bounded: %+v", got.Attributes)
	}
}
