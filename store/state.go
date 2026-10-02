package store

import (
	"reflect"
	"sort"

	"github.com/guoyyit/trace-analytics-go/model"
)

// traceData is an immutable-assembled trace body held by the store.
type traceData struct {
	spans         map[string]model.Span
	startUnixNano int64
	endUnixNano   int64
	hasError      bool
	services      map[string]struct{}
	createdSeq    uint64
}

func (t *traceData) clone() *traceData {
	spans := make(map[string]model.Span, len(t.spans)+1)
	for id, sp := range t.spans {
		spans[id] = sp
	}
	services := make(map[string]struct{}, len(t.services)+1)
	for svc := range t.services {
		services[svc] = struct{}{}
	}
	return &traceData{
		spans:         spans,
		startUnixNano: t.startUnixNano,
		endUnixNano:   t.endUnixNano,
		hasError:      t.hasError,
		services:      services,
		createdSeq:    t.createdSeq,
	}
}

func spansEqual(a, b model.Span) bool {
	return reflect.DeepEqual(a, b)
}

// assemble projects stored traceData into the API model.Trace. Spans are
// sorted by (start, spanId) for deterministic output.
func (t *traceData) assemble(traceID string) *model.Trace {
	spans := make([]model.Span, 0, len(t.spans))
	ids := map[string]struct{}{}
	for id, sp := range t.spans {
		spans = append(spans, sp)
		ids[id] = struct{}{}
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].StartUnixNano != spans[j].StartUnixNano {
			return spans[i].StartUnixNano < spans[j].StartUnixNano
		}
		return spans[i].SpanID < spans[j].SpanID
	})
	missing := 0
	for _, sp := range spans {
		if sp.ParentSpanID != "" {
			if _, ok := ids[sp.ParentSpanID]; !ok {
				missing++
			}
		}
	}
	services := make([]string, 0, len(t.services))
	for svc := range t.services {
		services = append(services, svc)
	}
	sort.Strings(services)
	return &model.Trace{
		TraceID:        traceID,
		Spans:          spans,
		StartUnixNano:  t.startUnixNano,
		EndUnixNano:    t.endUnixNano,
		HasError:       t.hasError,
		MissingParents: missing,
		Services:       services,
	}
}

func appendRing(ring []model.RejectedSpan, add []model.RejectedSpan, max int) []model.RejectedSpan {
	if max <= 0 {
		return nil
	}
	ring = append(ring, add...)
	if len(ring) > max {
		ring = ring[len(ring)-max:]
	}
	return ring
}
