package storage

import (
	"reflect"
	"sort"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/ingest"
	"github.com/guoyyit/trace-analytics-go/model"
)

type commitStats struct {
	accepted   int
	duplicates int
}

// builder incrementally constructs a new View from a previous one. Trace
// objects untouched by a batch are shared structurally between views.
type builder struct {
	cfg          config.Config
	traces       map[string]*model.Trace
	spansByKey   map[string]model.Span
	serviceNames map[string]struct{}
	tagKeys      map[string]struct{}
	tagSaturated bool
	spanCount    int
	seq          uint64
	rejected     []model.RejectedSpan
	rejectedTot  int64
	stats        commitStats
	acceptedSpan []model.Span
}

func newBuilderFrom(base *View, cfg config.Config, seq uint64) *builder {
	b := &builder{
		cfg:          cfg,
		traces:       map[string]*model.Trace{},
		spansByKey:   map[string]model.Span{},
		serviceNames: map[string]struct{}{},
		tagKeys:      map[string]struct{}{},
		seq:          seq,
	}
	if base != nil {
		for id, tr := range base.traces {
			b.traces[id] = tr
		}
		for k, sp := range base.spansByKey {
			b.spansByKey[k] = sp
		}
		b.serviceNames = cloneSet(base.serviceNames)
		b.tagKeys = cloneSet(base.tagKeys)
		b.tagSaturated = base.tagSaturated
		b.spanCount = base.spanCount
		b.rejected = append(b.rejected, base.rejected...)
		b.rejectedTot = base.rejectedTotal
	}
	return b
}

func cloneSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func (b *builder) apply(spans []model.Span) {
	groups := make(map[string][]model.Span)
	order := make([]string, 0)
	for i, sp := range spans {
		if reason := ingest.Validate(sp); reason != "" {
			b.reject(model.RejectedSpan{Index: i, TraceID: sp.TraceID, SpanID: sp.SpanID, Reason: reason})
			continue
		}
		key := spanKey(sp.TraceID, sp.SpanID)
		if existing, ok := b.spansByKey[key]; ok {
			if reflect.DeepEqual(existing, sp) {
				b.stats.duplicates++
				continue
			}
			b.reject(model.RejectedSpan{Index: i, TraceID: sp.TraceID, SpanID: sp.SpanID, Reason: "conflicting duplicate span"})
			continue
		}
		// Duplicate or conflict within the same input batch.
		var dup bool
		for _, prior := range groups[sp.TraceID] {
			if prior.SpanID == sp.SpanID {
				if reflect.DeepEqual(prior, sp) {
					b.stats.duplicates++
				} else {
					b.reject(model.RejectedSpan{Index: i, TraceID: sp.TraceID, SpanID: sp.SpanID, Reason: "conflicting duplicate span"})
				}
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		if _, seen := groups[sp.TraceID]; !seen {
			order = append(order, sp.TraceID)
		}
		groups[sp.TraceID] = append(groups[sp.TraceID], sp)
	}

	for _, traceID := range order {
		accepted := groups[traceID]
		existing := b.traces[traceID]
		merged := accepted
		if existing != nil {
			merged = append(merged, existing.Spans...)
		}
		// Dedup inside the batch itself.
		uniq := make(map[string]model.Span, len(merged))
		for _, sp := range merged {
			uniq[sp.SpanID] = sp
		}
		final := make([]model.Span, 0, len(uniq))
		for _, sp := range uniq {
			final = append(final, sp)
		}

		oldCount := 0
		if existing != nil {
			oldCount = len(existing.Spans)
		}
		newCount := len(final)
		added := newCount - oldCount
		if added < 0 {
			added = 0
		}
		if b.cfg.MaxSpansPerTrace > 0 && newCount > b.cfg.MaxSpansPerTrace {
			for _, sp := range accepted {
				b.reject(model.RejectedSpan{TraceID: traceID, SpanID: sp.SpanID, Reason: "trace span limit exceeded"})
			}
			continue
		}
		if b.cfg.MaxSpans > 0 && b.spanCount+added > b.cfg.MaxSpans {
			for _, sp := range accepted {
				b.reject(model.RejectedSpan{TraceID: traceID, SpanID: sp.SpanID, Reason: "global span limit exceeded"})
			}
			continue
		}

		tr := ingest.BuildTrace(traceID, final)
		b.traces[traceID] = tr
		for _, sp := range accepted {
			b.spansByKey[spanKey(sp.TraceID, sp.SpanID)] = sp
			b.serviceNames[sp.ServiceName] = struct{}{}
			for tagKey := range sp.Tags {
				if len(b.tagKeys) >= b.cfg.MaxTagIndexKeys {
					if _, ok := b.tagKeys[tagKey]; !ok {
						b.tagSaturated = true
					}
				} else {
					b.tagKeys[tagKey] = struct{}{}
				}
			}
		}
		b.spanCount += added
		b.stats.accepted += len(accepted)
		b.acceptedSpan = append(b.acceptedSpan, accepted...)
	}
}

func (b *builder) reject(r model.RejectedSpan) {
	b.rejectedTot++
	if b.cfg.MaxRejectedRetained > 0 && len(b.rejected) < b.cfg.MaxRejectedRetained {
		b.rejected = append(b.rejected, r)
	}
}

func (b *builder) build() *View {
	ids := make([]string, 0, len(b.traces))
	for id := range b.traces {
		ids = append(ids, id)
	}
	sortTraceIDs(ids, b.traces)
	return &View{
		seq:           b.seq,
		traces:        b.traces,
		traceIDs:      ids,
		spanCount:     b.spanCount,
		spansByKey:    b.spansByKey,
		rejected:      b.rejected,
		rejectedTotal: b.rejectedTot,
		serviceNames:  b.serviceNames,
		tagKeys:       b.tagKeys,
		tagSaturated:  b.tagSaturated,
	}
}

// sortTraceIDs orders traces for stable pagination: trace start time, then id.
func sortTraceIDs(ids []string, traces map[string]*model.Trace) {
	sort.Slice(ids, func(i, j int) bool {
		a, b := traces[ids[i]], traces[ids[j]]
		if !a.StartTime.Equal(b.StartTime) {
			return a.StartTime.Before(b.StartTime)
		}
		return a.TraceID < b.TraceID
	})
}
