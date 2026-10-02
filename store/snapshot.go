package store

import (
	"sort"

	"github.com/guoyyit/trace-analytics-go/model"
)

// Snapshot is an immutable view of the committed state at one sequence
// number. Readers never block writers: new commits create a fresh overlay
// snapshot and materialize a full base every SnapshotHistory commits.
type Snapshot struct {
	seq        uint64
	totalSpans int

	// base is a materialized full state; overlay records changes since base.
	base       map[string]*traceData
	overlay    map[string]*traceData
	svcBase    map[string]map[string]struct{}
	svcOverlay map[string]map[string]struct{}
	baseID     uint64

	prev *Snapshot

	rejected []model.RejectedSpan
}

func emptySnapshot() *Snapshot {
	return &Snapshot{
		base:       map[string]*traceData{},
		overlay:    map[string]*traceData{},
		svcBase:    map[string]map[string]struct{}{},
		svcOverlay: map[string]map[string]struct{}{},
	}
}

// derive creates a new empty overlay snapshot on top of s.
func (s *Snapshot) derive(seq uint64) *Snapshot {
	return &Snapshot{
		seq:        seq,
		totalSpans: s.totalSpans,
		base:       s.base,
		overlay:    map[string]*traceData{},
		svcBase:    s.svcBase,
		svcOverlay: map[string]map[string]struct{}{},
		baseID:     s.baseID,
		prev:       s,
	}
}

// materialize folds the overlay chain into fresh immutable base maps. It runs
// before the snapshot is published, so concurrent readers never see mutation.
func (s *Snapshot) materialize() {
	traces := make(map[string]*traceData, len(s.base)+chainLen(s))
	for id, td := range s.base {
		traces[id] = td
	}
	services := make(map[string]map[string]struct{}, len(s.svcBase))
	for svc, set := range s.svcBase {
		clone := make(map[string]struct{}, len(set))
		for id := range set {
			clone[id] = struct{}{}
		}
		services[svc] = clone
	}

	// Collect overlays newest->oldest down to (but excluding) the root node,
	// which is the only node whose base is the authoritative oldest state.
	var chain []*Snapshot
	for cur := s; cur != nil && cur.prev != nil; cur = cur.prev {
		chain = append(chain, cur)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		node := chain[i]
		for id, td := range node.overlay {
			traces[id] = td
		}
		for svc, set := range node.svcOverlay {
			dst := services[svc]
			if dst == nil {
				dst = map[string]struct{}{}
				services[svc] = dst
			}
			for id := range set {
				dst[id] = struct{}{}
			}
		}
	}

	s.base = traces
	s.overlay = map[string]*traceData{}
	s.svcBase = services
	s.svcOverlay = map[string]map[string]struct{}{}
	s.baseID = s.seq
	s.prev = nil
}

func chainLen(s *Snapshot) int {
	n := 0
	for cur := s; cur != nil && cur.prev != nil; cur = cur.prev {
		n++
	}
	return n
}

// resolveTrace walks overlays (newest first), then the materialized base.
func (s *Snapshot) resolveTrace(id string) *traceData {
	for cur := s; cur != nil; cur = cur.prev {
		if td, ok := cur.overlay[id]; ok {
			return td
		}
		if cur.prev == nil {
			// Root node: its base is the oldest state in this chain.
			return cur.base[id]
		}
	}
	return nil
}

func (s *Snapshot) resolveServiceSet(service string) map[string]struct{} {
	var merged map[string]struct{}
	for cur := s; cur != nil; cur = cur.prev {
		if set, ok := cur.svcOverlay[service]; ok {
			if merged == nil {
				merged = make(map[string]struct{}, len(set))
			} else {
				for id := range set {
					merged[id] = struct{}{}
				}
			}
			for id := range set {
				merged[id] = struct{}{}
			}
		}
		if cur.prev == nil {
			if baseSet, ok := cur.svcBase[service]; ok {
				if merged == nil {
					return baseSet
				}
				for id := range baseSet {
					merged[id] = struct{}{}
				}
			}
			return merged
		}
	}
	return merged
}

func (s *Snapshot) allTraceIDsLocked() []string {
	seen := map[string]struct{}{}
	for cur := s; cur != nil; cur = cur.prev {
		for id := range cur.overlay {
			seen[id] = struct{}{}
		}
		if cur.prev == nil {
			for id := range cur.base {
				seen[id] = struct{}{}
			}
			break
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Seq is the commit sequence number represented by this snapshot.
func (s *Snapshot) Seq() uint64 { return s.seq }

// TotalSpans is the number of persisted spans.
func (s *Snapshot) TotalSpans() int { return s.totalSpans }

// TraceIDsByService returns trace ids that contain the service (sorted).
func (s *Snapshot) TraceIDsByService(service string) []string {
	set := s.resolveServiceSet(service)
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// AllTraceIDs returns every trace id (sorted).
func (s *Snapshot) AllTraceIDs() []string { return s.allTraceIDsLocked() }

// Trace returns the assembled trace for id.
func (s *Snapshot) Trace(id string) (*model.Trace, bool) {
	td := s.resolveTrace(id)
	if td == nil {
		return nil, false
	}
	return td.assemble(id), true
}

// Span returns one span by (traceId, spanId).
func (s *Snapshot) Span(traceID, spanID string) (model.Span, bool) {
	td := s.resolveTrace(traceID)
	if td == nil {
		return model.Span{}, false
	}
	sp, ok := td.spans[spanID]
	return sp, ok
}

// Rejected returns the most recently quarantined spans (newest last).
func (s *Snapshot) Rejected(limit int) []model.RejectedSpan {
	if limit <= 0 || limit > len(s.rejected) {
		limit = len(s.rejected)
	}
	out := make([]model.RejectedSpan, limit)
	copy(out, s.rejected[len(s.rejected)-limit:])
	return out
}
