package storage

import (
	"context"
	"sync"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
)

// Store owns the current immutable View, an append-only batch history and a
// bounded ring of old views for cursor stability.
type Store struct {
	cfg     config.Config
	mu      sync.RWMutex
	wal     *WAL
	current *View
	pinned  []*View
	since   uint64
}

// OpenStore loads state from disk (snapshot + WAL replay). Corrupt snapshot
// or torn WAL tail must not prevent startup.
func OpenStore(ctx context.Context, cfg config.Config) (*Store, error) {
	wal, err := OpenWAL(cfg.DataDir, cfg.SyncWAL)
	if err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, wal: wal}

	snap, hasSnap, err := readSnapshot(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	var b *builder
	var baseSeq uint64
	if hasSnap {
		baseSeq = snap.LastSeq
		b = newBuilderFrom(nil, cfg, baseSeq)
		b.rejected = append(b.rejected, snap.Rejected...)
		b.rejectedTot = snap.RejectedTotal
		for _, pt := range snap.Traces {
			b.seq = baseSeq
			b.apply(pt.Spans)
		}
		b.seq = baseSeq
	}

	if err := wal.Replay(ctx, func(bt model.Batch) error {
		if bt.Seq <= baseSeq {
			return nil
		}
		if b == nil {
			b = newBuilderFrom(nil, cfg, bt.Seq)
		} else {
			b.seq = bt.Seq
		}
		b.apply(bt.Spans)
		b.seq = bt.Seq
		return nil
	}, func(r model.RejectedSpan) {
		if b == nil {
			b = newBuilderFrom(nil, cfg, 0)
		}
		b.reject(r)
	}); err != nil {
		return nil, err
	}

	if b != nil {
		s.current = b.build()
	} else {
		s.current = newBuilderFrom(nil, cfg, 0).build()
	}
	return s, nil
}

func (s *Store) Current() *View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// ViewFor returns the pinned view carrying cursorSeq; nil if expired.
func (s *Store) ViewFor(cursorSeq uint64) *View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current != nil && s.current.seq == cursorSeq {
		return s.current
	}
	for _, v := range s.pinned {
		if v.seq == cursorSeq {
			return v
		}
	}
	return nil
}

// Commit validates, applies and durably persists one batch. The returned
// rejections are persisted to the quarantine log.
func (s *Store) Commit(ctx context.Context, spans []model.Span) (model.Batch, []model.RejectedSpan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.current
	seq := s.wal.LastSeq() + 1
	b := newBuilderFrom(base, s.cfg, seq)
	b.apply(spans)

	rejected, rejTotal := rejectDiff(base, b)

	// Durable commit first; the visible swap only happens afterwards.
	batch := model.Batch{Seq: seq, Spans: b.acceptedSpan}
	if err := s.wal.Append(ctx, batch); err != nil {
		return model.Batch{}, nil, err
	}
	for _, r := range rejected {
		if err := s.wal.AppendRejected(ctx, r); err != nil {
			return model.Batch{}, nil, err
		}
	}

	next := b.build()
	_ = rejTotal
	if base != nil && s.cfg.MaxPinnedViews > 0 {
		s.pinned = append(s.pinned, base)
		if len(s.pinned) > s.cfg.MaxPinnedViews {
			s.pinned = s.pinned[len(s.pinned)-s.cfg.MaxPinnedViews:]
		}
	}
	s.current = next
	s.since++

	if s.cfg.SnapshotAfterBatches > 0 && s.since >= s.cfg.SnapshotAfterBatches {
		if err := s.snapshotLocked(ctx); err != nil {
			// Snapshot failure is not fatal: WAL remains the source of truth.
			s.since = 0
		}
	}
	return batch, rejected, nil
}

func rejectDiff(base *View, b *builder) ([]model.RejectedSpan, int64) {
	baseTotal := int64(0)
	if base != nil {
		baseTotal = base.rejectedTotal
	}
	if b.rejectedTot <= baseTotal {
		return nil, b.rejectedTot
	}
	n := b.rejectedTot - baseTotal
	if int(n) > len(b.rejected) {
		n = int64(len(b.rejected))
	}
	return append([]model.RejectedSpan(nil), b.rejected[len(b.rejected)-int(n):]...), b.rejectedTot
}

func (s *Store) snapshotLocked(ctx context.Context) error {
	data := snapshotData{
		LastSeq:       s.current.seq,
		Rejected:      s.current.rejected,
		RejectedTotal: s.current.rejectedTotal,
	}
	for _, id := range s.current.traceIDs {
		tr := s.current.traces[id]
		data.Traces = append(data.Traces, persistedTrace{TraceID: id, Spans: tr.Spans})
	}
	if err := atomicWriteSnapshot(s.cfg.DataDir, data); err != nil {
		return err
	}
	if err := s.wal.Rewrite(ctx, nil, data.LastSeq); err != nil {
		return err
	}
	s.since = 0
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}
