// Package store owns persistence and the committed in-memory state.
// Writes are appended to a write-ahead log as atomic batches and become
// visible through immutable snapshots, so concurrent readers always observe
// a fully committed state.
package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/guoyyit/trace-analytics-go/config"
	"github.com/guoyyit/trace-analytics-go/model"
)

// Batch is one atomic ingestion unit.
type Batch struct {
	ID    string
	Spans []model.Span
}

// walFrame is one durable, line-delimited commit record. JSON lines mean a
// torn tail write only loses that line and never corrupts earlier commits.
type walFrame struct {
	Seq   uint64       `json:"seq"`
	ID    string       `json:"id"`
	Spans []model.Span `json:"spans"`
}

// Store persists span batches and serves immutable snapshots.
type Store struct {
	cfg *config.Config

	mu      sync.Mutex
	head    *Snapshot
	history map[uint64]*Snapshot

	stage         *Snapshot
	stageRejected []model.RejectedSpan

	wal       *os.File
	walWriter *bufio.Writer

	quarantine    []model.RejectedSpan
	replayDropped int
}

// Open opens (or creates) the store in dir and replays any durable data.
// Corrupt tail records are skipped and counted; they never prevent startup.
func Open(dir string, cfg *config.Config) (*Store, error) {
	if cfg == nil {
		cfg = config.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, history: map[uint64]*Snapshot{}}
	s.head = emptySnapshot()
	s.history[0] = s.head

	walPath := filepath.Join(dir, "wal.log")
	dropped, err := s.replay(walPath)
	if err != nil {
		return nil, err
	}
	s.replayDropped = dropped
	if err := s.compact(walPath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.wal = f
	s.walWriter = bufio.NewWriter(f)
	return s, nil
}

// Close flushes and releases all resources.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	if err := s.walWriter.Flush(); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	return s.wal.Close()
}

// Commit validates and atomically applies one batch, returning counts of
// newly accepted spans and isolated (duplicate/conflict/limit) spans.
func (s *Store) Commit(b *Batch) (accepted int, rejected []model.RejectedSpan, err error) {
	if b == nil {
		return 0, nil, errors.New("nil batch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	seq := s.head.seq + 1
	if err := s.appendFrame(&walFrame{Seq: seq, ID: b.ID, Spans: b.Spans}); err != nil {
		return 0, nil, err
	}
	accepted, rejected = s.apply(seq, b.Spans)
	s.publish(seq)
	return accepted, rejected, nil
}

// Current returns the latest committed immutable snapshot.
func (s *Store) Current() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head
}

// SnapshotAt returns a retained snapshot for the sequence number.
func (s *Store) SnapshotAt(seq uint64) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap, ok := s.history[seq]; ok {
		return snap, nil
	}
	return nil, model.ErrCursorExpired
}

// ReplayDropped reports corrupt WAL records skipped during the last open.
func (s *Store) ReplayDropped() int { return s.replayDropped }

func (s *Store) appendFrame(f *walFrame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if _, err := s.walWriter.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := s.walWriter.Flush(); err != nil {
		return err
	}
	if s.cfg.SyncWAL {
		return s.wal.Sync()
	}
	return nil
}

// replay replays committed frames. Malformed lines are skipped and counted;
// a missing or corrupt tail never blocks startup.
func (s *Store) replay(walPath string) (int, error) {
	f, err := os.OpenFile(walPath, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	dropped := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var frame walFrame
		if err := json.Unmarshal(line, &frame); err != nil || frame.Seq == 0 || len(frame.Spans) == 0 {
			dropped++
			continue
		}
		_, rejected := s.apply(frame.Seq, frame.Spans)
		s.publishReplay(frame.Seq, rejected)
	}
	return dropped, nil
}

// compact rewrites the WAL from the current state, one frame per trace, so
// restart replay stays bounded and superseded duplicates are dropped.
func (s *Store) compact(walPath string) error {
	head := s.head
	tmp := walPath + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	ids := head.AllTraceIDs()
	for i, id := range ids {
		td := head.resolveTrace(id)
		spans := make([]model.Span, 0, len(td.spans))
		for _, sp := range td.spans {
			spans = append(spans, sp)
		}
		sort.Slice(spans, func(i, j int) bool { return spans[i].SpanID < spans[j].SpanID })
		data, err := json.Marshal(walFrame{Seq: uint64(i + 1), Spans: spans})
		if err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write(append(data, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, walPath)
}

// apply classifies spans into a staged snapshot that is not visible yet.
func (s *Store) apply(seq uint64, spans []model.Span) (int, []model.RejectedSpan) {
	next := s.head.derive(seq)
	accepted := 0
	var rejected []model.RejectedSpan

	for _, sp := range spans {
		td := next.resolveTrace(sp.TraceID)
		if td == nil {
			td = &traceData{
				spans:         map[string]model.Span{},
				services:      map[string]struct{}{},
				startUnixNano: sp.StartUnixNano,
				endUnixNano:   sp.EndUnixNano(),
				createdSeq:    seq,
			}
		} else {
			td = td.clone()
		}

		if existing, dup := td.spans[sp.SpanID]; dup {
			if spansEqual(existing, sp) {
				rejected = append(rejected, model.RejectedSpan{Span: sp, Reason: model.RejectDuplicate})
			} else {
				rejected = append(rejected, model.RejectedSpan{
					Span: sp, Reason: model.RejectConflict,
					Detail: "spanId already committed with a different body",
				})
			}
			next.overlay[sp.TraceID] = td
			continue
		}
		if s.cfg.MaxSpansPerTrace > 0 && len(td.spans) >= s.cfg.MaxSpansPerTrace {
			rejected = append(rejected, model.RejectedSpan{
				Span: sp, Reason: model.RejectLimitExceeded,
				Detail: fmt.Sprintf("max %d spans per trace", s.cfg.MaxSpansPerTrace),
			})
			next.overlay[sp.TraceID] = td
			continue
		}
		if s.cfg.MaxSpans > 0 && s.head.totalSpans+accepted >= s.cfg.MaxSpans {
			rejected = append(rejected, model.RejectedSpan{
				Span: sp, Reason: model.RejectLimitExceeded,
				Detail: fmt.Sprintf("global max %d spans reached", s.cfg.MaxSpans),
			})
			next.overlay[sp.TraceID] = td
			continue
		}

		td.spans[sp.SpanID] = sp
		if sp.StartUnixNano < td.startUnixNano {
			td.startUnixNano = sp.StartUnixNano
		}
		if sp.EndUnixNano() > td.endUnixNano {
			td.endUnixNano = sp.EndUnixNano()
		}
		if sp.Status == model.StatusError {
			td.hasError = true
		}
		td.services[sp.Service] = struct{}{}
		next.overlay[sp.TraceID] = td
		next.touchService(sp.Service, sp.TraceID)
		accepted++
	}

	next.totalSpans = s.head.totalSpans + accepted
	s.stage = next
	s.stageRejected = rejected
	return accepted, rejected
}

// touchService records that service now contains traceID in the overlay.
func (s *Snapshot) touchService(service, traceID string) {
	set := s.svcOverlay[service]
	if set == nil {
		set = map[string]struct{}{}
		s.svcOverlay[service] = set
	}
	set[traceID] = struct{}{}
}

// publish installs the staged snapshot and trims retained history.
func (s *Store) publish(seq uint64) {
	next := s.stage
	if next == nil {
		return
	}
	s.stage = nil
	rejected := s.stageRejected
	s.stageRejected = nil
	s.install(seq, next, rejected)
}

// publishReplay installs during WAL replay without touching the WAL.
func (s *Store) publishReplay(seq uint64, rejected []model.RejectedSpan) {
	next := s.stage
	if next == nil {
		return
	}
	s.stage = nil
	s.install(seq, next, rejected)
}

func (s *Store) install(seq uint64, next *Snapshot, rejected []model.RejectedSpan) {
	if len(rejected) > 0 {
		s.quarantine = appendRing(s.quarantine, rejected, s.cfg.MaxQuarantineRecords)
	}
	next.rejected = s.quarantine

	retain := s.cfg.SnapshotHistory
	if retain < 1 {
		retain = 1
	}
	if seq%uint64(retain) == 0 {
		next.materialize()
	}

	s.head = next
	s.history[seq] = next
	minSeq := uint64(1)
	if seq > uint64(retain) {
		minSeq = seq - uint64(retain) + 1
	}
	for k := range s.history {
		if k < minSeq && k != 0 {
			delete(s.history, k)
		}
	}
}
