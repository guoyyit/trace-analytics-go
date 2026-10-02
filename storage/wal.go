package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/guoyyit/trace-analytics-go/model"
)

const (
	walFileName       = "wal.log"
	snapshotName      = "snapshot.json"
	snapshotTmp       = "snapshot.tmp"
	frameMagic   byte = 0x57
	frameBatch   byte = 1
	frameRej     byte = 2
)

// WAL is a frame-based write-ahead log. Each record is a committed batch;
// a torn/failing frame at EOF marks the end of the durable prefix and is
// truncated on open.
type WAL struct {
	path string
	file *os.File
	sync bool
	seq  uint64
	size int64
}

type walFrame struct {
	typ     byte
	payload []byte
}

// OpenWAL opens (or creates) the WAL. Any torn or corrupt trailing frame is
// discarded by truncating at the end of the last valid frame, which is how an
// uncommitted (partially written) batch is dropped after a crash.
func OpenWAL(dir string, sync bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, walFileName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, file: file, sync: sync}
	if err := w.recover(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) recover() error {
	data, err := io.ReadAll(w.file)
	if err != nil {
		return err
	}
	off := 0
	var valid int64
	var lastSeq uint64
	for off < len(data) {
		frame, n, err := decodeFrame(data[off:])
		if err != nil {
			break
		}
		if frame.typ == frameBatch {
			var b model.Batch
			if json.Unmarshal(frame.payload, &b) != nil {
				break
			}
			if b.Seq > lastSeq {
				lastSeq = b.Seq
			}
		}
		off += n
		valid = int64(off)
	}
	if valid < int64(len(data)) {
		if err := w.file.Truncate(valid); err != nil {
			return err
		}
	}
	w.size = valid
	w.seq = lastSeq
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func encodeFrame(typ byte, payload []byte) []byte {
	buf := make([]byte, 0, 6+len(payload))
	buf = append(buf, frameMagic, typ)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf = append(buf, lenBuf[:]...)
	var crcBuf [4]byte
	crcInput := make([]byte, 0, 6+len(payload))
	crcInput = append(crcInput, frameMagic, typ)
	crcInput = append(crcInput, lenBuf[:]...)
	crcInput = append(crcInput, payload...)
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(crcInput))
	buf = append(buf, crcBuf[:]...)
	buf = append(buf, payload...)
	return buf
}

func decodeFrame(data []byte) (walFrame, int, error) {
	const header = 10
	if len(data) < header || data[0] != frameMagic {
		return walFrame{}, 0, errors.New("bad frame header")
	}
	typ := data[1]
	if typ != frameBatch && typ != frameRej {
		return walFrame{}, 0, errors.New("unknown frame type")
	}
	n := int(binary.BigEndian.Uint32(data[2:6]))
	if len(data) < header+n {
		return walFrame{}, 0, errors.New("truncated frame")
	}
	wantCRC := binary.BigEndian.Uint32(data[6:10])
	crcInput := make([]byte, 0, 6+n)
	crcInput = append(crcInput, data[:6]...)
	crcInput = append(crcInput, data[header:header+n]...)
	if crc32.ChecksumIEEE(crcInput) != wantCRC {
		return walFrame{}, 0, errors.New("crc mismatch")
	}
	payload := make([]byte, n)
	copy(payload, data[header:header+n])
	return walFrame{typ: typ, payload: payload}, header + n, nil
}

// Append durably appends one committed batch.
func (w *WAL) Append(ctx context.Context, batch model.Batch) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	if err := w.writeFrame(encodeFrame(frameBatch, payload)); err != nil {
		return err
	}
	w.noteSeq(batch.Seq)
	return nil
}

// AppendRejected durably appends one isolated malformed-record description.
func (w *WAL) AppendRejected(ctx context.Context, rej model.RejectedSpan) error {
	payload, err := json.Marshal(rej)
	if err != nil {
		return err
	}
	return w.writeFrame(encodeFrame(frameRej, payload))
}

func (w *WAL) writeFrame(frame []byte) error {
	if _, err := w.file.Write(frame); err != nil {
		return err
	}
	if w.sync {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	w.size += int64(len(frame))
	return nil
}

func (w *WAL) noteSeq(seq uint64) {
	if seq > w.seq {
		w.seq = seq
	}
}

// Replay invokes fn for every committed batch in order.
func (w *WAL) Replay(ctx context.Context, fn func(model.Batch) error, onRej func(model.RejectedSpan)) error {
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}
	off := 0
	for off < len(data) {
		frame, n, err := decodeFrame(data[off:])
		if err != nil {
			break
		}
		off += n
		switch frame.typ {
		case frameBatch:
			var b model.Batch
			if err := json.Unmarshal(frame.payload, &b); err != nil {
				return err
			}
			if err := fn(b); err != nil {
				return err
			}
		case frameRej:
			var r model.RejectedSpan
			if err := json.Unmarshal(frame.payload, &r); err != nil {
				return err
			}
			if onRej != nil {
				onRej(r)
			}
		}
	}
	return nil
}

// LastSeq returns the highest committed batch sequence, 0 if empty.
func (w *WAL) LastSeq() uint64 { return w.seq }

func (w *WAL) Close() error {
	if err := w.file.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		// best effort
	}
	return w.file.Close()
}

// Rewrite replaces the log content with batches (used after snapshotting).
func (w *WAL) Rewrite(ctx context.Context, batches []model.Batch, lastSeq uint64) error {
	var buf bytes.Buffer
	for _, b := range batches {
		payload, err := json.Marshal(b)
		if err != nil {
			return err
		}
		buf.Write(encodeFrame(frameBatch, payload))
	}
	tmp := w.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	file, err = os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.size = int64(buf.Len())
	w.seq = lastSeq
	_, err = file.Seek(0, io.SeekEnd)
	return err
}

// snapshotData is the on-disk persisted state.
type snapshotData struct {
	LastSeq       uint64               `json:"lastSeq"`
	Traces        []persistedTrace     `json:"traces"`
	Rejected      []model.RejectedSpan `json:"rejected,omitempty"`
	RejectedTotal int64                `json:"rejectedTotal"`
}

type persistedTrace struct {
	TraceID string       `json:"traceId"`
	Spans   []model.Span `json:"spans"`
}

func atomicWriteSnapshot(dir string, data snapshotData) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, snapshotTmp)
	final := filepath.Join(dir, snapshotName)
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(dir)
}

func readSnapshot(dir string) (snapshotData, bool, error) {
	payload, err := os.ReadFile(filepath.Join(dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return snapshotData{}, false, nil
	}
	if err != nil {
		return snapshotData{}, false, err
	}
	var data snapshotData
	if err := json.Unmarshal(payload, &data); err != nil {
		// Corrupt snapshot must not prevent startup: start from WAL only.
		return snapshotData{}, false, nil
	}
	return data, true, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
