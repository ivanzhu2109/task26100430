// Package fifoq implements a local-file persistent FIFO message queue
// with fixed consumer groups, acknowledgements, compaction and crash
// recovery. Only the Go standard library is used.
//
// Layout of the queue directory:
//
//	data.log  append-only record log (publish/ack batches)
//	snap.dat  compaction snapshot (atomically renamed into place)
//	snap.tmp  in-progress snapshot, removed during recovery
package fifoq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// NumGroups is the fixed number of consumer groups. Every group
// subscribes from queue creation and must acknowledge messages
// independently.
const NumGroups = 3

// Message is a single queued message. Body is an opaque byte string.
type Message struct {
	ID   uint64
	Body []byte
}

// Queue is a persistent FIFO queue stored in a single directory.
// Methods must be called serially from a single process; the directory
// must not be opened by more than one process at a time.
type Queue struct {
	mu      sync.Mutex
	dir     string
	log     *os.File // active append-only log (data.log)
	logBase uint64   // virtual offset horizon stored in the log header
	logLen  uint64   // bytes of records appended after the header

	nextID uint64
	order  []uint64 // retained message IDs, ascending
	msgs   map[uint64]*msgEnt

	closed     bool
	writeBytes int64 // test instrumentation: payload bytes written since Open
}

type msgEnt struct {
	body []byte
	ack  [NumGroups]bool
}

func (m *msgEnt) ackedByAll() bool {
	for _, a := range m.ack {
		if !a {
			return false
		}
	}
	return true
}

// Open opens (or creates) the queue in dir and recovers its state.
func Open(dir string) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	q := &Queue{dir: dir, nextID: 1, msgs: make(map[uint64]*msgEnt)}
	if err := q.recover(); err != nil {
		return nil, err
	}
	return q, nil
}

// recover rebuilds in-memory state from snap.dat and data.log.
func (q *Queue) recover() error {
	// Remove any incomplete snapshot from an interrupted Compact.
	tmp := filepath.Join(q.dir, "snap.tmp")
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Load the snapshot, if any. horizon is the virtual log offset the
	// snapshot reflects.
	var horizon uint64
	snapPath := filepath.Join(q.dir, "snap.dat")
	if _, err := os.Stat(snapPath); err == nil {
		h, err := q.loadSnapshot(snapPath)
		if err != nil {
			return err
		}
		horizon = h
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Open or create the log.
	logPath := filepath.Join(q.dir, "data.log")
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	q.log = f

	base, ok, err := readLogHeader(f)
	if err != nil {
		return err
	}
	if !ok {
		// Missing or torn header: reinitialize at the snapshot horizon.
		if err := f.Truncate(0); err != nil {
			return err
		}
		if err := writeLogHeader(f, horizon); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		q.logBase = horizon
		q.logLen = 0
		return nil
	}
	if base > horizon {
		return fmt.Errorf("fifoq: log base %d ahead of snapshot horizon %d", base, horizon)
	}
	q.logBase = base
	// Replay records at or beyond the snapshot horizon; older records
	// are already reflected in the snapshot. Torn tails are truncated.
	return q.replayLog(f, base, horizon, q.applyRecord)
}

// applyRecord applies one committed log record during replay.
func (q *Queue) applyRecord(typ byte, payload []byte) error {
	switch typ {
	case recPublish:
		bodies, err := decodePublishPayload(payload)
		if err != nil {
			return err
		}
		q.applyPublish(bodies)
	case recAck:
		group, ids, err := decodeAckPayload(payload)
		if err != nil {
			return err
		}
		q.applyAck(group, ids)
	default:
		return errCorrupt
	}
	return nil
}

func decodePublishPayload(p []byte) ([][]byte, error) {
	if len(p) < 4 {
		return nil, errCorrupt
	}
	n := int(u32(p[0:4]))
	p = p[4:]
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if len(p) < 4 {
			return nil, errCorrupt
		}
		l := int(u32(p[0:4]))
		p = p[4:]
		if len(p) < l {
			return nil, errCorrupt
		}
		body := make([]byte, l)
		copy(body, p[:l])
		p = p[l:]
		out = append(out, body)
	}
	return out, nil
}

func decodeAckPayload(p []byte) (int, []uint64, error) {
	if len(p) < 5 {
		return 0, nil, errCorrupt
	}
	group := int(p[0])
	n := int(u32(p[1:5]))
	p = p[5:]
	if len(p) < 8*n {
		return 0, nil, errCorrupt
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = u64(p[8*i:])
	}
	return group, ids, nil
}

func (q *Queue) applyPublish(bodies [][]byte) {
	for _, b := range bodies {
		id := q.nextID
		q.nextID++
		q.msgs[id] = &msgEnt{body: b}
		q.order = append(q.order, id)
	}
}

func (q *Queue) applyAck(group int, ids []uint64) {
	if group < 0 || group >= NumGroups {
		return
	}
	for _, id := range ids {
		if m, ok := q.msgs[id]; ok {
			m.ack[group] = true
		}
	}
}

// Close releases resources. All operations are already durable.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.log.Close()
}

func (q *Queue) checkOpen() error {
	if q.closed {
		return errors.New("fifoq: queue is closed")
	}
	return nil
}

// Publish atomically appends a batch and returns strictly increasing
// IDs in input order.
func (q *Queue) Publish(batch [][]byte) ([]uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, nil
	}
	rec := encodeRecord(recPublish, encodePublishPayload(batch))
	if err := q.appendRecord(rec); err != nil {
		return nil, err
	}
	ids := make([]uint64, len(batch))
	for i := range ids {
		ids[i] = q.nextID + uint64(i)
	}
	q.applyPublish(batch)
	return ids, nil
}

// Read returns up to limit unacknowledged messages for group, in
// increasing ID order, without changing consumption state.
func (q *Queue) Read(group int, limit int) ([]Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return nil, err
	}
	if group < 0 || group >= NumGroups {
		return nil, errBadGroup
	}
	if limit <= 0 {
		return nil, nil
	}
	var out []Message
	for _, id := range q.order {
		m := q.msgs[id]
		if m.ack[group] {
			continue
		}
		out = append(out, Message{ID: id, Body: m.body})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// Ack atomically acknowledges a set of IDs for one group. Out-of-order
// and duplicate acknowledgements are allowed and have no side effects.
func (q *Queue) Ack(group int, ids []uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}
	if group < 0 || group >= NumGroups {
		return errBadGroup
	}
	if len(ids) == 0 {
		return nil
	}
	rec := encodeRecord(recAck, encodeAckPayload(group, ids))
	if err := q.appendRecord(rec); err != nil {
		return err
	}
	q.applyAck(group, ids)
	return nil
}

// Compact physically reclaims messages acknowledged by all groups.
// Implemented in snapshot.go.

func u32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
func u64(b []byte) uint64 {
	return uint64(u32(b[0:4])) | uint64(u32(b[4:8]))<<32
}
