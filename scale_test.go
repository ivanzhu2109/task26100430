package fifoq

import (
	"fmt"
	"testing"
)

// TestWriteAmplification builds a 100k-message backlog, then measures
// the actual bytes written to files by a few small Publish and Ack
// calls. The writes must grow only with the operation input, never
// with history or live-body volume (i.e. no hidden full-state rewrite
// on the normal path).
func TestWriteAmplification(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 100k backlog messages, ~32 bytes each, in batches of 1000.
	const backlog = 100000
	batch := make([][]byte, 1000)
	for i := range batch {
		batch[i] = []byte(fmt.Sprintf("backlog-message-%032d", i))
	}
	for n := 0; n < backlog; n += len(batch) {
		if _, err := q.Publish(batch); err != nil {
			t.Fatal(err)
		}
	}
	// Ack most of them for group 0 and 1 so the backlog is realistic.
	all := make([]uint64, 0, backlog)
	for id := uint64(1); id <= backlog; id++ {
		all = append(all, id)
	}
	if err := q.Ack(0, all); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(1, all); err != nil {
		t.Fatal(err)
	}

	// Measure: a few small operations.
	q.writeBytes = 0
	var inputBytes int64
	for i := 0; i < 5; i++ {
		b := []byte(fmt.Sprintf("new-message-%d", i))
		inputBytes += int64(len(b))
		if _, err := q.Publish([][]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	ids := []uint64{backlog + 1, backlog + 2, backlog + 3}
	inputBytes += 8 * int64(len(ids))
	if err := q.Ack(2, ids); err != nil {
		t.Fatal(err)
	}
	written := q.writeBytes
	t.Logf("backlog=%d liveBodies~%dB; small ops wrote %d bytes (input %d bytes)",
		backlog, backlog*32, written, inputBytes)
	// Generous bound: per-op record overhead is tens of bytes. Anything
	// approaching the backlog size would indicate history rewriting.
	if written > 64*1024 {
		t.Fatalf("small ops wrote %d bytes; history was rewritten", written)
	}
	// And it must be proportional to input, not to the 100k backlog.
	if written > inputBytes+16*1024 {
		t.Fatalf("write amplification: wrote %d for %d input bytes", written, inputBytes)
	}
	q.Close()
}

// TestEmptyAndBinaryBodies covers edge-case payloads: empty bodies and
// arbitrary binary bytes survive a reopen byte-for-byte.
func TestEmptyAndBinaryBodies(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	bin := make([]byte, 256)
	for i := range bin {
		bin[i] = byte(i)
	}
	if _, err := q.Publish([][]byte{{}, bin, {0, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)
	msgs, err := q.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 || len(msgs[0].Body) != 0 || len(msgs[2].Body) != 3 {
		t.Fatalf("bad bodies: %v", msgs)
	}
	for i := range bin {
		if msgs[1].Body[i] != byte(i) {
			t.Fatalf("binary body corrupted at %d", i)
		}
	}
	q.Close()
}
