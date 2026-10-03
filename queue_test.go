package fifoq

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func body(s string) []byte { return []byte(s) }

func publishStrings(t *testing.T, q *Queue, ss ...string) []uint64 {
	t.Helper()
	batch := make([][]byte, len(ss))
	for i, s := range ss {
		batch[i] = body(s)
	}
	ids, err := q.Publish(batch)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Fatalf("IDs not strictly increasing: %v", ids)
		}
	}
	return ids
}

func readStrings(t *testing.T, q *Queue, group, limit int) []string {
	t.Helper()
	msgs, err := q.Read(group, limit)
	if err != nil {
		t.Fatalf("Read(%d): %v", group, err)
	}
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Body)
		if i > 0 && m.ID <= msgs[i-1].ID {
			t.Fatalf("Read(%d) not in increasing ID order", group)
		}
	}
	return out
}

func reopen(t *testing.T, q *Queue, dir string) *Queue {
	t.Helper()
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	q2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return q2
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBasicFlow covers publish, repeated reads, out-of-order and
// duplicate acks, and group isolation.
func TestBasicFlow(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := publishStrings(t, q, "a", "b", "c")
	if !equalStrings(toStrings(ids), []string{"1", "2", "3"}) {
		t.Fatalf("first IDs = %v, want 1,2,3", ids)
	}
	// Repeated reads return the same messages; read does not consume.
	for i := 0; i < 2; i++ {
		if got := readStrings(t, q, 0, 10); !equalStrings(got, []string{"a", "b", "c"}) {
			t.Fatalf("read %d = %v", i, got)
		}
	}
	// Out-of-order + duplicate ack.
	if err := q.Ack(0, []uint64{ids[2], ids[0], ids[2]}); err != nil {
		t.Fatal(err)
	}
	if got := readStrings(t, q, 0, 10); !equalStrings(got, []string{"b"}) {
		t.Fatalf("after ack group0 = %v", got)
	}
	// Other groups unaffected.
	if got := readStrings(t, q, 1, 10); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("group1 = %v", got)
	}
	// Limit respected.
	if got := readStrings(t, q, 1, 2); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("group1 limit2 = %v", got)
	}
	q.Close()
}

func toStrings(ids []uint64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = fmt.Sprint(id)
	}
	return out
}

// TestInterleavedAcksCompactReopen builds the required scenario: three
// groups acking interleaved and out of order, an early message still
// needed by one group while later messages are fully acknowledged.
// After Compact and reopen, full per-group reads and bodies are
// verified, not just counts.
func TestInterleavedAcksCompactReopen(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 8 messages: m1..m8
	var all []uint64
	for i := 1; i <= 8; i++ {
		all = append(all, publishStrings(t, q, fmt.Sprintf("m%d", i))...)
	}
	id := func(i int) uint64 { return all[i-1] }

	// Group 0: acks everything except m1 (out of order).
	q.Ack(0, []uint64{id(5), id(2), id(8), id(3), id(7), id(4), id(6)})
	// Group 1: keeps m1 and m4.
	q.Ack(1, []uint64{id(2), id(3), id(5), id(6), id(7), id(8)})
	// Group 2: keeps m4.
	q.Ack(2, []uint64{id(1), id(2), id(3), id(5), id(6), id(7), id(8)})

	// Live: m1 (needs g0,g1), m4 (needs g1,g2). m2,m3,m5..m8 fully acked.
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)

	if got := readStrings(t, q, 0, 100); !equalStrings(got, []string{"m1"}) {
		t.Fatalf("group0 = %v", got)
	}
	if got := readStrings(t, q, 1, 100); !equalStrings(got, []string{"m1", "m4"}) {
		t.Fatalf("group1 = %v", got)
	}
	if got := readStrings(t, q, 2, 100); !equalStrings(got, []string{"m4"}) {
		t.Fatalf("group2 = %v", got)
	}
	// Bodies byte-exact.
	msgs, _ := q.Read(1, 100)
	if !bytes.Equal(msgs[0].Body, body("m1")) || !bytes.Equal(msgs[1].Body, body("m4")) {
		t.Fatalf("bodies corrupted: %q %q", msgs[0].Body, msgs[1].Body)
	}
	// IDs preserved.
	if msgs[0].ID != id(1) || msgs[1].ID != id(4) {
		t.Fatalf("IDs = %d,%d", msgs[0].ID, msgs[1].ID)
	}
	// Duplicate old acks are no-ops and must not confirm new messages.
	if err := q.Ack(0, []uint64{id(2), id(3)}); err != nil {
		t.Fatal(err)
	}
	// Continue publishing after compaction: IDs keep increasing.
	newIDs := publishStrings(t, q, "m9", "m10")
	if newIDs[0] != id(8)+1 {
		t.Fatalf("ID reused after compact: got %d want %d", newIDs[0], id(8)+1)
	}
	q = reopen(t, q, dir)
	if got := readStrings(t, q, 0, 100); !equalStrings(got, []string{"m1", "m9", "m10"}) {
		t.Fatalf("group0 after republish = %v", got)
	}
	if got := readStrings(t, q, 2, 100); !equalStrings(got, []string{"m4", "m9", "m10"}) {
		t.Fatalf("group2 after republish = %v", got)
	}
	q.Close()
}

// TestCompactEmptyThenReuse: all messages acked, compact to empty,
// reopen, publish again (IDs must not be reused), old acks no-op.
func TestCompactEmptyThenReuse(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := publishStrings(t, q, "x", "y", "z")
	for g := 0; g < NumGroups; g++ {
		if err := q.Ack(g, ids); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)
	for g := 0; g < NumGroups; g++ {
		if got := readStrings(t, q, g, 100); len(got) != 0 {
			t.Fatalf("group %d not empty: %v", g, got)
		}
	}
	ids2 := publishStrings(t, q, "p", "q")
	if ids2[0] != ids[2]+1 {
		t.Fatalf("ID reused: got %d want %d", ids2[0], ids[2]+1)
	}
	// Old duplicate acks must not confirm the new messages.
	for g := 0; g < NumGroups; g++ {
		if err := q.Ack(g, ids); err != nil {
			t.Fatal(err)
		}
	}
	q = reopen(t, q, dir)
	if got := readStrings(t, q, 0, 100); !equalStrings(got, []string{"p", "q"}) {
		t.Fatalf("group0 = %v", got)
	}
	q.Close()
}

// TestSizeBoundAfterCompact checks the directory size constraint after
// Compact: sum of file sizes <= 2*liveBodies + 256*liveMsgs + 64KiB.
func TestSizeBoundAfterCompact(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var liveBodies, liveMsgs int64
	var all []uint64
	for i := 0; i < 50; i++ {
		b := bytes.Repeat([]byte{byte(i)}, 100)
		ids, _ := q.Publish([][]byte{b})
		all = append(all, ids[0])
		if i%3 == 0 { // keep every third message live for group 0
			liveBodies += 100
			liveMsgs++
		}
	}
	// Fully ack all but every third message.
	for g := 0; g < NumGroups; g++ {
		var ids []uint64
		for i, id := range all {
			if g == 0 && i%3 == 0 {
				continue // group 0 keeps these
			}
			ids = append(ids, id)
		}
		q.Ack(g, ids)
	}
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fi, _ := e.Info()
		total += fi.Size()
	}
	bound := 2*liveBodies + 256*liveMsgs + 64*1024
	if total > bound {
		t.Fatalf("dir size %d exceeds bound %d", total, bound)
	}
	q.Close()
}
