package mq

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

// readAll 循环 Read 直到取完该组全部未确认消息。
func readAll(t *testing.T, q *Queue, group int) []Message {
	t.Helper()
	var out []Message
	for {
		ms, err := q.Read(group, 1000)
		if err != nil {
			t.Fatalf("Read(%d): %v", group, err)
		}
		if len(ms) == 0 {
			return out
		}
		out = append(out, ms...)
		if len(ms) < 1000 {
			return out
		}
	}
}

// wantRead 核对该组读出的 ID 序列与正文。
func wantRead(t *testing.T, q *Queue, group int, want []Message) {
	t.Helper()
	got := readAll(t, q, group)
	if len(got) != len(want) {
		t.Fatalf("group %d: read %d messages, want %d\ngot IDs=%v", group, len(got), len(want), idsOf(got))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("group %d msg %d: ID=%d, want %d", group, i, got[i].ID, want[i].ID)
		}
		if !bytes.Equal(got[i].Body, want[i].Body) {
			t.Fatalf("group %d msg %d (ID=%d): body mismatch", group, i, got[i].ID)
		}
	}
}

func idsOf(ms []Message) []uint64 {
	ids := make([]uint64, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	return ids
}

func msgs(start, end uint64) []Message {
	out := make([]Message, 0, end-start+1)
	for id := start; id <= end; id++ {
		out = append(out, Message{ID: id, Body: []byte(fmt.Sprintf("msg-%d", id))})
	}
	return out
}

func publishN(t *testing.T, q *Queue, n int) []uint64 {
	t.Helper()
	var batch [][]byte
	base := q.nextID
	for i := 0; i < n; i++ {
		batch = append(batch, []byte(fmt.Sprintf("msg-%d", base+uint64(i))))
	}
	ids, err := q.Publish(batch)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, id := range ids {
		if id != base+uint64(i) {
			t.Fatalf("ids[%d]=%d, want %d", i, id, base+uint64(i))
		}
	}
	return ids
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		total += info.Size()
	}
	return total
}

func TestBasicPublishReadAck(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	publishN(t, q, 5)
	// 三个组都看到全部 5 条。
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q, g, msgs(1, 5))
	}
	// 重复 Read 返回相同结果（不改变消费状态）。
	wantRead(t, q, 0, msgs(1, 5))

	// 乱序确认，组间互不影响。
	if err := q.Ack(0, []uint64{4, 2, 5}); err != nil {
		t.Fatal(err)
	}
	wantRead(t, q, 0, append(msgs(1, 1), msgs(3, 3)...))
	wantRead(t, q, 1, msgs(1, 5))

	// 重复确认无副作用。
	if err := q.Ack(0, []uint64{2, 2, 99}); err != nil {
		t.Fatal(err)
	}
	wantRead(t, q, 0, append(msgs(1, 1), msgs(3, 3)...))
}

func TestReopenPersists(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	publishN(t, q, 4)
	q.Ack(1, []uint64{1, 3})
	q.Close()

	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	wantRead(t, q2, 0, msgs(1, 4))
	wantRead(t, q2, 1, append(msgs(2, 2), msgs(4, 4)...))
	wantRead(t, q2, 2, msgs(1, 4))
	// 重开后 ID 继续递增。
	ids := publishN(t, q2, 2)
	if ids[0] != 5 || ids[1] != 6 {
		t.Fatalf("ids after reopen = %v, want [5 6]", ids)
	}
}

// 三组交错、乱序确认：较早消息仍被某组需要，较晚消息已被全部确认。
// 压缩并重开后逐组核对完整读取结果与正文。
func TestInterleavedAcksCompactReopen(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	publishN(t, q, 10)

	// 组 0：确认 2..10，留下最早的 1。
	q.Ack(0, []uint64{5, 2, 9, 3, 10, 7, 4, 8, 6})
	// 组 1：确认 1 和 3..10，留下 2。
	q.Ack(1, []uint64{1, 7, 4, 10, 3, 6, 9, 5, 8})
	// 组 2：确认 3..10，留下 1、2。
	q.Ack(2, []uint64{8, 3, 6, 10, 4, 9, 5, 7})
	// 此时 3..10 已被所有组确认，1、2 仍被部分组需要。

	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q.Close()

	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	wantRead(t, q2, 0, msgs(1, 1))
	wantRead(t, q2, 1, msgs(2, 2))
	wantRead(t, q2, 2, msgs(1, 2))

	// 压缩后尺寸上界：存活 2 条，正文共 11 字节。
	live := int64(len("msg-1") + len("msg-2"))
	if sz := dirSize(t, dir); sz > 2*live+2*256+64*1024 {
		t.Fatalf("dir size %d exceeds bound", sz)
	}

	// 继续发布，ID 不复用；再次压缩重开后结果仍正确。
	publishN(t, q2, 3) // 11,12,13
	q2.Ack(0, []uint64{1, 11})
	q2.Ack(1, []uint64{2, 12})
	q2.Ack(2, []uint64{1, 2, 13})
	if err := q2.Compact(); err != nil {
		t.Fatal(err)
	}
	q2.Close()

	q3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	wantRead(t, q3, 0, append(msgs(12, 12), msgs(13, 13)...))
	wantRead(t, q3, 1, append(msgs(11, 11), msgs(13, 13)...))
	wantRead(t, q3, 2, msgs(11, 12))
}

// 全部确认后压缩为空、重开、继续发布、重复旧 Ack。
func TestCompactEmptyThenReuse(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	publishN(t, q, 6)
	for g := 0; g < NumGroups; g++ {
		q.Ack(g, []uint64{1, 2, 3, 4, 5, 6})
	}
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q.Close()

	// 目录里只剩一个很小的快照头。
	if sz := dirSize(t, dir); sz > 64*1024 {
		t.Fatalf("dir size %d after empty compact", sz)
	}

	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q2, g, nil)
	}
	// ID 不复用：下一条必须是 7。
	ids := publishN(t, q2, 2)
	if ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("ids = %v, want [7 8]", ids)
	}
	// 旧 ID 的重复 Ack 不得确认新消息。
	for g := 0; g < NumGroups; g++ {
		q2.Ack(g, []uint64{1, 2, 3, 4, 5, 6})
	}
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q2, g, msgs(7, 8))
	}
	// 再次压缩重开，新消息仍在。
	q2.Compact()
	q2.Close()
	q3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q3, g, msgs(7, 8))
	}
}

// 10 万条积压下，少量 Publish/Ack 的实际文件写入量只随输入增长。
func TestWriteAmplification(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	const n = 100000
	const body = "0123456789abcdef" // 16 字节
	// 分批发布 10 万条。
	batch := make([][]byte, 1000)
	for i := range batch {
		batch[i] = []byte(body)
	}
	for i := 0; i < n/1000; i++ {
		if _, err := q.Publish(batch); err != nil {
			t.Fatal(err)
		}
	}
	// 确认其中 9 万条（不压缩，保持积压）。
	var ids []uint64
	for id := uint64(1); id <= 90000; id++ {
		ids = append(ids, id)
	}
	if err := q.Ack(0, ids); err != nil {
		t.Fatal(err)
	}
	q.Close()

	before := dirSize(t, dir)
	t.Logf("log size with 100k backlog: %d bytes", before)

	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	// 少量 Publish：10 条 16 字节消息。
	small := make([][]byte, 10)
	for i := range small {
		small[i] = []byte(body)
	}
	if _, err := q2.Publish(small); err != nil {
		t.Fatal(err)
	}
	// 少量 Ack：20 个 ID。
	ackIDs := make([]uint64, 20)
	for i := range ackIDs {
		ackIDs[i] = uint64(90001 + i)
	}
	if err := q2.Ack(1, ackIDs); err != nil {
		t.Fatal(err)
	}
	q2.Close()

	after := dirSize(t, dir)
	growth := after - before
	t.Logf("growth after 10 publishes + 20 acks: %d bytes", growth)
	// 上界：记录头 + 10*(4+16) + 8+20*8，给足余量仍远小于历史体量。
	if growth > 4096 {
		t.Fatalf("growth %d bytes exceeds 4096; history is being rewritten", growth)
	}
}

// 压缩后目录总大小的精确上界校验。
func TestCompactSizeBound(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	var batch [][]byte
	var liveBytes int64
	for i := 0; i < 100; i++ {
		b := bytes.Repeat([]byte{byte(i)}, 100)
		batch = append(batch, b)
		liveBytes += int64(len(b))
	}
	q.Publish(batch)
	// 只确认组 0 和 1，组 2 全部保留 → 100 条全存活。
	q.Ack(0, idsOf(msgs(1, 100)))
	q.Ack(1, idsOf(msgs(1, 100)))
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q.Close()
	bound := 2*liveBytes + 100*256 + 64*1024
	if sz := dirSize(t, dir); sz > bound {
		t.Fatalf("dir size %d exceeds bound %d", sz, bound)
	}
	// 重开后无临时文件残留，尺寸仍满足上界。
	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	if sz := dirSize(t, dir); sz > bound {
		t.Fatalf("dir size %d after reopen exceeds bound %d", sz, bound)
	}
	var want []Message
	for i := 0; i < 100; i++ {
		want = append(want, Message{ID: uint64(i + 1), Body: bytes.Repeat([]byte{byte(i)}, 100)})
	}
	wantRead(t, q2, 2, want)
	wantRead(t, q2, 0, nil)
}
