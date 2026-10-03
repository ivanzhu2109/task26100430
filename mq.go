// Package mq 实现本地文件持久化的 FIFO 消息队列。
//
// 固定 3 个消费组，均从队列创建时开始订阅。消息为不透明字节串，
// ID 自 1 起按发布顺序严格递增，绝不复用。支持消费确认、
// 物理压缩与崩溃恢复；重新打开目录时自动恢复。
package mq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// NumGroups 是固定的消费组数量。
const NumGroups = 3

// allMask 表示所有组均未确认。
const allMask = (1 << NumGroups) - 1

// Message 是 Read 返回的一条消息。
type Message struct {
	ID   uint64
	Body []byte
}

// entry 是内存中的存活消息。
type entry struct {
	body []byte
	// pend 的第 g 位为 1 表示组 g 尚未确认该消息。
	pend uint8
}

// Queue 是打开的消息队列实例。方法可并发调用（内部串行化）。
type Queue struct {
	mu     sync.Mutex
	dir    string
	file   *os.File
	nextID uint64
	live   map[uint64]*entry
	order  []uint64 // 升序，可能包含已删除的惰性项
	closed bool
}

// ErrClosed 表示队列已关闭。
var ErrClosed = errors.New("mq: queue closed")

// Open 打开（必要时创建）dir 下的队列，并自动执行崩溃恢复。
func Open(dir string) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// 清理压缩中断残留的新文件；未完成的切换以旧文件为准。
	if err := os.Remove(filepath.Join(dir, newName)); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	q := &Queue{
		dir:    dir,
		nextID: 1,
		live:   make(map[uint64]*entry),
	}
	apply := func(typ uint32, p []byte) error {
		return q.applyRecord(typ, p)
	}
	f, err := applyLog(filepath.Join(dir, logName), apply)
	if err != nil {
		return nil, err
	}
	q.file = f
	return q, nil
}

// applyRecord 在恢复扫描时把一条记录应用到内存状态。
func (q *Queue) applyRecord(typ uint32, p []byte) error {
	switch typ {
	case recPublish:
		firstID, batch, err := decodePublish(p)
		if err != nil {
			return err
		}
		for i, b := range batch {
			id := firstID + uint64(i)
			body := make([]byte, len(b))
			copy(body, b)
			q.live[id] = &entry{body: body, pend: allMask}
			q.order = append(q.order, id)
		}
		if last := firstID + uint64(len(batch)); last > q.nextID {
			q.nextID = last
		}
	case recAck:
		group, ids, err := decodeAck(p)
		if err != nil {
			return err
		}
		if group < 0 || group >= NumGroups {
			return fmt.Errorf("mq: bad group %d in log", group)
		}
		q.applyAck(group, ids)
	case recSnapshot:
		nextID, ids, masks, bodies, err := decodeSnapshot(p)
		if err != nil {
			return err
		}
		q.live = make(map[uint64]*entry, len(ids))
		q.order = q.order[:0]
		for i, id := range ids {
			body := make([]byte, len(bodies[i]))
			copy(body, bodies[i])
			q.live[id] = &entry{body: body, pend: masks[i]}
			q.order = append(q.order, id)
		}
		if nextID > q.nextID {
			q.nextID = nextID
		}
	default:
		return fmt.Errorf("mq: unknown record type %d", typ)
	}
	return nil
}

// applyAck 把一组确认应用到内存状态（调用方需持锁）。
func (q *Queue) applyAck(group int, ids []uint64) {
	bit := uint8(1 << group)
	for _, id := range ids {
		e, ok := q.live[id]
		if !ok {
			continue // 已确认或不存在：重复确认无副作用
		}
		e.pend &^= bit
		if e.pend == 0 {
			delete(q.live, id) // order 中惰性删除
		}
	}
}

// Publish 原子发布一批消息，返回每条消息的 ID（按输入顺序）。
func (q *Queue) Publish(batch [][]byte) ([]uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if len(batch) == 0 {
		return nil, nil
	}
	firstID := q.nextID
	if err := q.appendRecord(recPublish, encodePublish(firstID, batch), "publish"); err != nil {
		return nil, err
	}
	ids := make([]uint64, len(batch))
	for i, b := range batch {
		id := firstID + uint64(i)
		ids[i] = id
		body := make([]byte, len(b))
		copy(body, b)
		q.live[id] = &entry{body: body, pend: allMask}
		q.order = append(q.order, id)
	}
	q.nextID = firstID + uint64(len(batch))
	return ids, nil
}

// Read 按 ID 递增返回该组最多 limit 条未确认消息，不改变消费状态。
func (q *Queue) Read(group int, limit int) ([]Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	if group < 0 || group >= NumGroups {
		return nil, fmt.Errorf("mq: bad group %d", group)
	}
	if limit <= 0 {
		return nil, nil
	}
	bit := uint8(1 << group)
	var out []Message
	for _, id := range q.order {
		e, ok := q.live[id]
		if !ok || e.pend&bit == 0 {
			continue
		}
		out = append(out, Message{ID: id, Body: e.body})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// Ack 原子确认一组 ID，允许乱序与重复确认；不影响其他组。
func (q *Queue) Ack(group int, ids []uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if group < 0 || group >= NumGroups {
		return fmt.Errorf("mq: bad group %d", group)
	}
	if len(ids) == 0 {
		return nil
	}
	if err := q.appendRecord(recAck, encodeAck(group, ids), "ack"); err != nil {
		return err
	}
	q.applyAck(group, ids)
	return nil
}

// Close 关闭队列。
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.file.Close()
}
