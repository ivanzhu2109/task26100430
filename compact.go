package mq

import (
	"os"
	"path/filepath"
)

// Compact 回收所有消费组均已确认的消息正文及历史记录。
// 快照先写入临时文件并 fsync，再原子 rename 替换旧日志，
// 最后 fsync 目录。崩溃时旧日志或新日志必有一份完整可用。
func (q *Queue) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}

	// 收集真实存活的 ID（过滤惰性删除项），保持升序。
	order := make([]uint64, 0, len(q.live))
	for _, id := range q.order {
		if _, ok := q.live[id]; ok {
			order = append(order, id)
		}
	}
	payload := encodeSnapshot(q.nextID, order, q.live)
	buf := encodeRecord(recSnapshot, payload)

	tmpPath := filepath.Join(q.dir, newName)
	logPath := filepath.Join(q.dir, logName)

	nf, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := nf.Write(buf); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Sync(); err != nil {
		nf.Close()
		return err
	}
	if err := nf.Close(); err != nil {
		return err
	}
	faultCheck("compact-new-written")

	// 原子切换：rename 之后新日志生效。
	if err := os.Rename(tmpPath, logPath); err != nil {
		return err
	}
	faultCheck("compact-renamed")

	// 持久化目录项，保证 rename 本身在崩溃后仍然有效。
	d, err := os.Open(q.dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}

	// 切换到新文件继续追加。
	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return err
	}
	old := q.file
	q.file = f
	old.Close()

	q.order = order
	return nil
}
