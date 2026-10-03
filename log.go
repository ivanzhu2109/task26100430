package mq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// 目录内的文件名。
const (
	logName = "mq.log"
	newName = "mq.log.new"
)

// 记录类型。
const (
	recPublish  uint32 = 1
	recAck      uint32 = 2
	recSnapshot uint32 = 3
)

// 记录帧：type u32 | payloadLen u32 | crc32 u32 | payload
// crc 覆盖前 8 字节头与 payload，用于识别半写记录。
const recordHeaderLen = 12

// maxPayload 是单条记录的合理上限，超过视为损坏。
const maxPayload = 1 << 30

var errCorrupt = errors.New("mq: corrupt record")

// encodeRecord 编码一条完整记录。
func encodeRecord(typ uint32, payload []byte) []byte {
	buf := make([]byte, recordHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], typ)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(buf[0:8])
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	binary.LittleEndian.PutUint32(buf[8:12], crc)
	copy(buf[recordHeaderLen:], payload)
	return buf
}

// appendRecord 追加一条记录并 fsync。point 是故障注入点名（"" 表示不注入）。
func (q *Queue) appendRecord(typ uint32, payload []byte, point string) error {
	buf := encodeRecord(typ, payload)
	if n := faultPartialLen(point, len(buf)); n >= 0 {
		// 半写注入：写入前 n 字节后模拟强杀。
		q.file.Write(buf[:n])
		faultKill()
	}
	if _, err := q.file.Write(buf); err != nil {
		return err
	}
	faultCheck(point + "-written")
	if err := q.file.Sync(); err != nil {
		return err
	}
	faultCheck(point + "-synced")
	return nil
}

// scanLog 顺序扫描日志，对每条完整记录调用 apply。
// 返回有效数据长度；首个不完整或校验失败的记录之后的字节视为半写尾部。
func scanLog(f *os.File, apply func(typ uint32, payload []byte) error) (int64, error) {
	var off int64
	hdr := make([]byte, recordHeaderLen)
	for {
		if _, err := f.ReadAt(hdr, off); err != nil {
			if err == io.EOF {
				return off, nil // 干净的结束
			}
			if err == io.ErrUnexpectedEOF {
				return off, nil // 半写头部
			}
			return 0, err
		}
		typ := binary.LittleEndian.Uint32(hdr[0:4])
		plen := binary.LittleEndian.Uint32(hdr[4:8])
		crc := binary.LittleEndian.Uint32(hdr[8:12])
		if plen > maxPayload {
			return off, nil // 损坏，按半写尾部处理
		}
		payload := make([]byte, plen)
		if _, err := f.ReadAt(payload, off+recordHeaderLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return off, nil // 半写正文
			}
			return 0, err
		}
		c := crc32.ChecksumIEEE(hdr[0:8])
		c = crc32.Update(c, crc32.IEEETable, payload)
		if c != crc {
			return off, nil // 校验失败，按半写尾部处理
		}
		if err := apply(typ, payload); err != nil {
			return 0, err
		}
		off += recordHeaderLen + int64(plen)
	}
}

// applyLog 打开日志文件、扫描应用记录并截断半写尾部，然后以追加模式返回文件。
func applyLog(path string, apply func(typ uint32, payload []byte) error) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	valid, err := scanLog(f, apply)
	if err != nil {
		f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() > valid {
		// 截断半写尾部，避免遮蔽后续提交。
		if err := f.Truncate(valid); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// --- 记录 payload 编解码 ---

func encodePublish(firstID uint64, batch [][]byte) []byte {
	n := 12
	for _, b := range batch {
		n += 4 + len(b)
	}
	p := make([]byte, 0, n)
	p = binary.LittleEndian.AppendUint64(p, firstID)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(batch)))
	for _, b := range batch {
		p = binary.LittleEndian.AppendUint32(p, uint32(len(b)))
		p = append(p, b...)
	}
	return p
}

func decodePublish(p []byte) (firstID uint64, batch [][]byte, err error) {
	if len(p) < 12 {
		return 0, nil, errCorrupt
	}
	firstID = binary.LittleEndian.Uint64(p[0:8])
	count := binary.LittleEndian.Uint32(p[8:12])
	p = p[12:]
	batch = make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(p) < 4 {
			return 0, nil, errCorrupt
		}
		l := binary.LittleEndian.Uint32(p[0:4])
		p = p[4:]
		if uint64(l) > uint64(len(p)) {
			return 0, nil, errCorrupt
		}
		batch = append(batch, p[:l])
		p = p[l:]
	}
	return firstID, batch, nil
}

func encodeAck(group int, ids []uint64) []byte {
	p := make([]byte, 0, 8+8*len(ids))
	p = binary.LittleEndian.AppendUint32(p, uint32(group))
	p = binary.LittleEndian.AppendUint32(p, uint32(len(ids)))
	for _, id := range ids {
		p = binary.LittleEndian.AppendUint64(p, id)
	}
	return p
}

func decodeAck(p []byte) (group int, ids []uint64, err error) {
	if len(p) < 8 {
		return 0, nil, errCorrupt
	}
	group = int(binary.LittleEndian.Uint32(p[0:4]))
	count := binary.LittleEndian.Uint32(p[4:8])
	p = p[8:]
	if uint64(count)*8 != uint64(len(p)) {
		return 0, nil, errCorrupt
	}
	ids = make([]uint64, 0, count)
	for len(p) >= 8 {
		ids = append(ids, binary.LittleEndian.Uint64(p[0:8]))
		p = p[8:]
	}
	return group, ids, nil
}

func encodeSnapshot(nextID uint64, order []uint64, live map[uint64]*entry) []byte {
	n := 12
	for _, id := range order {
		n += 13 + len(live[id].body)
	}
	p := make([]byte, 0, n)
	p = binary.LittleEndian.AppendUint64(p, nextID)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(order)))
	for _, id := range order {
		e := live[id]
		p = binary.LittleEndian.AppendUint64(p, id)
		p = append(p, e.pend)
		p = binary.LittleEndian.AppendUint32(p, uint32(len(e.body)))
		p = append(p, e.body...)
	}
	return p
}

func decodeSnapshot(p []byte) (nextID uint64, ids []uint64, masks []uint8, bodies [][]byte, err error) {
	if len(p) < 12 {
		return 0, nil, nil, nil, errCorrupt
	}
	nextID = binary.LittleEndian.Uint64(p[0:8])
	count := binary.LittleEndian.Uint32(p[8:12])
	p = p[12:]
	for i := uint32(0); i < count; i++ {
		if len(p) < 13 {
			return 0, nil, nil, nil, errCorrupt
		}
		id := binary.LittleEndian.Uint64(p[0:8])
		mask := p[8]
		l := binary.LittleEndian.Uint32(p[9:13])
		p = p[13:]
		if uint64(l) > uint64(len(p)) {
			return 0, nil, nil, nil, errCorrupt
		}
		ids = append(ids, id)
		masks = append(masks, mask)
		bodies = append(bodies, p[:l])
		p = p[l:]
	}
	return nextID, ids, masks, bodies, nil
}

var _ = fmt.Sprintf
