package fifoq

// snapshot.go: compaction snapshot (snap.dat) write and load.
//
// snap.dat layout:
//   magic(4) "FQSN" | version(4) | horizon(8) | nextID(8) | count(4)
//   per message: id(8) | ackMask(1) | bodyLen(4) | body
//   crc32(4) covering everything before it
//
// horizon is the virtual log offset the snapshot reflects; log records
// below it must not be replayed. ackMask bit i = acknowledged by
// group i. Only messages not acknowledged by all groups are stored.

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	snapMagic   = "FQSN"
	snapVersion = 1
)

func (q *Queue) buildSnapshot(horizon uint64) []byte {
	var buf []byte
	hdr := make([]byte, 24)
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], horizon)
	binary.LittleEndian.PutUint64(hdr[16:24], q.nextID)
	buf = append(buf, hdr...)

	var cnt [4]byte
	cntPos := len(buf)
	buf = append(buf, cnt[:]...)
	n := 0
	var tmp [13]byte
	for _, id := range q.order {
		m := q.msgs[id]
		if m.ackedByAll() {
			continue
		}
		var mask byte
		for g := 0; g < NumGroups; g++ {
			if m.ack[g] {
				mask |= 1 << uint(g)
			}
		}
		binary.LittleEndian.PutUint64(tmp[0:8], id)
		tmp[8] = mask
		binary.LittleEndian.PutUint32(tmp[9:13], uint32(len(m.body)))
		buf = append(buf, tmp[:]...)
		buf = append(buf, m.body...)
		n++
	}
	binary.LittleEndian.PutUint32(buf[cntPos:cntPos+4], uint32(n))
	crc := crc32.ChecksumIEEE(buf)
	var c [4]byte
	binary.LittleEndian.PutUint32(c[:], crc)
	buf = append(buf, c[:]...)
	return buf
}

// Compact physically reclaims messages acknowledged by all groups and
// rewrites the directory to hold only live data. It never changes the
// logical result of any subsequent Read.
func (q *Queue) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkOpen(); err != nil {
		return err
	}

	horizon := q.logBase + q.logLen
	data := q.buildSnapshot(horizon)

	// 1. Write the new snapshot to a temp file and fsync it.
	tmp := filepath.Join(q.dir, "snap.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
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
	q.writeBytes += int64(len(data))
	debugCrash("compact.tmp")

	// 2. Atomically replace snap.dat, then fsync the directory.
	final := filepath.Join(q.dir, "snap.dat")
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if err := syncDir(q.dir); err != nil {
		return err
	}
	debugCrash("compact.renamed")

	// 3. Reset the log: truncate, then write a fresh header at the new
	// horizon. The snapshot is already durable, so a crash here only
	// loses redundant log records.
	if err := q.log.Truncate(0); err != nil {
		return err
	}
	if err := q.log.Sync(); err != nil {
		return err
	}
	debugCrash("compact.truncated")
	if err := writeLogHeader(q.log, horizon); err != nil {
		return err
	}
	if err := q.log.Sync(); err != nil {
		return err
	}
	q.writeBytes += logHeaderSize

	// 4. Update in-memory state: drop fully-acknowledged messages.
	q.logBase = horizon
	q.logLen = 0
	kept := q.order[:0]
	for _, id := range q.order {
		m := q.msgs[id]
		if m.ackedByAll() {
			delete(q.msgs, id)
			continue
		}
		kept = append(kept, id)
	}
	q.order = kept
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// loadSnapshot loads snap.dat and returns its log horizon.
func (q *Queue) loadSnapshot(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) < 28+4 {
		return 0, errCorrupt
	}
	body, crcBytes := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(crcBytes) {
		return 0, errCorrupt
	}
	if string(body[0:4]) != snapMagic || binary.LittleEndian.Uint32(body[4:8]) != snapVersion {
		return 0, errCorrupt
	}
	horizon := binary.LittleEndian.Uint64(body[8:16])
	q.nextID = binary.LittleEndian.Uint64(body[16:24])
	n := int(binary.LittleEndian.Uint32(body[24:28]))
	p := body[28:]
	for i := 0; i < n; i++ {
		if len(p) < 13 {
			return 0, errCorrupt
		}
		id := binary.LittleEndian.Uint64(p[0:8])
		mask := p[8]
		blen := int(binary.LittleEndian.Uint32(p[9:13]))
		p = p[13:]
		if len(p) < blen {
			return 0, errCorrupt
		}
		b := make([]byte, blen)
		copy(b, p[:blen])
		p = p[blen:]
		m := &msgEnt{body: b}
		for g := 0; g < NumGroups; g++ {
			m.ack[g] = mask&(1<<uint(g)) != 0
		}
		q.msgs[id] = m
		q.order = append(q.order, id)
	}
	return horizon, nil
}
