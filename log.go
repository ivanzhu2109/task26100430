package fifoq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// data.log layout:
//   header: magic(4) "FQLG" | version(4) | base(8)
//   records...
//
// record layout:
//   magic(4) "FQRC" | type(1) | payloadLen(4) | crc32(4) | payload
//
// crc covers type byte and payload. A record is committed only when it
// is fully written and fsynced; a torn tail is truncated on recovery.
//
// publish payload: count(4) then per message len(4)+body.
// ack payload:     group(1) | count(4) | id(8)*count.

const (
	logMagic   = "FQLG"
	recMagic   = "FQRC"
	logVersion = 1

	logHeaderSize = 16
	recHeaderSize = 13

	recPublish = 1
	recAck     = 2
)

var errCorrupt = errors.New("fifoq: corrupt log")

// writeLogHeader writes a fresh log header at offset 0 of an empty log.
func writeLogHeader(f *os.File, base uint64) error {
	buf := make([]byte, logHeaderSize)
	copy(buf[0:4], logMagic)
	binary.LittleEndian.PutUint32(buf[4:8], logVersion)
	binary.LittleEndian.PutUint64(buf[8:16], base)
	if _, err := f.WriteAt(buf, 0); err != nil {
		return err
	}
	return nil
}

// encodeRecord builds a full record buffer.
func encodeRecord(typ byte, payload []byte) []byte {
	buf := make([]byte, recHeaderSize+len(payload))
	copy(buf[0:4], recMagic)
	buf[4] = typ
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(payload)))
	crc := crc32.NewIEEE()
	crc.Write(buf[4:5])
	crc.Write(payload)
	binary.LittleEndian.PutUint32(buf[9:13], crc.Sum32())
	copy(buf[recHeaderSize:], payload)
	return buf
}

func encodePublishPayload(batch [][]byte) []byte {
	n := 4
	for _, m := range batch {
		n += 4 + len(m)
	}
	buf := make([]byte, 0, n)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(batch)))
	buf = append(buf, tmp[:]...)
	for _, m := range batch {
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(m)))
		buf = append(buf, tmp[:]...)
		buf = append(buf, m...)
	}
	return buf
}

func encodeAckPayload(group int, ids []uint64) []byte {
	buf := make([]byte, 5+8*len(ids))
	buf[0] = byte(group)
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buf[5+8*i:], id)
	}
	return buf
}

// appendRecord writes one record at the current end of the log and
// fsyncs, making the batch durable and atomic. It returns the new log
// data length. crashPoint, when non-empty, is a test hook.
func (q *Queue) appendRecord(rec []byte) error {
	off := int64(logHeaderSize + q.logLen)
	if debugSplitWrite > 0 && debugSplitWrite < len(rec) {
		// Test-only path: persist a partial record, then crash.
		if _, err := q.log.WriteAt(rec[:debugSplitWrite], off); err != nil {
			return err
		}
		q.log.Sync()
		debugCrash("midwrite")
		if _, err := q.log.WriteAt(rec[debugSplitWrite:], off+int64(debugSplitWrite)); err != nil {
			return err
		}
	} else {
		if _, err := q.log.WriteAt(rec, off); err != nil {
			return err
		}
	}
	q.writeBytes += int64(len(rec))
	if err := q.log.Sync(); err != nil {
		return err
	}
	q.logLen += uint64(len(rec))
	return nil
}

// replayLog reads records from f (positioned after a validated header)
// and applies them via apply. Torn or corrupt tails are truncated so
// later appends start at a clean boundary. base is the virtual offset
// of the first record; records with virtual offset < skipBelow are
// already reflected in the snapshot and are skipped.
func (q *Queue) replayLog(f *os.File, base, skipBelow uint64, apply func(typ byte, payload []byte) error) error {
	off := uint64(logHeaderSize)
	size, err := fileSize(f)
	if err != nil {
		return err
	}
	hdr := make([]byte, recHeaderSize)
	for {
		virt := base + (off - logHeaderSize)
		if off+recHeaderSize > size {
			break // torn or absent record header
		}
		if _, err := f.ReadAt(hdr, int64(off)); err != nil {
			return err
		}
		if string(hdr[0:4]) != recMagic {
			break // garbage: torn write
		}
		plen := binary.LittleEndian.Uint32(hdr[5:9])
		if plen > 1<<31 {
			break
		}
		if off+recHeaderSize+uint64(plen) > size {
			break // torn payload
		}
		payload := make([]byte, plen)
		if _, err := f.ReadAt(payload, int64(off+recHeaderSize)); err != nil {
			return err
		}
		crc := crc32.NewIEEE()
		crc.Write(hdr[4:5])
		crc.Write(payload)
		if crc.Sum32() != binary.LittleEndian.Uint32(hdr[9:13]) {
			break // torn/corrupt record
		}
		if virt >= skipBelow {
			if err := apply(hdr[4], payload); err != nil {
				return err
			}
		}
		off += recHeaderSize + uint64(plen)
	}
	// Truncate any incomplete tail so future appends are not shadowed.
	if off < size {
		if err := f.Truncate(int64(off)); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	q.logLen = off - logHeaderSize
	return nil
}

func fileSize(f *os.File) (uint64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

// readLogHeader validates the log header and returns its base offset.
// ok=false means the log is missing, empty, or has a torn header and
// should be reinitialized.
func readLogHeader(f *os.File) (base uint64, ok bool, err error) {
	size, err := fileSize(f)
	if err != nil {
		return 0, false, err
	}
	if size < logHeaderSize {
		return 0, false, nil
	}
	buf := make([]byte, logHeaderSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return 0, false, err
	}
	if string(buf[0:4]) != logMagic || binary.LittleEndian.Uint32(buf[4:8]) != logVersion {
		return 0, false, nil
	}
	return binary.LittleEndian.Uint64(buf[8:16]), true, nil
}

var errBadGroup = fmt.Errorf("fifoq: group out of range [0,%d)", NumGroups)
