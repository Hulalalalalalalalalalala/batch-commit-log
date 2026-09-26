package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// On-disk batch entry layout, all integers little-endian:
//
//	magic   4 bytes  "BCL1"
//	seq     8 bytes  batch sequence number
//	count   4 bytes  number of records
//	records count × (4-byte length + opaque bytes)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// The trailing checksum makes tampering detectable as ErrCorruptSegment
// when the segment is parsed. A structurally incomplete final entry in
// the newest segment (a crash tail) is reported separately via
// errTrailingEntry so Open can discard it instead of failing.
var segmentMagic = []byte{'B', 'C', 'L', '1'}

const entryHeaderLen = 4 + 8 + 4

func encodeBatch(seq uint64, records [][]byte) []byte {
	size := entryHeaderLen + 4
	for _, r := range records {
		size += 4 + len(r)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, segmentMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(records)))
	buf = append(buf, tmp[:4]...)
	for _, r := range records {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

type parsedBatch struct {
	seq     uint64
	records [][]byte
}

// errTrailingEntry marks a structurally incomplete entry at the very end
// of data: a crash tail, never committed. It is only survivable in the
// newest segment; an earlier segment ending like this is corrupt.
var errTrailingEntry = errors.New("trailing incomplete entry")

// parseSegment decodes every complete batch entry in data. It returns the
// parsed batches and the number of bytes consumed by complete entries.
//
// A structurally truncated final entry (missing header bytes, record
// payload, or the trailing checksum position) yields errTrailingEntry.
// Any other problem — bad magic anywhere, a checksum mismatch on a
// fully framed entry, or a length field overrunning an earlier segment —
// yields ErrCorruptSegment.
func parseSegment(data []byte) (batches []parsedBatch, consumed int, err error) {
	off := 0
	for off < len(data) {
		remaining := len(data) - off
		if remaining < entryHeaderLen {
			// A torn header is a crash tail only when the bytes are a
			// prefix of the entry magic; other bytes are garbage.
			if !bytes.HasPrefix(data[off:], segmentMagic) {
				return nil, 0, ErrCorruptSegment
			}
			return batches, off, errTrailingEntry
		}
		if !bytes.Equal(data[off:off+4], segmentMagic) {
			return nil, 0, ErrCorruptSegment
		}
		seq := binary.LittleEndian.Uint64(data[off+4 : off+12])
		count := binary.LittleEndian.Uint32(data[off+12 : off+16])
		pos := off + entryHeaderLen
		records := make([][]byte, 0, min(int(count), 1<<20))
		for i := uint32(0); i < count; i++ {
			if len(data)-pos < 4 {
				return batches, off, errTrailingEntry
			}
			n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if n < 0 || len(data)-pos < n {
				return batches, off, errTrailingEntry
			}
			rec := make([]byte, n)
			copy(rec, data[pos:pos+n])
			pos += n
			records = append(records, rec)
		}
		if len(data)-pos < 4 {
			return batches, off, errTrailingEntry
		}
		crc := binary.LittleEndian.Uint32(data[pos : pos+4])
		if crc32.ChecksumIEEE(data[off:pos]) != crc {
			return nil, 0, ErrCorruptSegment
		}
		pos += 4
		batches = append(batches, parsedBatch{seq: seq, records: records})
		off = pos
	}
	return batches, off, nil
}
