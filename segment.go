package log

import (
	"bytes"
	"encoding/binary"
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
// The trailing checksum makes truncation and tampering detectable as
// ErrCorruptSegment when the segment is parsed.
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

// parseSegment decodes every batch entry in data. Any truncation,
// unknown content, or checksum mismatch yields ErrCorruptSegment.
func parseSegment(data []byte) ([]parsedBatch, error) {
	var batches []parsedBatch
	off := 0
	for off < len(data) {
		if len(data)-off < entryHeaderLen {
			return nil, ErrCorruptSegment
		}
		if !bytes.Equal(data[off:off+4], segmentMagic) {
			return nil, ErrCorruptSegment
		}
		seq := binary.LittleEndian.Uint64(data[off+4 : off+12])
		count := binary.LittleEndian.Uint32(data[off+12 : off+16])
		pos := off + entryHeaderLen
		records := make([][]byte, 0, min(int(count), 1<<20))
		for i := uint32(0); i < count; i++ {
			if len(data)-pos < 4 {
				return nil, ErrCorruptSegment
			}
			n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if n < 0 || len(data)-pos < n {
				return nil, ErrCorruptSegment
			}
			rec := make([]byte, n)
			copy(rec, data[pos:pos+n])
			pos += n
			records = append(records, rec)
		}
		if len(data)-pos < 4 {
			return nil, ErrCorruptSegment
		}
		crc := binary.LittleEndian.Uint32(data[pos : pos+4])
		if crc32.ChecksumIEEE(data[off:pos]) != crc {
			return nil, ErrCorruptSegment
		}
		pos += 4
		batches = append(batches, parsedBatch{seq: seq, records: records})
		off = pos
	}
	return batches, nil
}
