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
// The trailing checksum makes tampering detectable as ErrCorruptSegment
// when the segment is parsed. A torn write left by a crash is instead
// reported as a tail: a strict prefix of a valid entry, never a full
// entry with a bad checksum.
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

// segmentTail describes an incomplete entry at the end of a segment: the
// remnant of a crash in the middle of a write.
type segmentTail struct {
	off    int    // offset where the incomplete entry starts
	seq    uint64 // sequence reserved for the crashed batch
	hasSeq bool   // whether the partial header held a full sequence
}

// parseSegment decodes every complete batch entry in data. A truncated
// tail — trailing bytes that are only a prefix of an entry — is reported
// separately as the signature of a crash mid-write. Any other
// inconsistency (unknown magic, bad checksum) yields ErrCorruptSegment.
func parseSegment(data []byte) ([]parsedBatch, *segmentTail, error) {
	var batches []parsedBatch
	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < entryHeaderLen {
			// A partial header is a torn write only if it is a
			// prefix of a valid header.
			n := min(len(rest), len(segmentMagic))
			if !bytes.Equal(rest[:n], segmentMagic[:n]) {
				return nil, nil, ErrCorruptSegment
			}
			tail := &segmentTail{off: off}
			if len(rest) >= 12 {
				tail.seq = binary.LittleEndian.Uint64(rest[4:12])
				tail.hasSeq = true
			}
			return batches, tail, nil
		}
		if !bytes.Equal(rest[:4], segmentMagic) {
			return nil, nil, ErrCorruptSegment
		}
		seq := binary.LittleEndian.Uint64(rest[4:12])
		count := binary.LittleEndian.Uint32(rest[12:16])
		tail := &segmentTail{off: off, seq: seq, hasSeq: true}
		pos := off + entryHeaderLen
		records := make([][]byte, 0, min(int(count), 1<<20))
		for i := uint32(0); i < count; i++ {
			if len(data)-pos < 4 {
				return batches, tail, nil
			}
			n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if n < 0 {
				return nil, nil, ErrCorruptSegment
			}
			if len(data)-pos < n {
				return batches, tail, nil
			}
			rec := make([]byte, n)
			copy(rec, data[pos:pos+n])
			pos += n
			records = append(records, rec)
		}
		if len(data)-pos < 4 {
			return batches, tail, nil
		}
		crc := binary.LittleEndian.Uint32(data[pos : pos+4])
		if crc32.ChecksumIEEE(data[off:pos]) != crc {
			return nil, nil, ErrCorruptSegment
		}
		pos += 4
		batches = append(batches, parsedBatch{seq: seq, records: records})
		off = pos
	}
	return batches, nil, nil
}
