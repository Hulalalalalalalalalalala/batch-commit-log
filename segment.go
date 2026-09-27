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
// when the segment is parsed. A crash can instead leave a half-written
// final entry in the last segment; that torn tail is discarded on open.
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

// parsedSegment is the result of decoding one segment file.
type parsedSegment struct {
	batches []parsedBatch // complete, checksum-verified entries
	// A torn tail is the half-written final entry a crash can leave
	// behind in the last segment. It is ignored, never replayed.
	torn       bool   // a torn tail was found at goodLen
	tornSeq    uint64 // sequence recovered from the torn entry header
	hasTornSeq bool   // the torn header was complete enough to read its seq
	goodLen    int    // length of the well-formed prefix of the file
}

// parseSegment decodes every complete batch entry in data. A half-written
// tail entry is tolerated only when allowTornTail is true, i.e. for the
// last segment file, where a crash can leave one behind. Anything else
// that does not parse cleanly — bad magic, bad checksum, or any defect in
// an earlier segment — is tampering and yields ErrCorruptSegment.
func parseSegment(data []byte, allowTornTail bool) (parsedSegment, error) {
	var ps parsedSegment
	off := 0
	for off < len(data) {
		remaining := len(data) - off
		if remaining < 4 || !bytes.Equal(data[off:off+4], segmentMagic) {
			// A torn write can stop inside the magic itself, leaving a
			// prefix of it; anything else is not a write this log made.
			if allowTornTail && remaining < 4 && bytes.Equal(data[off:], segmentMagic[:remaining]) {
				ps.torn = true
				break
			}
			return ps, ErrCorruptSegment
		}
		if remaining < entryHeaderLen {
			if !allowTornTail {
				return ps, ErrCorruptSegment
			}
			ps.torn = true
			if remaining >= 12 {
				ps.tornSeq = binary.LittleEndian.Uint64(data[off+4 : off+12])
				ps.hasTornSeq = true
			}
			break
		}
		seq := binary.LittleEndian.Uint64(data[off+4 : off+12])
		count := binary.LittleEndian.Uint32(data[off+12 : off+16])
		pos := off + entryHeaderLen
		records := make([][]byte, 0, min(int(count), 1<<20))
		torn := false
		for i := uint32(0); i < count && !torn; i++ {
			if len(data)-pos < 4 {
				torn = true
				break
			}
			n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if n < 0 || len(data)-pos < n {
				torn = true
				break
			}
			rec := make([]byte, n)
			copy(rec, data[pos:pos+n])
			pos += n
			records = append(records, rec)
		}
		if !torn && len(data)-pos < 4 {
			torn = true // checksum missing or cut short
		}
		if torn {
			if !allowTornTail {
				return ps, ErrCorruptSegment
			}
			ps.torn = true
			ps.tornSeq = seq
			ps.hasTornSeq = true
			break
		}
		crc := binary.LittleEndian.Uint32(data[pos : pos+4])
		if crc32.ChecksumIEEE(data[off:pos]) != crc {
			return ps, ErrCorruptSegment
		}
		pos += 4
		ps.batches = append(ps.batches, parsedBatch{seq: seq, records: records})
		off = pos
	}
	ps.goodLen = off
	return ps, nil
}
