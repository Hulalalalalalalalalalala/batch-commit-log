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
// A group commit frames several entries so the group stays atomic
// across a crash:
//
//	magic   4 bytes  "BCLG"
//	count   4 bytes  number of batches in the group
//	seqs    count × 8 bytes  reserved sequence of each batch
//	crc32   4 bytes  IEEE checksum of the header before it
//	entries count × batch entries as above
//
// The whole frame is written with a single Write. A crash can only
// leave a prefix of it behind, and recovery treats an incomplete frame
// as a torn tail starting at the frame — so a crashed group leaves
// either every batch or none of them behind, never a subset.
//
// The trailing checksums make tampering detectable as ErrCorruptSegment
// when the segment is parsed. A torn write left by a crash is instead
// reported as a tail: a strict prefix of a valid entry or group frame,
// never a full one with a bad checksum.
var segmentMagic = []byte{'B', 'C', 'L', '1'}
var groupMagic = []byte{'B', 'C', 'L', 'G'}

const entryHeaderLen = 4 + 8 + 4

// groupHeaderSize is the encoded size of a group frame header for a
// group of count batches: magic + count + count sequences + checksum.
func groupHeaderSize(count int) int { return 4 + 4 + 8*count + 4 }

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

// encodeGroup frames already-encoded batch entries as one group: a
// header naming every reserved sequence in entry order, then the
// entries themselves.
func encodeGroup(seqs []uint64, entries [][]byte) []byte {
	size := groupHeaderSize(len(seqs))
	for _, e := range entries {
		size += len(e)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, groupMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(seqs)))
	buf = append(buf, tmp[:4]...)
	for _, seq := range seqs {
		binary.LittleEndian.PutUint64(tmp[:], seq)
		buf = append(buf, tmp[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	buf = append(buf, tmp[:4]...)
	for _, e := range entries {
		buf = append(buf, e...)
	}
	return buf
}

type parsedBatch struct {
	seq     uint64
	records [][]byte
	off     int // offset of the entry within its segment
	length  int // encoded entry length in bytes
}

// segmentTail describes an incomplete entry or group frame at the end
// of a segment: the remnant of a crash in the middle of a write.
type segmentTail struct {
	off  int      // offset where the incomplete entry or frame starts
	seqs []uint64 // sequences reserved by the crashed write, when recoverable
}

// parseSegment decodes every complete batch entry in data, whether
// written singly or inside a group frame. A truncated tail — trailing
// bytes that are only a prefix of an entry or frame — is reported
// separately as the signature of a crash mid-write. Any other
// inconsistency (unknown magic, bad checksum) yields ErrCorruptSegment.
func parseSegment(data []byte) ([]parsedBatch, *segmentTail, error) {
	var batches []parsedBatch
	off := 0
	for off < len(data) {
		rest := data[off:]
		if len(rest) < len(segmentMagic) {
			// A partial magic is a torn write only if it is a
			// prefix of a valid one; both magics share "BCL".
			if !bytes.Equal(rest, segmentMagic[:len(rest)]) {
				return nil, nil, ErrCorruptSegment
			}
			return batches, &segmentTail{off: off}, nil
		}
		switch {
		case bytes.Equal(rest[:4], segmentMagic):
			pb, next, tail, err := parseEntry(data, off)
			if err != nil {
				return nil, nil, err
			}
			if tail != nil {
				return batches, tail, nil
			}
			batches = append(batches, pb)
			off = next
		case bytes.Equal(rest[:4], groupMagic):
			group, next, tail, err := parseGroup(data, off)
			if err != nil {
				return nil, nil, err
			}
			if tail != nil {
				return batches, tail, nil
			}
			batches = append(batches, group...)
			off = next
		default:
			return nil, nil, ErrCorruptSegment
		}
	}
	return batches, nil, nil
}

// parseEntry decodes the single batch entry starting at off; the magic
// must already be verified by the caller. A truncated entry — the data
// ends inside what would otherwise be a valid entry — is reported as a
// torn tail; a bad checksum is ErrCorruptSegment.
func parseEntry(data []byte, off int) (parsedBatch, int, *segmentTail, error) {
	rest := data[off:]
	if len(rest) < entryHeaderLen {
		tail := &segmentTail{off: off}
		if len(rest) >= 12 {
			tail.seqs = []uint64{binary.LittleEndian.Uint64(rest[4:12])}
		}
		return parsedBatch{}, 0, tail, nil
	}
	seq := binary.LittleEndian.Uint64(rest[4:12])
	count := binary.LittleEndian.Uint32(rest[12:16])
	tail := &segmentTail{off: off, seqs: []uint64{seq}}
	pos := off + entryHeaderLen
	records := make([][]byte, 0, min(int(count), 1<<20))
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 4 {
			return parsedBatch{}, 0, tail, nil
		}
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if n < 0 {
			return parsedBatch{}, 0, nil, ErrCorruptSegment
		}
		if len(data)-pos < n {
			return parsedBatch{}, 0, tail, nil
		}
		rec := make([]byte, n)
		copy(rec, data[pos:pos+n])
		pos += n
		records = append(records, rec)
	}
	if len(data)-pos < 4 {
		return parsedBatch{}, 0, tail, nil
	}
	crc := binary.LittleEndian.Uint32(data[pos : pos+4])
	if crc32.ChecksumIEEE(data[off:pos]) != crc {
		return parsedBatch{}, 0, nil, ErrCorruptSegment
	}
	pos += 4
	return parsedBatch{seq: seq, records: records, off: off, length: pos - off}, pos, nil, nil
}

// parseGroup decodes the group frame starting at off; the magic must
// already be verified by the caller. The frame is complete only when
// the header and every declared entry follow; anything less is a torn
// tail starting at the frame, so a crashed group commit leaves none of
// its batches behind. A complete frame whose contents fail validation
// is ErrCorruptSegment.
func parseGroup(data []byte, off int) ([]parsedBatch, int, *segmentTail, error) {
	rest := data[off:]
	if len(rest) < 8 {
		return nil, 0, &segmentTail{off: off}, nil
	}
	count := binary.LittleEndian.Uint32(rest[4:8])
	headerLen := uint64(groupHeaderSize(0)) + 8*uint64(count)
	if uint64(len(rest)) < headerLen {
		// Partial header: recover whichever whole sequences fit.
		var seqs []uint64
		for p := 8; p+8 <= len(rest); p += 8 {
			seqs = append(seqs, binary.LittleEndian.Uint64(rest[p:p+8]))
		}
		return nil, 0, &segmentTail{off: off, seqs: seqs}, nil
	}
	hl := int(headerLen)
	if crc32.ChecksumIEEE(rest[:hl-4]) != binary.LittleEndian.Uint32(rest[hl-4:hl]) {
		return nil, 0, nil, ErrCorruptSegment
	}
	seqs := make([]uint64, count)
	for i := range seqs {
		seqs[i] = binary.LittleEndian.Uint64(rest[8+8*i : 16+8*i])
	}
	// From here the header is intact, so every reserved sequence is
	// known; a frame that stops early is torn with all of them.
	tail := &segmentTail{off: off, seqs: seqs}
	batches := make([]parsedBatch, 0, min(int(count), 1<<20))
	pos := off + hl
	for i := uint32(0); i < count; i++ {
		remain := len(data) - pos
		if remain < len(segmentMagic) {
			if remain == 0 || bytes.Equal(data[pos:], segmentMagic[:remain]) {
				return nil, 0, tail, nil
			}
			return nil, 0, nil, ErrCorruptSegment
		}
		if !bytes.Equal(data[pos:pos+4], segmentMagic) {
			return nil, 0, nil, ErrCorruptSegment
		}
		pb, next, etail, err := parseEntry(data, pos)
		if err != nil {
			return nil, 0, nil, err
		}
		if etail != nil {
			return nil, 0, tail, nil
		}
		if pb.seq != seqs[i] {
			return nil, 0, nil, ErrCorruptSegment
		}
		batches = append(batches, pb)
		pos = next
	}
	return batches, pos, nil, nil
}
