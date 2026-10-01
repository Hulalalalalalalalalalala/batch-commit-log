package log

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"unicode/utf8"
)

// On-disk entry layouts, all integers little-endian. A segment file is a
// sequence of entries of three kinds:
//
// Single batch, written by Commit ("BCL1"):
//
//	magic   4 bytes  "BCL1"
//	seq     8 bytes  batch sequence number
//	count   4 bytes  number of records
//	records count × (4-byte length + opaque bytes)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Batch group, written by CommitGroup as one atomic unit ("BCLG"):
//
//	magic   4 bytes  "BCLG"
//	count   4 bytes  number of batches in the group
//	batches count × (8-byte seq + 4-byte record count + records)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Keyed single batch, written by Commit for a batch from AppendIdempotent
// ("BCLI"): identical to BCL1 with one extra field — a length-prefixed
// idempotency key between the sequence and the record count:
//
//	magic   4 bytes  "BCLI"
//	seq     8 bytes  batch sequence number
//	keyLen  2 bytes  length of the idempotency key
//	key     keyLen bytes
//	count   4 bytes  number of records
//	records count × (4-byte length + opaque bytes)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// The indexed body starts at seq (right after the magic), so it reads
// seq + keyLen + key + count + records.
//
// Keyed batch group, written by CommitGroup when at least one member was
// staged with AppendIdempotent ("BCLK"). Each member is the BCLG member
// layout with a length-prefixed key inserted right after its seq; key
// length 0 marks an anonymous member:
//
//	magic   4 bytes  "BCLK"
//	count   4 bytes  number of batches in the group
//	batches count × (8-byte seq + 2-byte key length + key
//	                + 4-byte record count + records)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Hole marker, written by crash recovery to make the sequences reserved
// by a torn tail permanent ("BCLH"):
//
//	magic   4 bytes  "BCLH"
//	count   4 bytes  number of reserved sequences
//	seqs    count × 8 bytes
//	crc32   4 bytes  IEEE checksum of everything before it
//
// The trailing checksum makes tampering detectable as ErrCorruptSegment
// when the segment is parsed. A torn write left by a crash is instead
// reported as a tail: a strict prefix of a valid entry, never a full
// entry with a bad checksum. A group entry is durable only as a whole —
// a truncated group is a tail and none of its batches become visible.
// The keyed encodings exist only for keyed batches: anonymous traffic
// keeps using BCL1/BCLG byte for byte, and recovery never invents a key
// for an anonymous batch.
var (
	segmentMagic = []byte{'B', 'C', 'L', '1'}
	groupMagic   = []byte{'B', 'C', 'L', 'G'}
	holeMagic    = []byte{'B', 'C', 'L', 'H'}
	keyedMagic   = []byte{'B', 'C', 'L', 'I'}
	keyedGroup   = []byte{'B', 'C', 'L', 'K'}
)

const (
	entryHeaderLen = 4 + 8 + 4
	// maxBatchIDLen bounds an idempotency key on disk; AppendIdempotent
	// rejects longer keys before they reach the encoder.
	maxBatchIDLen = 256
)

// groupMember is one staged batch inside a group commit. A non-empty id
// marks a member staged with AppendIdempotent.
type groupMember struct {
	seq     uint64
	id      string
	records [][]byte
}

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
	appendRecords(&buf, &tmp, records)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// encodeKeyedBatch encodes a single batch carrying an idempotency key
// (BCLI). The layout matches encodeBatch apart from the length-prefixed
// key between the sequence and the record count.
func encodeKeyedBatch(seq uint64, id string, records [][]byte) []byte {
	size := 4 + 8 + 2 + len(id) + 4 + 4
	for _, r := range records {
		size += 4 + len(r)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, keyedMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len(id)))
	buf = append(buf, tmp[:2]...)
	buf = append(buf, id...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(records)))
	buf = append(buf, tmp[:4]...)
	appendRecords(&buf, &tmp, records)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// appendRecords appends count records as (4-byte length + opaque bytes).
func appendRecords(buf *[]byte, tmp *[8]byte, records [][]byte) {
	for _, r := range records {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r)))
		*buf = append(*buf, tmp[:4]...)
		*buf = append(*buf, r...)
	}
}

// encodeGroup encodes a batch group as a single entry. It uses BCLK and
// key-framed members whenever at least one member carries a key, so a
// mixed anonymous/keyed group is one atomic entry; a fully anonymous
// group keeps the historical BCLG encoding byte for byte. It also
// returns, for each member, the offset and length of its decodable body
// relative to the start of the entry, so the index can point straight at
// one member of the group. A keyed member's body begins at its key
// length prefix; an anonymous BCLG member's body begins at its seq.
func encodeGroup(members []groupMember) (entry []byte, offs []int, lens []int) {
	for _, m := range members {
		if m.id != "" {
			return encodeKeyedGroup(members)
		}
	}
	return encodeAnonGroup(members)
}

// encodeAnonGroup encodes a fully anonymous group with the historical
// BCLG layout; each member body is seq + record count + records.
func encodeAnonGroup(members []groupMember) (entry []byte, offs []int, lens []int) {
	size := 4 + 4 + 4
	for _, m := range members {
		size += 8 + 4
		for _, r := range m.records {
			size += 4 + len(r)
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, groupMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(members)))
	buf = append(buf, tmp[:4]...)
	offs = make([]int, len(members))
	lens = make([]int, len(members))
	for i, m := range members {
		offs[i] = len(buf)
		binary.LittleEndian.PutUint64(tmp[:], m.seq)
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(m.records)))
		buf = append(buf, tmp[:4]...)
		appendRecords(&buf, &tmp, m.records)
		lens[i] = len(buf) - offs[i]
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...), offs, lens
}

// encodeKeyedGroup encodes a group with at least one keyed member as a
// single BCLK entry. Each member body (whose span is returned for the
// index) is: seq + 2-byte key length + key + record count + records; an
// anonymous member's key length is zero.
func encodeKeyedGroup(members []groupMember) (entry []byte, offs []int, lens []int) {
	size := 4 + 4 + 4
	for _, m := range members {
		size += 8 + 2 + len(m.id) + 4
		for _, r := range m.records {
			size += 4 + len(r)
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, keyedGroup...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(members)))
	buf = append(buf, tmp[:4]...)
	offs = make([]int, len(members))
	lens = make([]int, len(members))
	for i, m := range members {
		offs[i] = len(buf)
		binary.LittleEndian.PutUint64(tmp[:], m.seq)
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(m.id)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, m.id...)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(m.records)))
		buf = append(buf, tmp[:4]...)
		appendRecords(&buf, &tmp, m.records)
		lens[i] = len(buf) - offs[i]
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...), offs, lens
}

// encodeHoles encodes a hole marker reserving the given sequences
// forever. Recovery writes it in place of a torn tail so the reserved
// sequences survive as permanent holes across reopens.
func encodeHoles(seqs []uint64) []byte {
	buf := make([]byte, 0, 4+4+8*len(seqs)+4)
	buf = append(buf, holeMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(seqs)))
	buf = append(buf, tmp[:4]...)
	for _, seq := range seqs {
		binary.LittleEndian.PutUint64(tmp[:], seq)
		buf = append(buf, tmp[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// decodeBody decodes one anonymous batch body (seq + record count +
// records) as located by the index. Unlike segment parsing, any
// inconsistency here is corruption: an indexed body was validated when
// the segment was parsed, so a mismatch means the file changed
// underneath the log.
func decodeBody(body []byte) (uint64, [][]byte, error) {
	if len(body) < 12 {
		return 0, nil, ErrCorruptSegment
	}
	seq := binary.LittleEndian.Uint64(body[:8])
	count := binary.LittleEndian.Uint32(body[8:12])
	records, pos, ok := parseRecords(body, 12, count)
	if !ok || pos != len(body) {
		return 0, nil, ErrCorruptSegment
	}
	return seq, records, nil
}

// decodeKeyedBody decodes one keyed-framed body as located by the
// index. BCLI and BCLK member bodies share one shape: seq, then a
// 2-byte length-prefixed key, then the record count and records. A zero
// key length yields id == "" for an anonymous member of a BCLK group;
// readRef only reaches this decoder for keyed-framed refs. Any framing
// defect is corruption: the encoder only ever writes valid frames.
func decodeKeyedBody(body []byte) (uint64, string, [][]byte, error) {
	if len(body) < 8 {
		return 0, "", nil, ErrCorruptSegment
	}
	seq := binary.LittleEndian.Uint64(body[:8])
	id, n, ok, bad := parseKey(body, 8)
	if bad || !ok || len(body)-n < 4 {
		return 0, "", nil, ErrCorruptSegment
	}
	count := binary.LittleEndian.Uint32(body[n : n+4])
	records, pos, ok := parseRecords(body, n+4, count)
	if !ok || pos != len(body) {
		return 0, "", nil, ErrCorruptSegment
	}
	return seq, id, records, nil
}

// parseKey reads a 2-byte length-prefixed key from body at pos. A zero
// length is framing-valid and returns an empty id (an anonymous member
// of a BCLK group); callers that require a key (BCLI) reject that
// themselves. ok is false when the declared key does not fit (a torn
// write). A present but malformed key (length over 256, NUL byte or
// invalid UTF-8) is reported as bad=true: a torn write cannot produce
// whole-but-invalid key bytes, so callers treat it as ErrCorruptSegment
// rather than a tail.
func parseKey(body []byte, pos int) (id string, npos int, ok, bad bool) {
	if len(body)-pos < 2 {
		return "", 0, false, false
	}
	n := int(binary.LittleEndian.Uint16(body[pos : pos+2]))
	if n > maxBatchIDLen {
		return "", 0, false, true
	}
	end := pos + 2 + n
	if len(body) < end {
		return "", 0, false, false
	}
	if n == 0 {
		return "", end, true, false
	}
	raw := body[pos+2 : end]
	if bytes.IndexByte(raw, 0) >= 0 || !utf8.Valid(raw) {
		return "", 0, false, true
	}
	return string(raw), end, true, false
}

type parsedBatch struct {
	seq     uint64
	id      string // idempotency key, empty for an anonymous batch
	records [][]byte
	off     int // offset of the batch body within the segment
	length  int // length of the batch body
	// keyed reports the entry framing: true for BCLI and every BCLK
	// member (whose body starts with a key length prefix), false for
	// BCL1/BCLG. An anonymous member of a mixed BCLK group is keyed.
	keyed bool
	// diskOff/diskLen/diskCRC describe the whole entry on disk (magic
	// through trailing checksum) and the checksum it stores. The index
	// sidecar records them so adoption can re-verify an entry against
	// the segment without decoding any of its records.
	diskOff int
	diskLen int
	diskCRC uint32
}

type parsedHole struct {
	off    int // offset of the marker's magic within the segment
	length int // total length of the marker on disk, including its CRC
	crc    uint32
	seqs   []uint64
}

// segmentTail describes an incomplete entry at the end of a segment: the
// remnant of a crash in the middle of a write.
type segmentTail struct {
	off  int      // offset where the incomplete entry starts
	seqs []uint64 // sequences the torn entry had already reserved
}

// parseSegment decodes every complete entry in data, returning the
// committed batches (with the offsets of their bodies, for the index)
// and the durable hole markers. A truncated tail — trailing bytes that
// are only a prefix of an entry — is reported separately as the
// signature of a crash mid-write, along with every sequence the torn
// entry had already reserved. Any other inconsistency (unknown magic,
// bad checksum) yields ErrCorruptSegment.
func parseSegment(data []byte) ([]parsedBatch, []parsedHole, *segmentTail, error) {
	var batches []parsedBatch
	var holes []parsedHole
	var off int
	for off < len(data) {
		rest := data[off:]
		if len(rest) < len(segmentMagic) {
			// A partial magic is a torn write only if it is a
			// prefix of a valid magic; all magics share "BCL".
			if !bytes.Equal(rest, segmentMagic[:len(rest)]) {
				return nil, nil, nil, ErrCorruptSegment
			}
			return batches, holes, &segmentTail{off: off}, nil
		}
		switch {
		case bytes.Equal(rest[:4], segmentMagic):
			if len(rest) < entryHeaderLen {
				tail := &segmentTail{off: off}
				if len(rest) >= 12 {
					tail.seqs = append(tail.seqs, binary.LittleEndian.Uint64(rest[4:12]))
				}
				return batches, holes, tail, nil
			}
			seq := binary.LittleEndian.Uint64(rest[4:12])
			count := binary.LittleEndian.Uint32(rest[12:16])
			pos := off + entryHeaderLen
			records, npos, ok := parseRecords(data, pos, count)
			if !ok {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			pos = npos
			if len(data)-pos < 4 {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			batches = append(batches, parsedBatch{
				seq: seq, records: records,
				off: off + 4, length: pos - off - 4,
				diskOff: off, diskLen: pos + 4 - off, diskCRC: crc,
			})
			off = pos + 4
		case bytes.Equal(rest[:4], keyedMagic):
			// BCLI: magic + seq + 2-byte key length + key + count +
			// records + crc. The indexed body starts at seq (right
			// after the magic); decodeKeyedBody consumes it on reads.
			if len(rest) < entryHeaderLen {
				tail := &segmentTail{off: off}
				if len(rest) >= 12 {
					tail.seqs = append(tail.seqs, binary.LittleEndian.Uint64(rest[4:12]))
				}
				return batches, holes, tail, nil
			}
			seq := binary.LittleEndian.Uint64(rest[4:12])
			id, kpos, ok, bad := parseKey(data, off+12)
			if bad {
				return nil, nil, nil, ErrCorruptSegment
			}
			if !ok {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			if id == "" {
				// BCLI always carries a key; a key-less one is tampering.
				return nil, nil, nil, ErrCorruptSegment
			}
			if len(data)-kpos < 4 {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			count := binary.LittleEndian.Uint32(data[kpos : kpos+4])
			records, npos, ok := parseRecords(data, kpos+4, count)
			if !ok {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			pos := npos
			if len(data)-pos < 4 {
				return batches, holes, &segmentTail{off: off, seqs: []uint64{seq}}, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			batches = append(batches, parsedBatch{
				seq: seq, id: id, records: records, keyed: true,
				off: off + 4, length: pos - off - 4,
				diskOff: off, diskLen: pos + 4 - off, diskCRC: crc,
			})
			off = pos + 4
		case bytes.Equal(rest[:4], groupMagic):
			tail := &segmentTail{off: off}
			if len(rest) < 8 {
				return batches, holes, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			var members []parsedBatch
			for i := uint32(0); i < count; i++ {
				if len(data)-pos < 12 {
					// Torn between or inside member headers;
					// recover the member's sequence if whole.
					if len(data)-pos >= 8 {
						tail.seqs = append(tail.seqs, binary.LittleEndian.Uint64(data[pos:pos+8]))
					}
					return batches, holes, tail, nil
				}
				mOff := pos
				seq := binary.LittleEndian.Uint64(data[pos : pos+8])
				nrec := binary.LittleEndian.Uint32(data[pos+8 : pos+12])
				tail.seqs = append(tail.seqs, seq)
				records, npos, ok := parseRecords(data, pos+12, nrec)
				if !ok {
					return batches, holes, tail, nil
				}
				pos = npos
				members = append(members, parsedBatch{seq: seq, records: records, off: mOff, length: pos - mOff})
			}
			if len(data)-pos < 4 {
				return batches, holes, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			// Only a complete, checksummed group publishes its
			// members; a torn group above publishes none.
			for _, mb := range members {
				mb.diskOff, mb.diskLen, mb.diskCRC = off, pos+4-off, crc
				batches = append(batches, mb)
			}
			off = pos + 4
		case bytes.Equal(rest[:4], keyedGroup):
			// BCLK: count members, each framed as seq + 2-byte key
			// length + key + record count + records. Key length 0 marks
			// an anonymous member of a mixed group.
			tail := &segmentTail{off: off}
			if len(rest) < 8 {
				return batches, holes, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			var members []parsedBatch
			for i := uint32(0); i < count; i++ {
				if len(data)-pos < 8 {
					// Torn before the member's sequence: no sequence to
					// reserve a hole for.
					return batches, holes, tail, nil
				}
				mOff := pos
				seq := binary.LittleEndian.Uint64(data[pos : pos+8])
				tail.seqs = append(tail.seqs, seq)
				id, kpos, ok, bad := parseKey(data, pos+8)
				if bad {
					return nil, nil, nil, ErrCorruptSegment
				}
				if !ok {
					// Missing key-length bytes or a key cut short: torn.
					return batches, holes, tail, nil
				}
				pos = kpos
				if len(data)-pos < 4 {
					return batches, holes, tail, nil
				}
				nrec := binary.LittleEndian.Uint32(data[pos : pos+4])
				records, npos, ok := parseRecords(data, pos+4, nrec)
				if !ok {
					return batches, holes, tail, nil
				}
				pos = npos
				members = append(members, parsedBatch{
					seq: seq, id: id, records: records, keyed: true,
					off: mOff, length: pos - mOff,
				})
			}
			if len(data)-pos < 4 {
				return batches, holes, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			for _, mb := range members {
				mb.diskOff, mb.diskLen, mb.diskCRC = off, pos+4-off, crc
				mb.keyed = true
				batches = append(batches, mb)
			}
			off = pos + 4
		case bytes.Equal(rest[:4], holeMagic):
			tail := &segmentTail{off: off}
			if len(rest) < 8 {
				return batches, holes, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			var seqs []uint64
			for i := uint32(0); i < count; i++ {
				if len(data)-pos < 8 {
					tail.seqs = append(tail.seqs, seqs...)
					return batches, holes, tail, nil
				}
				seqs = append(seqs, binary.LittleEndian.Uint64(data[pos:pos+8]))
				pos += 8
			}
			if len(data)-pos < 4 {
				tail.seqs = append(tail.seqs, seqs...)
				return batches, holes, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			holes = append(holes, parsedHole{
				off: off, length: pos + 4 - off, crc: crc,
				seqs: append(make([]uint64, 0, len(seqs)), seqs...),
			})
			off = pos + 4
		default:
			return nil, nil, nil, ErrCorruptSegment
		}
	}
	return batches, holes, nil, nil
}

// parseRecords decodes count records starting at pos. ok is false when
// the data runs out first, which callers read as a torn write.
func parseRecords(data []byte, pos int, count uint32) (records [][]byte, npos int, ok bool) {
	records = make([][]byte, 0, min(int(count), 1<<20))
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 4 {
			return nil, 0, false
		}
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if n < 0 || len(data)-pos < n {
			return nil, 0, false
		}
		rec := make([]byte, n)
		copy(rec, data[pos:pos+n])
		pos += n
		records = append(records, rec)
	}
	return records, pos, true
}
