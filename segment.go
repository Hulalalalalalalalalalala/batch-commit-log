package log

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
// Hole marker, written by crash recovery to make the sequences reserved
// by a torn tail permanent ("BCLH"):
//
//	magic   4 bytes  "BCLH"
//	count   4 bytes  number of reserved sequences
//	seqs    count × 8 bytes
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Keyed single batch, written by Commit for a batch staged with
// AppendIdempotent ("BCLK"):
//
//	magic   4 bytes  "BCLK"
//	seq     8 bytes  batch sequence number
//	klen    4 bytes  idempotency-key length
//	key     klen bytes
//	count   4 bytes  number of records
//	records count × (4-byte length + opaque bytes)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Mixed batch group, written by CommitGroup when at least one member
// carries an idempotency key ("BCLM"):
//
//	magic   4 bytes  "BCLM"
//	count   4 bytes  number of batches in the group
//	batches count × (1-byte flag; if keyed: 4-byte klen + key;
//	                 8-byte seq + 4-byte record count + records)
//	crc32   4 bytes  IEEE checksum of everything before it
//
// Keyed encodings exist only for batches created with AppendIdempotent;
// old anonymous batches keep their BCL1/BCLG framing and no key is ever
// inferred for them.
//
// The trailing checksum makes tampering detectable as ErrCorruptSegment
// when the segment is parsed. A torn write left by a crash is instead
// reported as a tail: a strict prefix of a valid entry, never a full
// entry with a bad checksum. A group entry is durable only as a whole —
// a truncated group is a tail and none of its batches become visible.
var (
	segmentMagic = []byte{'B', 'C', 'L', '1'}
	groupMagic   = []byte{'B', 'C', 'L', 'G'}
	holeMagic    = []byte{'B', 'C', 'L', 'H'}
	keyedMagic   = []byte{'B', 'C', 'L', 'K'}
	mixedMagic   = []byte{'B', 'C', 'L', 'M'}
)

// memberKeyFlag marks a keyed member inside a BCLM mixed group entry.
const memberKeyFlag byte = 1

const entryHeaderLen = 4 + 8 + 4

// groupMember is one staged batch inside a group commit.
type groupMember struct {
	seq     uint64
	key     string // empty for an anonymous member
	records [][]byte
}

func encodeBatch(seq uint64, records [][]byte) []byte {
	return encodeBatchWithMagic(segmentMagic, seq, "", records)
}

// encodeKeyedBatch encodes a single batch carrying its idempotency key.
func encodeKeyedBatch(seq uint64, key string, records [][]byte) []byte {
	return encodeBatchWithMagic(keyedMagic, seq, key, records)
}

func encodeBatchWithMagic(magic []byte, seq uint64, key string, records [][]byte) []byte {
	size := 4 + 8 + 4
	if len(key) > 0 {
		size += 4 + len(key)
	}
	for _, r := range records {
		size += 4 + len(r)
	}
	size += 4
	buf := make([]byte, 0, size)
	buf = append(buf, magic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	buf = append(buf, tmp[:]...)
	if len(key) > 0 {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(key)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, key...)
	}
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

// encodeGroup encodes a batch group as a single entry. It emits the
// anonymous BCLG framing only when every member is keyless; a group
// containing any AppendIdempotent batch uses the BCLM framing that
// carries per-member keys. It also returns, for each member, the offset
// and length of its decodable body (seq + record count + records)
// relative to the start of the entry, so the index can point straight at
// one member of the group.
func encodeGroup(members []groupMember) (entry []byte, offs []int, lens []int) {
	anyKey := false
	for _, m := range members {
		if m.key != "" {
			anyKey = true
			break
		}
	}
	magic := groupMagic
	if anyKey {
		magic = mixedMagic
	}
	size := 4 + 4 + 4
	for _, m := range members {
		if anyKey {
			size += 1
			if m.key != "" {
				size += 4 + len(m.key)
			}
		}
		size += 8 + 4
		for _, r := range m.records {
			size += 4 + len(r)
		}
	}
	buf := make([]byte, 0, size)
	buf = append(buf, magic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(members)))
	buf = append(buf, tmp[:4]...)
	offs = make([]int, len(members))
	lens = make([]int, len(members))
	for i, m := range members {
		if anyKey {
			if m.key != "" {
				buf = append(buf, memberKeyFlag)
				binary.LittleEndian.PutUint32(tmp[:4], uint32(len(m.key)))
				buf = append(buf, tmp[:4]...)
				buf = append(buf, m.key...)
			} else {
				buf = append(buf, 0)
			}
		}
		offs[i] = len(buf)
		binary.LittleEndian.PutUint64(tmp[:], m.seq)
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(m.records)))
		buf = append(buf, tmp[:4]...)
		for _, r := range m.records {
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r)))
			buf = append(buf, tmp[:4]...)
			buf = append(buf, r...)
		}
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

// decodeBody decodes one batch body (seq + record count + records) as
// located by the index. Unlike segment parsing, any inconsistency here
// is corruption: an indexed body was validated when the segment was
// parsed, so a mismatch means the file changed underneath the log.
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

// decodeKeyedBody decodes the body of a BCLK keyed single batch
// (seq + klen + key + record count + records). The caller already knows
// the batch's key from its index ref; it is cross-checked here, so a
// segment changed underneath the log surfaces as ErrCorruptSegment.
func decodeKeyedBody(body []byte, wantKey string) (uint64, [][]byte, error) {
	if len(body) < 16 {
		return 0, nil, ErrCorruptSegment
	}
	seq := binary.LittleEndian.Uint64(body[:8])
	klen := int(binary.LittleEndian.Uint32(body[8:12]))
	if klen <= 0 || 12+klen+4 > len(body) {
		return 0, nil, ErrCorruptSegment
	}
	if string(body[12:12+klen]) != wantKey {
		return 0, nil, ErrCorruptSegment
	}
	count := binary.LittleEndian.Uint32(body[12+klen : 16+klen])
	records, pos, ok := parseRecords(body, 16+klen, count)
	if !ok || pos != len(body) {
		return 0, nil, ErrCorruptSegment
	}
	return seq, records, nil
}

type parsedBatch struct {
	seq     uint64
	key     string // empty for an anonymous batch
	records [][]byte
	off     int // offset of the batch body within the segment
	length  int // length of the batch body
	// keyedSingle marks a BCLK body, whose bytes are seq + klen + key +
	// record count + records rather than the plain body layout.
	keyedSingle bool
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
			tail := &segmentTail{off: off}
			if len(rest) < 12 {
				return batches, holes, tail, nil
			}
			seq := binary.LittleEndian.Uint64(rest[4:12])
			tail.seqs = []uint64{seq}
			if len(rest) < 16 {
				return batches, holes, tail, nil
			}
			klen := int(binary.LittleEndian.Uint32(rest[12:16]))
			// An out-of-range klen can never prefix an entry this log
			// wrote: framing corruption, not a torn write.
			if klen < 1 || klen > maxKeyLen {
				return nil, nil, nil, ErrCorruptSegment
			}
			if len(rest) < 16+klen {
				// Some of the key is cut off: a torn write.
				return batches, holes, tail, nil
			}
			key := string(rest[16 : 16+klen])
			if !validKey(key) {
				return nil, nil, nil, ErrCorruptSegment
			}
			if len(rest) < 20+klen {
				return batches, holes, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[16+klen : 20+klen])
			pos := off + 20 + klen
			records, npos, ok := parseRecords(data, pos, count)
			if !ok {
				return batches, holes, tail, nil
			}
			pos = npos
			if len(data)-pos < 4 {
				return batches, holes, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[off:pos]) != crc {
				return nil, nil, nil, ErrCorruptSegment
			}
			batches = append(batches, parsedBatch{
				seq: seq, key: key, records: records,
				off: off + 4, length: pos - off - 4, keyedSingle: true,
				diskOff: off, diskLen: pos + 4 - off, diskCRC: crc,
			})
			off = pos + 4
		case bytes.Equal(rest[:4], groupMagic), bytes.Equal(rest[:4], mixedMagic):
			mixed := bytes.Equal(rest[:4], mixedMagic)
			tail := &segmentTail{off: off}
			if len(rest) < 8 {
				return batches, holes, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			var members []parsedBatch
			for i := uint32(0); i < count; i++ {
				var key string
				if mixed {
					// Per-member key prefix: 1-byte flag, then, for a
					// keyed member, 4-byte klen and the key.
					if len(data)-pos < 1 {
						return batches, holes, tail, nil
					}
					flag := data[pos]
					pos++
					if flag&^memberKeyFlag != 0 {
						return nil, nil, nil, ErrCorruptSegment
					}
					if flag&memberKeyFlag != 0 {
						if len(data)-pos < 4 {
							return batches, holes, tail, nil
						}
						klen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
						if klen < 1 || klen > maxKeyLen {
							// An out-of-range klen is framing
							// corruption, not a torn write.
							return nil, nil, nil, ErrCorruptSegment
						}
						if len(data)-pos-4 < klen {
							// The whole klen field is present but some
							// of the key was cut off: a torn write.
							return batches, holes, tail, nil
						}
						key = string(data[pos+4 : pos+4+klen])
						if !validKey(key) {
							return nil, nil, nil, ErrCorruptSegment
						}
						pos += 4 + klen
					}
				}
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
				members = append(members, parsedBatch{
					seq: seq, key: key, records: records, off: mOff, length: pos - mOff,
				})
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
