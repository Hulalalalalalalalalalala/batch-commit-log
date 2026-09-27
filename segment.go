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
// The trailing checksum makes tampering detectable as ErrCorruptSegment
// when the segment is parsed. A torn write left by a crash is instead
// reported as a tail: a strict prefix of a valid entry, never a full
// entry with a bad checksum. A group entry is durable only as a whole —
// a truncated group is a tail and none of its batches become visible.
//
// Which bytes count as committed is decided solely by the index side
// file: an entry that sits in the last segment past the indexed prefix
// (a crash after the segment sync but before the index frame was
// synced) is an uncommitted remnant, even when it is structurally
// complete. Recovery truncates it exactly like a torn tail and its
// reserved sequences become permanent holes.
var (
	segmentMagic = []byte{'B', 'C', 'L', '1'}
	groupMagic   = []byte{'B', 'C', 'L', 'G'}
	holeMagic    = []byte{'B', 'C', 'L', 'H'}
)

const entryHeaderLen = 4 + 8 + 4

// groupMember is one staged batch inside a group commit.
type groupMember struct {
	seq     uint64
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
	for _, r := range records {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(r)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, r...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// encodeGroup encodes a batch group as a single entry. It also returns,
// for each member, the offset and length of its decodable body (seq +
// record count + records) relative to the start of the entry, so the
// index can point straight at one member of the group.
func encodeGroup(members []groupMember) (entry []byte, offs []int, lens []int) {
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
// adopted, so a mismatch means the file changed underneath the log.
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

// segmentTail describes an uncommitted remnant at the end of a
// segment: a torn entry (crash mid-write) or a structurally complete
// entry whose index frame was never synced (crash between the segment
// sync and the index sync).
type segmentTail struct {
	off       int      // offset where the remnant starts
	seqs      []uint64 // sequences the remnant had already reserved
	prefixCRC uint32   // CRC32 of the durable bytes before the remnant
}

// walkEntries scans one segment file structurally without copying
// record payloads, emitting exactly the index frames that describe its
// durable entries: one data frame per Commit/CommitGroup entry (a
// group stays one frame with all its members) and one holes frame per
// hole marker. Every frame carries the segment size and running CRC32
// right after that entry, so the frames produced by a full rebuild are
// byte-for-byte the ones the live commits would have appended.
//
// A truncated suffix — trailing bytes that are only a prefix of an
// entry — is reported separately as the signature of a crash
// mid-write, along with every sequence the torn entry had already
// reserved. Any other inconsistency (unknown magic, bad checksum)
// yields ErrCorruptSegment.
func walkEntries(data []byte, seg uint64) (frames []indexFrame, tail *segmentTail, err error) {
	var runCRC uint32
	off := 0
	for off < len(data) {
		entryStart := off
		rest := data[off:]
		if len(rest) < len(segmentMagic) {
			// A partial magic is a torn write only if it is a
			// prefix of a valid magic; all magics share "BCL".
			if !bytes.Equal(rest, segmentMagic[:len(rest)]) {
				return nil, nil, ErrCorruptSegment
			}
			return frames, &segmentTail{off: off, prefixCRC: runCRC}, nil
		}
		switch {
		case bytes.Equal(rest[:4], segmentMagic):
			if len(rest) < entryHeaderLen {
				tail = &segmentTail{off: off, prefixCRC: runCRC}
				if len(rest) >= 12 {
					tail.seqs = append(tail.seqs, binary.LittleEndian.Uint64(rest[4:12]))
				}
				return frames, tail, nil
			}
			seq := binary.LittleEndian.Uint64(rest[4:12])
			count := binary.LittleEndian.Uint32(rest[12:16])
			pos, ok := skipRecords(data, off+entryHeaderLen, count)
			if !ok {
				return frames, &segmentTail{off: off, seqs: []uint64{seq}, prefixCRC: runCRC}, nil
			}
			if len(data)-pos < 4 {
				return frames, &segmentTail{off: off, seqs: []uint64{seq}, prefixCRC: runCRC}, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[entryStart:pos]) != crc {
				return nil, nil, ErrCorruptSegment
			}
			off = pos + 4
			runCRC = crc32.Update(runCRC, crc32.IEEETable, data[entryStart:off])
			frames = append(frames, indexFrame{
				kind:   kindData,
				seg:    seg,
				endOff: uint64(off),
				segCRC: runCRC,
				members: []frameMember{{
					seq:    seq,
					off:    uint64(entryStart + len(segmentMagic)),
					length: uint64(pos - entryStart - len(segmentMagic)),
				}},
			})
		case bytes.Equal(rest[:4], groupMagic):
			tail = &segmentTail{off: off, prefixCRC: runCRC}
			if len(rest) < 8 {
				return frames, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			members := make([]frameMember, 0, count)
			for i := uint32(0); i < count; i++ {
				if len(data)-pos < 12 {
					// Torn between or inside member headers;
					// recover the member's sequence if whole.
					if len(data)-pos >= 8 {
						tail.seqs = append(tail.seqs, binary.LittleEndian.Uint64(data[pos:pos+8]))
					}
					return frames, tail, nil
				}
				mOff := pos
				seq := binary.LittleEndian.Uint64(data[pos : pos+8])
				nrec := binary.LittleEndian.Uint32(data[pos+8 : pos+12])
				tail.seqs = append(tail.seqs, seq)
				npos, ok := skipRecords(data, pos+12, nrec)
				if !ok {
					return frames, tail, nil
				}
				pos = npos
				members = append(members, frameMember{
					seq:    seq,
					off:    uint64(mOff),
					length: uint64(pos - mOff),
				})
			}
			if len(data)-pos < 4 {
				return frames, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[entryStart:pos]) != crc {
				return nil, nil, ErrCorruptSegment
			}
			// Only a complete, checksummed group publishes its
			// members; a torn group above publishes none.
			off = pos + 4
			runCRC = crc32.Update(runCRC, crc32.IEEETable, data[entryStart:off])
			frames = append(frames, indexFrame{
				kind:    kindData,
				seg:     seg,
				endOff:  uint64(off),
				segCRC:  runCRC,
				members: members,
			})
		case bytes.Equal(rest[:4], holeMagic):
			tail := &segmentTail{off: off, prefixCRC: runCRC}
			if len(rest) < 8 {
				return frames, tail, nil
			}
			count := binary.LittleEndian.Uint32(rest[4:8])
			pos := off + 8
			var seqs []uint64
			for i := uint32(0); i < count; i++ {
				if len(data)-pos < 8 {
					tail.seqs = append(tail.seqs, seqs...)
					return frames, tail, nil
				}
				seqs = append(seqs, binary.LittleEndian.Uint64(data[pos:pos+8]))
				pos += 8
			}
			if len(data)-pos < 4 {
				tail.seqs = append(tail.seqs, seqs...)
				return frames, tail, nil
			}
			crc := binary.LittleEndian.Uint32(data[pos : pos+4])
			if crc32.ChecksumIEEE(data[entryStart:pos]) != crc {
				return nil, nil, ErrCorruptSegment
			}
			off = pos + 4
			runCRC = crc32.Update(runCRC, crc32.IEEETable, data[entryStart:off])
			frames = append(frames, indexFrame{
				kind:   kindHoles,
				seg:    seg,
				endOff: uint64(off),
				segCRC: runCRC,
				seqs:   seqs,
			})
		default:
			return nil, nil, ErrCorruptSegment
		}
	}
	return frames, nil, nil
}

// skipRecords walks count records starting at pos using only their
// length prefixes, without copying payloads. ok is false when the data
// runs out first, which callers read as a torn write.
func skipRecords(data []byte, pos int, count uint32) (npos int, ok bool) {
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 4 {
			return 0, false
		}
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if n < 0 || len(data)-pos < n {
			return 0, false
		}
		pos += n
	}
	return pos, true
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
