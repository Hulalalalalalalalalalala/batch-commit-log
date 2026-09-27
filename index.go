package log

import (
	"encoding/binary"
	"hash/crc32"
)

// The segment-level index lives in a side file next to the segment
// files ("index.idx"), appended after the segment bytes of every
// Commit and CommitGroup are written and synced. The side file is
// only ever adopted incrementally when it is intact and consistent
// with the segment files; otherwise Open discards it and rebuilds it
// from the segments, so losing or corrupting it never loses or
// invents data.
//
// Side file layout, all integers little-endian:
//
//	header:
//	magic   4 bytes  "BCLI"
//	version 4 bytes  indexVersion
//
//	followed by zero or more frames:
//
//	magic   4 bytes  "BCLU"
//	kind    1 byte   kindData (1) or kindHoles (2)
//	seg     8 bytes  segment file number the entry landed in
//	endOff  8 bytes  segment file size right after the entry
//	segCRC  4 bytes  CRC32-IEEE of segment bytes [0, endOff)
//	count   4 bytes  members (kindData) or reserved seqs (kindHoles)
//	data:   count × (seq 8, off 8, len 8)
//	holes:  count × (seq 8)
//	crc32   4 bytes  IEEE checksum of every frame byte before it
//
// segCRC chains over the exact bytes appended to a segment, so an
// intact side file lets Open verify every segment with one streaming
// checksum pass per file instead of decoding its entries. The CRC of
// a concatenation equals crc32.Update of the pieces, hence a frame
// recorded at commit time can carry the running segment digest
// without re-reading the file.
var (
	indexMagic = []byte{'B', 'C', 'L', 'I'}
	frameMagic = []byte{'B', 'C', 'L', 'U'}
)

const indexVersion uint32 = 1

const (
	kindData  = 1 // frame publishes committed batches
	kindHoles = 2 // frame records permanent hole sequences
)

// frameMember locates one committed batch body inside a segment, the
// persistent form of entryRef.
type frameMember struct {
	seq    uint64
	off    uint64
	length uint64
}

// indexFrame is one appended side-file record: either the publication
// of one Commit or CommitGroup (all group members share one frame) or
// the hole sequences written by crash recovery.
type indexFrame struct {
	kind    uint8
	seg     uint64
	endOff  uint64
	segCRC  uint32
	members []frameMember
	seqs    []uint64
}

// encodeIndexShard encodes the header and all frames as one self
// contained side file image.
func encodeIndexShard(frames []indexFrame) []byte {
	buf := make([]byte, 0, 8)
	buf = append(buf, indexMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], indexVersion)
	buf = append(buf, tmp[:4]...)
	for i := range frames {
		buf = append(buf, encodeFrame(&frames[i])...)
	}
	return buf
}

func encodeFrame(f *indexFrame) []byte {
	count := len(f.members)
	if f.kind == kindHoles {
		count = len(f.seqs)
	}
	size := 4 + 1 + 8 + 8 + 4 + 4
	if f.kind == kindData {
		size += count * 24
	} else {
		size += count * 8
	}
	buf := make([]byte, 0, size+4)
	buf = append(buf, frameMagic...)
	buf = append(buf, f.kind)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], f.seg)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], f.endOff)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], f.segCRC)
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(count))
	buf = append(buf, tmp[:4]...)
	if f.kind == kindData {
		for _, m := range f.members {
			binary.LittleEndian.PutUint64(tmp[:], m.seq)
			buf = append(buf, tmp[:]...)
			binary.LittleEndian.PutUint64(tmp[:], m.off)
			buf = append(buf, tmp[:]...)
			binary.LittleEndian.PutUint64(tmp[:], m.length)
			buf = append(buf, tmp[:]...)
		}
	} else {
		for _, seq := range f.seqs {
			binary.LittleEndian.PutUint64(tmp[:], seq)
			buf = append(buf, tmp[:]...)
		}
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// parseIndex decodes and fully validates a side file image. Any
// missing prefix, unknown version, malformed frame or checksum
// mismatch makes it return ok == false, which sends Open down the
// rebuild path.
func parseIndex(data []byte) (frames []indexFrame, ok bool) {
	frames, consumed, headerOK, truncated := salvageIndex(data)
	if !headerOK || truncated || consumed != len(data) {
		return nil, false
	}
	return frames, true
}

// salvageIndex returns the longest strictly-valid frame prefix of a
// side file. Every frame up to consumed is structurally intact and
// checksum-valid. truncated reports that parsing stopped because the
// file ran out of bytes mid-frame — the signature of a crash while
// appending a frame — in which case the returned frames still mark
// the durable commit watermark; a bad magic, unknown kind or checksum
// mismatch with all bytes present is content corruption instead,
// truncated is false and callers discard the whole file. headerOK is
// false when the 8-byte header is missing or carries an unknown
// version.
func salvageIndex(data []byte) (frames []indexFrame, consumed int, headerOK, truncated bool) {
	if len(data) < 8 || string(data[:4]) != string(indexMagic) {
		return nil, 0, false, false
	}
	if binary.LittleEndian.Uint32(data[4:8]) != indexVersion {
		return nil, 0, false, false
	}
	const frameHead = 4 + 1 + 8 + 8 + 4 + 4
	pos := 8
	for pos < len(data) {
		if len(data)-pos < frameHead+4 {
			return frames, pos, true, true
		}
		if string(data[pos:pos+4]) != string(frameMagic) {
			return frames, pos, true, false
		}
		kind := data[pos+4]
		seg := binary.LittleEndian.Uint64(data[pos+5 : pos+13])
		endOff := binary.LittleEndian.Uint64(data[pos+13 : pos+21])
		segCRC := binary.LittleEndian.Uint32(data[pos+21 : pos+25])
		count := binary.LittleEndian.Uint32(data[pos+25 : pos+29])
		payPos := pos + frameHead
		var payload int
		switch kind {
		case kindData:
			payload = int(count) * 24
		case kindHoles:
			payload = int(count) * 8
		default:
			return frames, pos, true, false
		}
		if payload < 0 || count > uint32(len(data)) {
			return frames, pos, true, false
		}
		if len(data)-payPos < payload+4 {
			return frames, pos, true, true
		}
		crcAt := payPos + payload
		if crc32.ChecksumIEEE(data[pos:crcAt]) != binary.LittleEndian.Uint32(data[crcAt:crcAt+4]) {
			return frames, pos, true, false
		}
		f := indexFrame{kind: kind, seg: seg, endOff: endOff, segCRC: segCRC}
		if kind == kindData {
			f.members = make([]frameMember, 0, count)
			for i := uint32(0); i < count; i++ {
				m := frameMember{
					seq:    binary.LittleEndian.Uint64(data[payPos : payPos+8]),
					off:    binary.LittleEndian.Uint64(data[payPos+8 : payPos+16]),
					length: binary.LittleEndian.Uint64(data[payPos+16 : payPos+24]),
				}
				f.members = append(f.members, m)
				payPos += 24
			}
		} else {
			f.seqs = make([]uint64, 0, count)
			for i := uint32(0); i < count; i++ {
				f.seqs = append(f.seqs, binary.LittleEndian.Uint64(data[payPos:payPos+8]))
				payPos += 8
			}
		}
		frames = append(frames, f)
		pos = crcAt + 4
	}
	return frames, pos, true, false
}
