package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"unicode/utf8"
)

// The segment-level index lives in a sidecar file ("index.idx") next to
// the segments so that Open never has to decode every segment entry as
// the log grows. The sidecar is a derived, rebuildable cache: losing
// it, or finding it truncated, version-mismatched, checksum-bad or
// contradictory to the segments, only triggers a full rebuild from the
// segment files — never data loss and never a user-visible error.
//
// Layout, all integers little-endian:
//
//	header  8 bytes  "BCLIDX" (6 bytes) + uint16 version
//	records          length-prefixed, checksummed, in (segment, offset)
//	                 order:
//
//	  committed entry ("E"):
//	    8 bytes segment file number
//	    8 bytes offset of the entry's segment magic
//	    8 bytes length of the whole entry on disk (magic ... crc)
//	    4 bytes disk entry CRC (the checksum stored by the segment)
//	    4 bytes member count m
//	    1 byte  flags (bit 0: entry uses keyed framing — BCLI/BCLK)
//	    m × (8-byte seq, 8-byte body offset, 4-byte body length,
//	         2-byte key length, key bytes; key length 0 = anonymous)
//
// Version 1 sidecars (no flags byte and no key fields) are not
// interpreted: a version mismatch triggers the ordinary one-time rebuild
// from the segment files, exactly like any other invalid sidecar.
//
//	hole marker ("H"):
//	  8 bytes segment file number
//	  8 bytes offset of the marker's segment magic
//	  8 bytes length of the whole marker on disk
//	  4 bytes disk marker CRC
//	  4 bytes sequence count c
//	  c × 8-byte sequence
//
// Every record is framed as: 1-byte type, 4-byte body length, body,
// 4-byte CRC32 of type+length+body. A torn final record — the remnant
// of a crash between the index write and its sync — is discarded on the
// next open, and any such truncation makes Open rebuild from the
// segments instead. Records are appended only after the corresponding
// segment entry is durable, so an adopted index can never advertise a
// batch the segments do not contain; adoption re-verifies the physical
// layout and every disk CRC before trusting it.
var (
	indexMagic   = []byte{'B', 'C', 'L', 'I', 'D', 'X'}
	indexVersion = uint16(2)

	recEntry = byte('E')
	recHole  = byte('H')

	// entryFlagKeyed marks an E record whose segment entry uses keyed
	// framing (BCLI for a lone keyed commit, BCLK for a group containing
	// at least one keyed member).
	entryFlagKeyed = byte(1)
)

// indexEntry is one committed segment entry: either a single batch or a
// whole group, located on disk and carrying every member's index ref.
type indexEntry struct {
	seg     int
	off     int // offset of the entry's segment magic
	length  int // total length of the entry on disk, including its CRC
	diskCRC uint32
	keyed   bool // segment entry is BCLI/BCLK rather than BCL1/BCLG
	members []indexMember
}

type indexMember struct {
	seq    uint64
	id     string // idempotency key, "" for an anonymous member
	off    int    // body offset within the segment file
	length int    // body length
}

// indexHole is one durable BCLH marker on disk.
type indexHole struct {
	seg     int
	off     int
	length  int
	diskCRC uint32
	seqs    []uint64
}

type parsedIndex struct {
	entries []indexEntry
	holes   []indexHole
}

var errIndexInvalid = errors.New("log: invalid index sidecar")

// readIndexFile parses the sidecar bytes strictly. A truncated final
// record — the remnant of a crash between an index write and its sync —
// is reported as tailLen (the valid prefix length). Missing tail bytes
// are the only tolerated defect; bad framing, an unknown version or any
// checksum mismatch yield errIndexInvalid.
func readIndexFile(data []byte) (p parsedIndex, tailLen int, err error) {
	const headerLen = 8
	if len(data) < headerLen {
		return parsedIndex{}, 0, errIndexInvalid
	}
	if string(data[:6]) != string(indexMagic) {
		return parsedIndex{}, 0, errIndexInvalid
	}
	if binary.LittleEndian.Uint16(data[6:8]) != indexVersion {
		return parsedIndex{}, 0, errIndexInvalid
	}
	pos := headerLen
	for pos < len(data) {
		start := pos
		rest := data[pos:]
		if len(rest) < 10 {
			// type(1) + length(4) + crc(4) is the smallest record.
			return p, start, nil
		}
		kind := rest[0]
		bodyLen := int(binary.LittleEndian.Uint32(rest[1:5]))
		if bodyLen < 0 {
			return parsedIndex{}, 0, errIndexInvalid
		}
		total := 5 + bodyLen + 4 // type + length + body + crc
		if len(data)-start < total {
			// Any truncation of the last record is a crash tail.
			return p, start, nil
		}
		body := rest[5 : 5+bodyLen]
		crc := binary.LittleEndian.Uint32(rest[5+bodyLen : 9+bodyLen])
		if crc32.ChecksumIEEE(rest[:5+bodyLen]) != crc {
			return parsedIndex{}, 0, errIndexInvalid
		}
		bp := 0
		switch kind {
		case recEntry:
			const fixed = 8 + 8 + 8 + 4 + 4 + 1
			if bodyLen < fixed {
				return parsedIndex{}, 0, errIndexInvalid
			}
			e := indexEntry{}
			e.seg = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			e.off = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			e.length = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			e.diskCRC = binary.LittleEndian.Uint32(body[bp : bp+4])
			bp += 4
			m := int(binary.LittleEndian.Uint32(body[bp : bp+4]))
			bp += 4
			flags := body[bp]
			bp++
			if flags & ^entryFlagKeyed != 0 {
				return parsedIndex{}, 0, errIndexInvalid
			}
			e.keyed = flags&entryFlagKeyed != 0
			if m <= 0 {
				return parsedIndex{}, 0, errIndexInvalid
			}
			e.members = make([]indexMember, m)
			for i := 0; i < m; i++ {
				if bodyLen-bp < 8+8+4+2 {
					return parsedIndex{}, 0, errIndexInvalid
				}
				mb := indexMember{
					seq:    binary.LittleEndian.Uint64(body[bp : bp+8]),
					off:    int(binary.LittleEndian.Uint64(body[bp+8 : bp+16])),
					length: int(binary.LittleEndian.Uint32(body[bp+16 : bp+20])),
				}
				bp += 20
				klen := int(binary.LittleEndian.Uint16(body[bp : bp+2]))
				bp += 2
				if !e.keyed && klen != 0 {
					return parsedIndex{}, 0, errIndexInvalid
				}
				if klen < 0 || klen > maxBatchIDLen || bodyLen-bp < klen {
					return parsedIndex{}, 0, errIndexInvalid
				}
				if klen > 0 {
					raw := body[bp : bp+klen]
					if bytes.IndexByte(raw, 0) >= 0 || !utf8.Valid(raw) {
						return parsedIndex{}, 0, errIndexInvalid
					}
					mb.id = string(raw)
					bp += klen
				}
				e.members[i] = mb
			}
			if bp != bodyLen {
				return parsedIndex{}, 0, errIndexInvalid
			}
			if e.seg <= 0 || e.off < 0 || e.length <= 0 {
				return parsedIndex{}, 0, errIndexInvalid
			}
			p.entries = append(p.entries, e)
		case recHole:
			if bodyLen < 8+8+8+4+4 {
				return parsedIndex{}, 0, errIndexInvalid
			}
			h := indexHole{}
			h.seg = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			h.off = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			h.length = int(binary.LittleEndian.Uint64(body[bp : bp+8]))
			bp += 8
			h.diskCRC = binary.LittleEndian.Uint32(body[bp : bp+4])
			bp += 4
			c := int(binary.LittleEndian.Uint32(body[bp : bp+4]))
			bp += 4
			if h.seg <= 0 || h.off < 0 || h.length <= 0 || c < 0 || 8*c != bodyLen-bp {
				return parsedIndex{}, 0, errIndexInvalid
			}
			h.seqs = make([]uint64, c)
			for i := 0; i < c; i++ {
				h.seqs[i] = binary.LittleEndian.Uint64(body[bp : bp+8])
				bp += 8
			}
			p.holes = append(p.holes, h)
		default:
			return parsedIndex{}, 0, errIndexInvalid
		}
		pos = start + total
	}
	return p, 0, nil
}

// encodeIndexHeader returns the sidecar header.
func encodeIndexHeader() []byte {
	buf := make([]byte, 0, 8)
	buf = append(buf, indexMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], indexVersion)
	return append(buf, tmp[:2]...)
}

// finishIndexRecord wraps a record body with its type tag, length and
// trailing checksum.
func finishIndexRecord(kind byte, body []byte) []byte {
	rec := make([]byte, 0, 5+len(body)+4)
	rec = append(rec, kind)
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(body)))
	rec = append(rec, tmp[:4]...)
	rec = append(rec, body...)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(rec))
	return append(rec, tmp[:4]...)
}

func putIndexU64(body *[]byte, tmp *[8]byte, v uint64) {
	binary.LittleEndian.PutUint64(tmp[:], v)
	*body = append(*body, tmp[:]...)
}

// encodeEntryRecord encodes one committed segment entry and its members.
func encodeEntryRecord(e indexEntry) []byte {
	size := 8 + 8 + 8 + 4 + 4 + 1
	for _, m := range e.members {
		size += 8 + 8 + 4 + 2 + len(m.id)
	}
	body := make([]byte, 0, size)
	var tmp [8]byte
	putIndexU64(&body, &tmp, uint64(e.seg))
	putIndexU64(&body, &tmp, uint64(e.off))
	putIndexU64(&body, &tmp, uint64(e.length))
	binary.LittleEndian.PutUint32(tmp[:4], e.diskCRC)
	body = append(body, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.members)))
	body = append(body, tmp[:4]...)
	flags := byte(0)
	if e.keyed {
		flags = entryFlagKeyed
	}
	body = append(body, flags)
	for _, m := range e.members {
		putIndexU64(&body, &tmp, m.seq)
		putIndexU64(&body, &tmp, uint64(m.off))
		binary.LittleEndian.PutUint32(tmp[:4], uint32(m.length))
		body = append(body, tmp[:4]...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(m.id)))
		body = append(body, tmp[:2]...)
		body = append(body, m.id...)
	}
	return finishIndexRecord(recEntry, body)
}

// encodeHoleRecord encodes one durable hole marker as seen in a segment.
func encodeHoleRecord(h indexHole) []byte {
	body := make([]byte, 0, 8+8+8+4+4+8*len(h.seqs))
	var tmp [8]byte
	putIndexU64(&body, &tmp, uint64(h.seg))
	putIndexU64(&body, &tmp, uint64(h.off))
	putIndexU64(&body, &tmp, uint64(h.length))
	binary.LittleEndian.PutUint32(tmp[:4], h.diskCRC)
	body = append(body, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(h.seqs)))
	body = append(body, tmp[:4]...)
	for _, seq := range h.seqs {
		putIndexU64(&body, &tmp, seq)
	}
	return finishIndexRecord(recHole, body)
}

// diskRecord is one physical on-disk item located by the index, merged
// in (segment, offset) order for the canonical snapshot.
type diskRecord struct {
	seg int
	off int
	enc []byte
}

// writeIndexSnapshot replaces the sidecar atomically with a full image
// of the recovered log: the header, then every committed entry and hole
// marker in (segment, offset) order. The temp file is fsynced before
// the rename; callers run this after the segment bytes it describes are
// synced. A lost rename only leaves the previous sidecar behind, which
// adoption validation rejects as contradictory, triggering a rebuild.
func (l *Log) writeIndexSnapshot(p parsedIndex) error {
	var recs []diskRecord
	for i := range p.entries {
		e := &p.entries[i]
		recs = append(recs, diskRecord{seg: e.seg, off: e.off, enc: encodeEntryRecord(*e)})
	}
	for i := range p.holes {
		h := &p.holes[i]
		recs = append(recs, diskRecord{seg: h.seg, off: h.off, enc: encodeHoleRecord(*h)})
	}
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].seg != recs[j].seg {
			return recs[i].seg < recs[j].seg
		}
		return recs[i].off < recs[j].off
	})
	var buf []byte
	buf = append(buf, encodeIndexHeader()...)
	for _, r := range recs {
		buf = append(buf, r.enc...)
	}
	tmp := l.indexPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, l.indexPath()); err != nil {
		os.Remove(tmp)
		return err
	}
	// Best effort: if the rename is not journaled and power loss keeps
	// the previous sidecar, adoption validation detects the divergence
	// and rebuilds, so snapshot correctness never depends on this sync.
	_ = syncDir(l.dir)
	return nil
}

// appendIndexRecord appends one record to the live sidecar after a
// commit's segment entry is durable. With Options.Sync it fsyncs the
// sidecar before returning. Any failure is returned to the caller,
// which rolls the just-written segment entry and this record back; an
// fsync failure is wrapped as ErrSyncFailed so commits keep their
// documented error semantics.
func (l *Log) appendIndexRecord(rec []byte) error {
	if l.idxFile == nil {
		return errIndexInvalid
	}
	if _, err := l.idxFile.Write(rec); err != nil {
		return err
	}
	l.idxSize += len(rec)
	if l.opts.Sync {
		if err := syncFile(l.idxFile); err != nil {
			return fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	return nil
}

// truncateIndex rolls the sidecar back to size n after a failed commit,
// mirroring the segment-entry rollback so no uncommitted record is left
// behind and a retry cannot duplicate it.
func (l *Log) truncateIndex(n int) {
	if l.idxFile == nil {
		return
	}
	if err := l.idxFile.Truncate(int64(n)); err != nil {
		// Best effort: an untidy sidecar is harmless. Open never trusts
		// it blindly and rebuilds from the segments on any contradiction.
		return
	}
	l.idxSize = n
}

// openIndex opens the live sidecar for append, creating it absent. The
// file has already been validated (or freshly snapshotted) by Open; an
// empty file (a log with no commits yet) gets its header first.
func (l *Log) openIndex() error {
	f, err := os.OpenFile(l.indexPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.idxFile = f
	l.idxSize = int(st.Size())
	if l.idxSize == 0 {
		hdr := encodeIndexHeader()
		if _, err := f.Write(hdr); err != nil {
			f.Close()
			l.idxFile = nil
			return err
		}
		l.idxSize = len(hdr)
	}
	return nil
}

// readIndexBytes reads the whole sidecar. Used only by adoption.
func (l *Log) readIndexBytes() ([]byte, error) {
	f, err := os.Open(l.indexPath())
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
