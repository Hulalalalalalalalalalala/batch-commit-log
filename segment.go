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
// when the segment is parsed. An entry cut short by the end of the file
// is a torn tail left by a crash mid-write, not corruption.
var segmentMagic = []byte{'B', 'C', 'L', '1'}

const entryHeaderLen = 4 + 8 + 4

// errTornEntry marks an entry cut short by the end of the file: the
// bytes present are a valid prefix of an entry that was never finished
// writing. It is reported separately from corruption so the latest
// segment's tail can be discarded without failing the whole log.
var errTornEntry = errors.New("log: torn entry")

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

// parseSegment decodes every complete batch entry in data and reports
// how many bytes those entries occupy. A torn final entry is tolerated
// only when tolerateTail is set (the latest segment): parsing stops
// before it and the caller discards the leftover bytes. Anything else
// malformed — bad magic, a checksum mismatch, or a tear in an earlier
// segment — is corruption and yields ErrCorruptSegment.
func parseSegment(data []byte, tolerateTail bool) ([]parsedBatch, int, error) {
	var batches []parsedBatch
	off := 0
	for off < len(data) {
		batch, size, err := parseEntry(data[off:])
		if err != nil {
			if errors.Is(err, errTornEntry) && tolerateTail {
				break
			}
			return nil, 0, ErrCorruptSegment
		}
		batches = append(batches, batch)
		off += size
	}
	return batches, off, nil
}

// parseEntry decodes the single batch entry at the start of data and
// returns its length in bytes. Running out of data mid-entry yields
// errTornEntry; content that contradicts itself (bad magic or checksum)
// yields ErrCorruptSegment. A torn write is always a prefix of a valid
// entry, so even a short remain must start with the magic.
func parseEntry(data []byte) (parsedBatch, int, error) {
	if len(data) < 4 {
		if !bytes.Equal(data, segmentMagic[:len(data)]) {
			return parsedBatch{}, 0, ErrCorruptSegment
		}
		return parsedBatch{}, 0, errTornEntry
	}
	if !bytes.Equal(data[:4], segmentMagic) {
		return parsedBatch{}, 0, ErrCorruptSegment
	}
	if len(data) < entryHeaderLen {
		return parsedBatch{}, 0, errTornEntry
	}
	seq := binary.LittleEndian.Uint64(data[4:12])
	count := binary.LittleEndian.Uint32(data[12:16])
	pos := entryHeaderLen
	records := make([][]byte, 0, min(int(count), 1<<20))
	for i := uint32(0); i < count; i++ {
		if len(data)-pos < 4 {
			return parsedBatch{}, 0, errTornEntry
		}
		n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if n < 0 || len(data)-pos < n {
			return parsedBatch{}, 0, errTornEntry
		}
		rec := make([]byte, n)
		copy(rec, data[pos:pos+n])
		pos += n
		records = append(records, rec)
	}
	if len(data)-pos < 4 {
		return parsedBatch{}, 0, errTornEntry
	}
	crc := binary.LittleEndian.Uint32(data[pos : pos+4])
	if crc32.ChecksumIEEE(data[:pos]) != crc {
		return parsedBatch{}, 0, ErrCorruptSegment
	}
	pos += 4
	return parsedBatch{seq: seq, records: records}, pos, nil
}
