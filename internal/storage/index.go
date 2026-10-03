package storage

import (
	"encoding/binary"
	"sort"
)

// indexEntrySize is the on-disk size of one sparse index entry.
const indexEntrySize = 8

// indexEntry maps a batch's offset relative to the segment base to its byte position in the .log file.
type indexEntry struct {
	rel int32
	pos int32
}

func (e indexEntry) encode() []byte {
	var b [indexEntrySize]byte
	binary.BigEndian.PutUint32(b[0:], uint32(e.rel))
	binary.BigEndian.PutUint32(b[4:], uint32(e.pos))
	return b[:]
}

func decodeIndex(b []byte) []indexEntry {
	n := len(b) / indexEntrySize
	out := make([]indexEntry, n)
	for i := range out {
		out[i] = indexEntry{
			rel: int32(binary.BigEndian.Uint32(b[i*indexEntrySize:])),
			pos: int32(binary.BigEndian.Uint32(b[i*indexEntrySize+4:])),
		}
	}
	return out
}

// lookupIndex returns the position of the greatest entry with rel <= wantRel, or 0.
func lookupIndex(entries []indexEntry, wantRel int64) int64 {
	i := sort.Search(len(entries), func(i int) bool { return int64(entries[i].rel) > wantRel })
	if i == 0 {
		return 0
	}
	return int64(entries[i-1].pos)
}
