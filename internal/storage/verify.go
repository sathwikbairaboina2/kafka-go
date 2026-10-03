package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// VerifyReport is the result of VerifyDir.
type VerifyReport struct {
	Partitions, Segments, Batches int
	Records                       int64
	Problems                      []string
}

func (r *VerifyReport) problem(format string, args ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
}

// VerifyDir checks every <topic>-<partition> directory under dataDir (meta/ and __offsets/ are skipped):
// every batch parses with a valid CRC, base offsets are dense within and across segments, and index
// entries are increasing and point at batch starts whose base offset is segment base + rel.
func VerifyDir(dataDir string) (VerifyReport, error) {
	var rep VerifyReport
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return rep, fmt.Errorf("read data dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || name == "meta" || name == "__offsets" {
			continue
		}
		i := strings.LastIndexByte(name, '-')
		if i < 0 {
			continue
		}
		if _, err := strconv.Atoi(name[i+1:]); err != nil {
			continue
		}
		rep.Partitions++
		if err := verifyPartition(filepath.Join(dataDir, name), name, &rep); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

func verifyPartition(dir, name string, rep *VerifyReport) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read %s: %w", dir, err)
	}
	var bases []int64
	for _, f := range files {
		if b, ok := parseSegmentBase(f.Name()); ok {
			bases = append(bases, b)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	var next int64 = -1
	for _, base := range bases {
		rep.Segments++
		if next >= 0 && base != next {
			rep.problem("%s: gap before segment %s: previous segment ends at offset %d", name, segmentName(base), next)
		}
		end, err := verifySegment(dir, name, base, rep)
		if err != nil {
			return err
		}
		next = end
	}
	return nil
}

func verifySegment(dir, name string, base int64, rep *VerifyReport) (int64, error) {
	seg := segmentName(base)
	idxRaw, err := os.ReadFile(filepath.Join(dir, seg+".index"))
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("read index: %w", err)
	}
	if len(idxRaw)%indexEntrySize != 0 {
		rep.problem("%s/%s.index: size %d is not a multiple of %d", name, seg, len(idxRaw), indexEntrySize)
	}
	idx := decodeIndex(idxRaw)
	wantAt := map[int64]int32{}
	for i, e := range idx {
		if i > 0 && (e.rel <= idx[i-1].rel || e.pos <= idx[i-1].pos) {
			rep.problem("%s/%s.index: entry %d is not increasing", name, seg, i)
		}
		wantAt[int64(e.pos)] = e.rel
	}
	f, err := os.Open(filepath.Join(dir, seg+".log"))
	if err != nil {
		return 0, fmt.Errorf("open log: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	br := bufio.NewReaderSize(f, 256<<10)
	next := base
	var pos int64
	for pos < st.Size() {
		var prefix [12]byte
		if _, err := io.ReadFull(br, prefix[:]); err != nil {
			rep.problem("%s/%s.log: truncated batch prefix at %d", name, seg, pos)
			return next, nil
		}
		size, perr := record.PeekSize(prefix[:])
		if perr != nil || pos+size > st.Size() {
			rep.problem("%s/%s.log: bad batch size at %d", name, seg, pos)
			return next, nil
		}
		buf := make([]byte, size)
		copy(buf, prefix[:])
		if _, err := io.ReadFull(br, buf[12:]); err != nil {
			rep.problem("%s/%s.log: truncated batch at %d", name, seg, pos)
			return next, nil
		}
		h, perr := record.Parse(buf)
		if perr != nil {
			rep.problem("%s/%s.log: batch at %d: %v", name, seg, pos, perr)
			return next, nil
		}
		if h.BaseOffset != next {
			rep.problem("%s/%s.log: batch at %d has base offset %d, expected %d (gap or overlap)", name, seg, pos, h.BaseOffset, next)
		}
		if rel, ok := wantAt[pos]; ok && base+int64(rel) != h.BaseOffset {
			rep.problem("%s/%s.index: entry for position %d says offset %d, batch has %d", name, seg, pos, base+int64(rel), h.BaseOffset)
		}
		delete(wantAt, pos)
		rep.Batches++
		rep.Records += int64(h.RecordCount)
		next = h.BaseOffset + int64(h.LastOffsetDelta) + 1
		pos += size
	}
	for p := range wantAt {
		rep.problem("%s/%s.index: entry points at position %d which is not a batch start", name, seg, p)
	}
	return next, nil
}
