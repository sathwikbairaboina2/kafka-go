// Command kgoctl inspects and verifies kgod log directories offline.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

const usage = "usage: kgoctl dump-log <segment.log> | kgoctl verify-log <data-dir>"

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "dump-log":
		if err := dumpLog(args[1], stdout); err != nil {
			fmt.Fprintln(stderr, "kgoctl:", err)
			return 1
		}
		return 0
	case "verify-log":
		return verifyLog(args[1], stdout, stderr)
	}
	fmt.Fprintln(stderr, usage)
	return 2
}

var codecNames = []string{"none", "gzip", "snappy", "lz4", "zstd"}

func codecName(c int) string {
	if c < len(codecNames) {
		return codecNames[c]
	}
	return fmt.Sprintf("unknown(%d)", c)
}

// clip renders a key or value, quoting at most 64 bytes.
func clip(b []byte) string {
	if b == nil {
		return "nil"
	}
	if len(b) > 64 {
		return fmt.Sprintf("%q...(%d bytes)", b[:64], len(b))
	}
	return fmt.Sprintf("%q", b)
}

func dumpLog(path string, out io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 256<<10)
	var prefix [12]byte
	for {
		if _, err := io.ReadFull(br, prefix[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("truncated batch prefix: %w", err)
		}
		size, err := record.PeekSize(prefix[:])
		if err != nil {
			return err
		}
		buf := make([]byte, size)
		copy(buf, prefix[:])
		if _, err := io.ReadFull(br, buf[12:]); err != nil {
			return fmt.Errorf("truncated batch: %w", err)
		}
		h, err := record.ReadHeader(buf)
		if err != nil {
			return err
		}
		crc := "ok"
		if _, err := record.Parse(buf); err != nil {
			crc = "BAD"
		}
		fmt.Fprintf(out, "offset=%d..%d records=%d bytes=%d crc=%s codec=%s maxTs=%d\n",
			h.BaseOffset, h.BaseOffset+int64(h.LastOffsetDelta), h.RecordCount, size, crc, codecName(h.Compression()), h.MaxTimestamp)
		if h.Compression() != 0 {
			fmt.Fprintf(out, "  compressed (codec %d), %d records\n", h.Compression(), h.RecordCount)
			continue
		}
		recs, err := record.Records(buf)
		if err != nil {
			fmt.Fprintf(out, "  records unreadable: %v\n", err)
			continue
		}
		for _, r := range recs {
			fmt.Fprintf(out, "  offset=%d key=%s value=%s\n", h.BaseOffset+int64(r.OffsetDelta), clip(r.Key), clip(r.Value))
		}
	}
}

func verifyLog(dir string, stdout, stderr io.Writer) int {
	rep, err := storage.VerifyDir(dir)
	if err != nil {
		fmt.Fprintln(stderr, "kgoctl:", err)
		return 1
	}
	for _, p := range rep.Problems {
		fmt.Fprintln(stdout, p)
	}
	if len(rep.Problems) > 0 {
		fmt.Fprintf(stdout, "FAIL problems=%d\n", len(rep.Problems))
		return 1
	}
	fmt.Fprintf(stdout, "OK partitions=%d segments=%d batches=%d records=%d\n", rep.Partitions, rep.Segments, rep.Batches, rep.Records)
	return 0
}
