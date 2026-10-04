// Command report prints the README benchmark table and headline sentence from bench/results/*.json.
// Every number comes from those files; nothing is typed in here.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

type runStats struct {
	RecordsPerSec float64 `json:"records_per_s"`
	MBPerSec      float64 `json:"mb_per_s"`
	P50Ms         float64 `json:"p50_ms"`
	P99Ms         float64 `json:"p99_ms"`
	P999Ms        float64 `json:"p999_ms"`
	Errors        int64   `json:"errors"`
}

type loadResult struct {
	Label  string     `json:"label"`
	Runs   []runStats `json:"runs"`
	Median runStats   `json:"median"`
}

type crashRun struct {
	Mode       string `json:"mode"`
	Iterations int    `json:"iterations"`
	Acked      int    `json:"acked"`
	Lost       int    `json:"lost"`
	VerifyOK   bool   `json:"verify_ok"`
}

type crashResult struct {
	Runs []crashRun `json:"runs"`
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

func main() {
	dir := flag.String("dir", "bench/results", "directory with the result files")
	flag.Parse()
	var never, always, kafka loadResult
	var crash crashResult
	for _, f := range []struct {
		name string
		v    any
	}{{"kgod-never.json", &never}, {"kgod-always.json", &always}, {"kafka.json", &kafka}, {"crash.json", &crash}} {
		if err := readJSON(filepath.Join(*dir, f.name), f.v); err != nil {
			fmt.Fprintln(os.Stderr, "report:", err)
			os.Exit(1)
		}
	}
	if kafka.Median.MBPerSec <= 0 {
		fmt.Fprintln(os.Stderr, "report: kafka.json has no throughput")
		os.Exit(1)
	}

	fmt.Println("| Target | MB/s | records/s | p50 ms | p99 ms |")
	fmt.Println("|---|---:|---:|---:|---:|")
	row := func(name string, r runStats) {
		fmt.Printf("| %s | %.1f | %.0f | %.2f | %.2f |\n", name, r.MBPerSec, r.RecordsPerSec, r.P50Ms, r.P99Ms)
	}
	row("kgod, fsync never", never.Median)
	row("kgod, fsync always", always.Median)
	row("Apache Kafka 4.3.1 (defaults)", kafka.Median)
	fmt.Println()

	var acked, lost, kills int
	for _, r := range crash.Runs {
		acked += r.Acked
		lost += r.Lost
		kills += r.Iterations
	}
	pct := never.Median.MBPerSec / kafka.Median.MBPerSec * 100
	fmt.Printf("kgod: %.1f MB/s produce (%.0f%% of Apache Kafka 4.3.1 on the same machine), p99 %.1f ms; %d of %d acked records lost across %d kill -9 restarts.\n",
		never.Median.MBPerSec, pct, never.Median.P99Ms, lost, acked, kills)
}
