// Command convert exports SQLite music metadata databases to Parquet files.
//
// Usage:
//
//	convert -db /path/to/main_database.sqlite3 -out /path/to/output/
//	convert -db /path/to/main_database.sqlite3 -out /path/to/output/ -batch-size 50000
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"metadata-api/internal/convert"
)

func main() {
	cfg := convert.DefaultConfig()

	var (
		dbPath    = flag.String("db", "", "path to main_database.sqlite3")
		outDir    = flag.String("out", "", "output directory for parquet files")
		batchSize = flag.Int("batch-size", cfg.BatchSize, "rows per batch when reading from SQLite")
	)
	flag.Parse()

	if *dbPath == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "usage: convert -db <sqlite-path> -out <output-dir>")
		os.Exit(1)
	}

	cfg.BatchSize = *batchSize

	if err := convert.Run(*dbPath, *outDir, cfg); err != nil {
		slog.Error("conversion failed", "err", err)
		os.Exit(1)
	}
}
