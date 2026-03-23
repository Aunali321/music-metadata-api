package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"metadata-api/internal/api"
	"metadata-api/internal/convert"
	"metadata-api/internal/db"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "listen address")
		dbPath    = flag.String("db", "", "path to main_database.sqlite3 (sqlite backend)")
		backend   = flag.String("backend", "sqlite", "database backend: sqlite or parquet")
		dataDir   = flag.String("data", "", "directory containing parquet files (parquet backend)")
		batchSize = flag.Int("batch-size", 100000, "rows per batch during parquet conversion")
	)
	flag.Parse()

	var database db.Database
	var err error

	switch *backend {
	case "sqlite":
		if *dbPath == "" {
			slog.Error("-db path required for sqlite backend")
			os.Exit(1)
		}
		database, err = db.Open(*dbPath)

	case "parquet":
		if *dataDir == "" {
			if *dbPath != "" {
				*dataDir = filepath.Join(filepath.Dir(*dbPath), "parquet")
			} else {
				slog.Error("-data or -db required for parquet backend")
				os.Exit(1)
			}
		}
		database, err = openParquetWithAutoConvert(*dbPath, *dataDir, *batchSize)

	default:
		slog.Error("unknown backend", "backend", *backend)
		os.Exit(1)
	}

	if err != nil {
		slog.Error("open database", "backend", *backend, "err", err)
		os.Exit(1)
	}
	defer database.Close()

	slog.Info("database opened", "backend", *backend)

	handler := api.New(database)
	rateLimiter := api.NewRateLimiter(100, 200)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      rateLimiter.Middleware(handler.Routes()),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		slog.Info("starting server", "addr", *addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

// openParquetWithAutoConvert opens parquet files, converting from SQLite first if needed.
func openParquetWithAutoConvert(dbPath, dataDir string, batchSize int) (db.Database, error) {
	if convert.ParquetFilesExist(dataDir) {
		slog.Info("parquet files found", "dir", dataDir)
		return db.OpenParquet(dataDir)
	}

	if dbPath == "" {
		slog.Error("parquet files not found and no -db path for conversion", "dir", dataDir)
		os.Exit(1)
	}

	slog.Info("parquet files not found, converting from SQLite", "db", dbPath, "out", dataDir)
	cfg := convert.Config{BatchSize: batchSize}
	if err := convert.Run(dbPath, dataDir, cfg); err != nil {
		return nil, err
	}

	slog.Info("conversion complete, opening parquet backend")
	return db.OpenParquet(dataDir)
}
