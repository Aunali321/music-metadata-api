//go:build duckdb

package db

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// OpenParquet opens a DuckDB in-memory database and creates views over parquet
// files in the given directory. The directory must contain parquet files exported
// by the convert tool: tracks.parquet, albums.parquet, artists.parquet,
// album_images.parquet, artist_images.parquet, track_artists.parquet,
// artist_albums.parquet, artist_genres.parquet, and track_files.parquet.
func OpenParquet(dir string) (*DB, error) {
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	conn.SetMaxOpenConns(8)

	// Create views for each parquet file
	tables := []string{
		"tracks",
		"albums",
		"artists",
		"album_images",
		"artist_images",
		"track_artists",
		"artist_albums",
		"artist_genres",
		"track_files",
	}

	for _, table := range tables {
		path := filepath.Join(dir, table+".parquet")
		query := fmt.Sprintf(
			`CREATE VIEW %s AS SELECT * FROM read_parquet('%s')`,
			table, path,
		)
		if _, err := conn.Exec(query); err != nil {
			conn.Close()
			return nil, fmt.Errorf("create view %s: %w", table, err)
		}
	}

	return &DB{main: conn, trackFiles: conn, backend: "parquet"}, nil
}
