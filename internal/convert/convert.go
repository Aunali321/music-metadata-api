// Package convert provides SQLite-to-Parquet conversion for the music metadata database.
package convert

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	_ "modernc.org/sqlite"
)

// Config holds configuration for the conversion process.
type Config struct {
	BatchSize int // Rows per batch when reading from SQLite (default: 100000)
	Threads   int // Ignored for now; reserved for future parallel export
}

// DefaultConfig returns sensible defaults for conversion.
func DefaultConfig() Config {
	return Config{
		BatchSize: 100_000,
		Threads:   1,
	}
}

// Parquet row types — one per SQLite table.

type pqTrack struct {
	RowID      int64   `parquet:"rowid"`
	ID         string  `parquet:"id"`
	Name       string  `parquet:"name"`
	ISRC       *string `parquet:"external_id_isrc,optional"`
	DurationMs int64   `parquet:"duration_ms"`
	Explicit   bool    `parquet:"explicit"`
	TrackNum   int32   `parquet:"track_number"`
	DiscNum    int32   `parquet:"disc_number"`
	Popularity int32   `parquet:"popularity"`
	PreviewURL *string `parquet:"preview_url,optional"`
	AlbumRowID int64   `parquet:"album_rowid"`
}

type pqAlbum struct {
	RowID                int64   `parquet:"rowid"`
	ID                   string  `parquet:"id"`
	Name                 string  `parquet:"name"`
	AlbumType            string  `parquet:"album_type"`
	Label                string  `parquet:"label"`
	ReleaseDate          string  `parquet:"release_date"`
	ReleaseDatePrecision string  `parquet:"release_date_precision"`
	UPC                  *string `parquet:"external_id_upc,optional"`
	TotalTracks          int32   `parquet:"total_tracks"`
	CopyrightC           *string `parquet:"copyright_c,optional"`
	CopyrightP           *string `parquet:"copyright_p,optional"`
}

type pqArtist struct {
	RowID      int64  `parquet:"rowid"`
	ID         string `parquet:"id"`
	Name       string `parquet:"name"`
	Followers  int64  `parquet:"followers_total"`
	Popularity int32  `parquet:"popularity"`
}

type pqAlbumImage struct {
	AlbumRowID int64  `parquet:"album_rowid"`
	URL        string `parquet:"url"`
	Width      int32  `parquet:"width"`
	Height     int32  `parquet:"height"`
}

type pqArtistImage struct {
	ArtistRowID int64  `parquet:"artist_rowid"`
	URL         string `parquet:"url"`
	Width       int32  `parquet:"width"`
	Height      int32  `parquet:"height"`
}

type pqTrackArtist struct {
	TrackRowID  int64 `parquet:"track_rowid"`
	ArtistRowID int64 `parquet:"artist_rowid"`
}

type pqArtistAlbum struct {
	ArtistRowID  int64  `parquet:"artist_rowid"`
	AlbumRowID   int64  `parquet:"album_rowid"`
	IndexInAlbum *int32 `parquet:"index_in_album,optional"`
}

type pqArtistGenre struct {
	ArtistRowID int64  `parquet:"artist_rowid"`
	Genre       string `parquet:"genre"`
}

type pqTrackFile struct {
	TrackID      string  `parquet:"track_id"`
	HasLyrics    *int32  `parquet:"has_lyrics,optional"`
	OrigTitle    *string `parquet:"original_title,optional"`
	VersionTitle *string `parquet:"version_title,optional"`
	Languages    *string `parquet:"language_of_performance,optional"`
	ArtistRoles  *string `parquet:"artist_roles,optional"`
}

// Run converts SQLite databases at dbPath (and track_files.sqlite3 in the same
// directory) into Parquet files in outDir.
func Run(dbPath, outDir string, cfg Config) error {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultConfig().BatchSize
	}

	start := time.Now()
	slog.Info("starting conversion",
		"sqlite", dbPath,
		"output", outDir,
		"batch_size", cfg.BatchSize,
	)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	pragmas := "?mode=ro&_journal_mode=off&_query_only=true"

	mainDB, err := sql.Open("sqlite", dbPath+pragmas)
	if err != nil {
		return fmt.Errorf("open main db: %w", err)
	}
	defer mainDB.Close()
	mainDB.SetMaxOpenConns(1)

	// Export main database tables
	mainTables := []struct {
		name string
		fn   func(*sql.DB, string, int) error
	}{
		{"tracks", exportTracks},
		{"albums", exportAlbums},
		{"artists", exportArtists},
		{"album_images", exportAlbumImages},
		{"artist_images", exportArtistImages},
		{"track_artists", exportTrackArtists},
		{"artist_albums", exportArtistAlbums},
		{"artist_genres", exportArtistGenres},
	}

	for _, t := range mainTables {
		slog.Info("exporting", "table", t.name)
		if err := t.fn(mainDB, outDir, cfg.BatchSize); err != nil {
			return fmt.Errorf("export %s: %w", t.name, err)
		}
		slog.Info("done", "table", t.name)
	}

	// Export track_files from separate database
	tfPath := filepath.Join(filepath.Dir(dbPath), "track_files.sqlite3")
	if _, err := os.Stat(tfPath); err == nil {
		slog.Info("exporting", "table", "track_files")
		tfDB, err := sql.Open("sqlite", tfPath+pragmas)
		if err != nil {
			return fmt.Errorf("open track_files db: %w", err)
		}
		defer tfDB.Close()
		tfDB.SetMaxOpenConns(1)

		if err := exportTrackFiles(tfDB, outDir, cfg.BatchSize); err != nil {
			return fmt.Errorf("export track_files: %w", err)
		}
		slog.Info("done", "table", "track_files")
	} else {
		slog.Warn("track_files.sqlite3 not found, skipping", "path", tfPath)
	}

	slog.Info("conversion complete", "duration", time.Since(start).Round(time.Second))
	return nil
}

// ParquetFilesExist checks whether all required parquet files are present in dir.
func ParquetFilesExist(dir string) bool {
	required := []string{
		"tracks.parquet", "albums.parquet", "artists.parquet",
		"album_images.parquet", "artist_images.parquet",
		"track_artists.parquet", "artist_albums.parquet", "artist_genres.parquet",
	}
	for _, f := range required {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return false
		}
	}
	return true
}

func nullStr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	return &ns.String
}

func nullInt32(ni sql.NullInt64) *int32 {
	if !ni.Valid {
		return nil
	}
	v := int32(ni.Int64)
	return &v
}

func createWriter[T any](dir, name string) (*parquet.GenericWriter[T], *os.File, error) {
	path := filepath.Join(dir, name+".parquet")
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("create %s: %w", path, err)
	}
	w := parquet.NewGenericWriter[T](f,
		parquet.Compression(&snappy.Codec{}),
		parquet.CreatedBy("music-metadata-api", "1.0", ""),
	)
	return w, f, nil
}

// exportRowID exports a table that uses rowid-based pagination.
func exportRowID[T any](db *sql.DB, outDir, table, query string, batchSize int, scan func(*sql.Rows) (T, int64, error)) error {
	w, f, err := createWriter[T](outDir, table)
	if err != nil {
		return err
	}
	defer f.Close()

	var lastRowID int64
	var total int64
	for {
		rows, err := db.Query(query, lastRowID, batchSize)
		if err != nil {
			return fmt.Errorf("query %s: %w", table, err)
		}

		count := 0
		batch := make([]T, 0, batchSize)
		for rows.Next() {
			row, rowID, err := scan(rows)
			if err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
			lastRowID = rowID
			count++
		}
		rows.Close()

		if len(batch) > 0 {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write %s: %w", table, err)
			}
			total += int64(len(batch))
		}

		if count < batchSize {
			break
		}
		slog.Info("progress", "table", table, "exported", total)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("exported", "table", table, "total", total)
	return nil
}

// exportStream exports a table by streaming all rows (no rowid pagination).
func exportStream[T any](db *sql.DB, outDir, table, query string, batchSize int, scan func(*sql.Rows) (T, error)) error {
	w, f, err := createWriter[T](outDir, table)
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("query %s: %w", table, err)
	}
	defer rows.Close()

	var total int64
	batch := make([]T, 0, batchSize)
	for rows.Next() {
		row, err := scan(rows)
		if err != nil {
			return err
		}
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write %s: %w", table, err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("progress", "table", table, "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write %s: %w", table, err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("exported", "table", table, "total", total)
	return nil
}

func exportTracks(db *sql.DB, outDir string, batchSize int) error {
	return exportRowID[pqTrack](db, outDir, "tracks", `
		SELECT rowid, id, name, external_id_isrc, duration_ms, explicit,
		       track_number, disc_number, popularity, preview_url, album_rowid
		FROM tracks WHERE rowid > ? ORDER BY rowid LIMIT ?
	`, batchSize, func(rows *sql.Rows) (pqTrack, int64, error) {
		var t pqTrack
		var isrc, preview sql.NullString
		err := rows.Scan(&t.RowID, &t.ID, &t.Name, &isrc, &t.DurationMs,
			&t.Explicit, &t.TrackNum, &t.DiscNum, &t.Popularity, &preview, &t.AlbumRowID)
		t.ISRC = nullStr(isrc)
		t.PreviewURL = nullStr(preview)
		return t, t.RowID, err
	})
}

func exportAlbums(db *sql.DB, outDir string, batchSize int) error {
	return exportRowID[pqAlbum](db, outDir, "albums", `
		SELECT rowid, id, name, album_type, label, release_date, release_date_precision,
		       external_id_upc, total_tracks, copyright_c, copyright_p
		FROM albums WHERE rowid > ? ORDER BY rowid LIMIT ?
	`, batchSize, func(rows *sql.Rows) (pqAlbum, int64, error) {
		var a pqAlbum
		var upc, copyC, copyP sql.NullString
		err := rows.Scan(&a.RowID, &a.ID, &a.Name, &a.AlbumType, &a.Label,
			&a.ReleaseDate, &a.ReleaseDatePrecision, &upc, &a.TotalTracks, &copyC, &copyP)
		a.UPC = nullStr(upc)
		a.CopyrightC = nullStr(copyC)
		a.CopyrightP = nullStr(copyP)
		return a, a.RowID, err
	})
}

func exportArtists(db *sql.DB, outDir string, batchSize int) error {
	return exportRowID[pqArtist](db, outDir, "artists", `
		SELECT rowid, id, name, followers_total, popularity
		FROM artists WHERE rowid > ? ORDER BY rowid LIMIT ?
	`, batchSize, func(rows *sql.Rows) (pqArtist, int64, error) {
		var a pqArtist
		err := rows.Scan(&a.RowID, &a.ID, &a.Name, &a.Followers, &a.Popularity)
		return a, a.RowID, err
	})
}

func exportAlbumImages(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqAlbumImage](db, outDir, "album_images",
		`SELECT album_rowid, url, width, height FROM album_images`,
		batchSize, func(rows *sql.Rows) (pqAlbumImage, error) {
			var img pqAlbumImage
			err := rows.Scan(&img.AlbumRowID, &img.URL, &img.Width, &img.Height)
			return img, err
		})
}

func exportArtistImages(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqArtistImage](db, outDir, "artist_images",
		`SELECT artist_rowid, url, width, height FROM artist_images`,
		batchSize, func(rows *sql.Rows) (pqArtistImage, error) {
			var img pqArtistImage
			err := rows.Scan(&img.ArtistRowID, &img.URL, &img.Width, &img.Height)
			return img, err
		})
}

func exportTrackArtists(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqTrackArtist](db, outDir, "track_artists",
		`SELECT track_rowid, artist_rowid FROM track_artists`,
		batchSize, func(rows *sql.Rows) (pqTrackArtist, error) {
			var ta pqTrackArtist
			err := rows.Scan(&ta.TrackRowID, &ta.ArtistRowID)
			return ta, err
		})
}

func exportArtistAlbums(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqArtistAlbum](db, outDir, "artist_albums",
		`SELECT artist_rowid, album_rowid, index_in_album FROM artist_albums`,
		batchSize, func(rows *sql.Rows) (pqArtistAlbum, error) {
			var aa pqArtistAlbum
			var idx sql.NullInt64
			err := rows.Scan(&aa.ArtistRowID, &aa.AlbumRowID, &idx)
			aa.IndexInAlbum = nullInt32(idx)
			return aa, err
		})
}

func exportArtistGenres(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqArtistGenre](db, outDir, "artist_genres",
		`SELECT artist_rowid, genre FROM artist_genres`,
		batchSize, func(rows *sql.Rows) (pqArtistGenre, error) {
			var ag pqArtistGenre
			err := rows.Scan(&ag.ArtistRowID, &ag.Genre)
			return ag, err
		})
}

func exportTrackFiles(db *sql.DB, outDir string, batchSize int) error {
	return exportStream[pqTrackFile](db, outDir, "track_files", `
		SELECT track_id, has_lyrics, original_title, version_title,
		       language_of_performance, artist_roles
		FROM track_files
	`, batchSize, func(rows *sql.Rows) (pqTrackFile, error) {
		var tf pqTrackFile
		var hasLyrics sql.NullInt64
		var origTitle, versionTitle, langJSON, rolesJSON sql.NullString
		err := rows.Scan(&tf.TrackID, &hasLyrics, &origTitle, &versionTitle, &langJSON, &rolesJSON)
		tf.HasLyrics = nullInt32(hasLyrics)
		tf.OrigTitle = nullStr(origTitle)
		tf.VersionTitle = nullStr(versionTitle)
		tf.Languages = nullStr(langJSON)
		tf.ArtistRoles = nullStr(rolesJSON)
		return tf, err
	})
}
