// Command convert exports SQLite music metadata databases to Parquet files.
//
// Usage:
//
//	convert -db /path/to/main_database.sqlite3 -out /path/to/output/
//
// This reads the main_database.sqlite3 and track_files.sqlite3 (located in
// the same directory) and produces one .parquet file per table in the output
// directory. The output files can be served by the API server with the
// -backend parquet flag.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	_ "modernc.org/sqlite"
)

// Parquet row types — one per SQLite table. Fields must be exported and
// tagged for the parquet-go generic writer.

type Track struct {
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

type Album struct {
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

type Artist struct {
	RowID      int64  `parquet:"rowid"`
	ID         string `parquet:"id"`
	Name       string `parquet:"name"`
	Followers  int64  `parquet:"followers_total"`
	Popularity int32  `parquet:"popularity"`
}

type AlbumImage struct {
	AlbumRowID int64  `parquet:"album_rowid"`
	URL        string `parquet:"url"`
	Width      int32  `parquet:"width"`
	Height     int32  `parquet:"height"`
}

type ArtistImage struct {
	ArtistRowID int64  `parquet:"artist_rowid"`
	URL         string `parquet:"url"`
	Width       int32  `parquet:"width"`
	Height      int32  `parquet:"height"`
}

type TrackArtist struct {
	TrackRowID  int64 `parquet:"track_rowid"`
	ArtistRowID int64 `parquet:"artist_rowid"`
}

type ArtistAlbum struct {
	ArtistRowID  int64  `parquet:"artist_rowid"`
	AlbumRowID   int64  `parquet:"album_rowid"`
	IndexInAlbum *int32 `parquet:"index_in_album,optional"`
}

type ArtistGenre struct {
	ArtistRowID int64  `parquet:"artist_rowid"`
	Genre       string `parquet:"genre"`
}

type TrackFile struct {
	TrackID     string  `parquet:"track_id"`
	HasLyrics   *int32  `parquet:"has_lyrics,optional"`
	OrigTitle   *string `parquet:"original_title,optional"`
	VersionTitle *string `parquet:"version_title,optional"`
	Languages   *string `parquet:"language_of_performance,optional"`
	ArtistRoles *string `parquet:"artist_roles,optional"`
}

const batchSize = 100_000

func main() {
	var (
		dbPath = flag.String("db", "", "path to main_database.sqlite3")
		outDir = flag.String("out", "", "output directory for parquet files")
	)
	flag.Parse()

	if *dbPath == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "usage: convert -db <sqlite-path> -out <output-dir>")
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		slog.Error("create output dir", "err", err)
		os.Exit(1)
	}

	pragmas := "?mode=ro&_journal_mode=off&_query_only=true"

	mainDB, err := sql.Open("sqlite", *dbPath+pragmas)
	if err != nil {
		slog.Error("open main db", "err", err)
		os.Exit(1)
	}
	defer mainDB.Close()
	mainDB.SetMaxOpenConns(1)

	tfPath := filepath.Join(filepath.Dir(*dbPath), "track_files.sqlite3")
	tfDB, err := sql.Open("sqlite", tfPath+pragmas)
	if err != nil {
		slog.Error("open track_files db", "err", err)
		os.Exit(1)
	}
	defer tfDB.Close()
	tfDB.SetMaxOpenConns(1)

	// Export each table
	exporters := []struct {
		name string
		fn   func() error
	}{
		{"tracks", func() error { return exportTracks(mainDB, *outDir) }},
		{"albums", func() error { return exportAlbums(mainDB, *outDir) }},
		{"artists", func() error { return exportArtists(mainDB, *outDir) }},
		{"album_images", func() error { return exportAlbumImages(mainDB, *outDir) }},
		{"artist_images", func() error { return exportArtistImages(mainDB, *outDir) }},
		{"track_artists", func() error { return exportTrackArtists(mainDB, *outDir) }},
		{"artist_albums", func() error { return exportArtistAlbums(mainDB, *outDir) }},
		{"artist_genres", func() error { return exportArtistGenres(mainDB, *outDir) }},
		{"track_files", func() error { return exportTrackFiles(tfDB, *outDir) }},
	}

	for _, e := range exporters {
		slog.Info("exporting", "table", e.name)
		if err := e.fn(); err != nil {
			slog.Error("export failed", "table", e.name, "err", err)
			os.Exit(1)
		}
		slog.Info("done", "table", e.name)
	}

	slog.Info("all tables exported successfully", "dir", *outDir)
}

// nullStr converts sql.NullString to *string for parquet optional fields.
func nullStr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	return &ns.String
}

// nullInt32 converts sql.NullInt64 to *int32 for parquet optional fields.
func nullInt32(ni sql.NullInt64) *int32 {
	if !ni.Valid {
		return nil
	}
	v := int32(ni.Int64)
	return &v
}

// jsonStr converts a JSON-encoded value to a *string for storage as a parquet string.
// This preserves the original JSON encoding for language_of_performance and artist_roles.
func jsonStr(v interface{}) *string {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
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

func exportTracks(db *sql.DB, outDir string) error {
	w, f, err := createWriter[Track](outDir, "tracks")
	if err != nil {
		return err
	}
	defer f.Close()

	var lastRowID int64
	var total int64
	for {
		rows, err := db.Query(`
			SELECT rowid, id, name, external_id_isrc, duration_ms, explicit,
			       track_number, disc_number, popularity, preview_url, album_rowid
			FROM tracks WHERE rowid > ? ORDER BY rowid LIMIT ?
		`, lastRowID, batchSize)
		if err != nil {
			return fmt.Errorf("query tracks: %w", err)
		}

		count := 0
		batch := make([]Track, 0, batchSize)
		for rows.Next() {
			var t Track
			var isrc, preview sql.NullString
			if err := rows.Scan(&t.RowID, &t.ID, &t.Name, &isrc, &t.DurationMs,
				&t.Explicit, &t.TrackNum, &t.DiscNum, &t.Popularity, &preview, &t.AlbumRowID); err != nil {
				rows.Close()
				return fmt.Errorf("scan track: %w", err)
			}
			t.ISRC = nullStr(isrc)
			t.PreviewURL = nullStr(preview)
			batch = append(batch, t)
			lastRowID = t.RowID
			count++
		}
		rows.Close()

		if len(batch) > 0 {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write tracks: %w", err)
			}
			total += int64(len(batch))
		}

		if count < batchSize {
			break
		}
		slog.Info("tracks progress", "exported", total)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("tracks exported", "total", total)
	return nil
}

func exportAlbums(db *sql.DB, outDir string) error {
	w, f, err := createWriter[Album](outDir, "albums")
	if err != nil {
		return err
	}
	defer f.Close()

	var lastRowID int64
	var total int64
	for {
		rows, err := db.Query(`
			SELECT rowid, id, name, album_type, label, release_date, release_date_precision,
			       external_id_upc, total_tracks, copyright_c, copyright_p
			FROM albums WHERE rowid > ? ORDER BY rowid LIMIT ?
		`, lastRowID, batchSize)
		if err != nil {
			return fmt.Errorf("query albums: %w", err)
		}

		count := 0
		batch := make([]Album, 0, batchSize)
		for rows.Next() {
			var a Album
			var upc, copyC, copyP sql.NullString
			if err := rows.Scan(&a.RowID, &a.ID, &a.Name, &a.AlbumType, &a.Label,
				&a.ReleaseDate, &a.ReleaseDatePrecision, &upc, &a.TotalTracks, &copyC, &copyP); err != nil {
				rows.Close()
				return fmt.Errorf("scan album: %w", err)
			}
			a.UPC = nullStr(upc)
			a.CopyrightC = nullStr(copyC)
			a.CopyrightP = nullStr(copyP)
			batch = append(batch, a)
			lastRowID = a.RowID
			count++
		}
		rows.Close()

		if len(batch) > 0 {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write albums: %w", err)
			}
			total += int64(len(batch))
		}

		if count < batchSize {
			break
		}
		slog.Info("albums progress", "exported", total)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("albums exported", "total", total)
	return nil
}

func exportArtists(db *sql.DB, outDir string) error {
	w, f, err := createWriter[Artist](outDir, "artists")
	if err != nil {
		return err
	}
	defer f.Close()

	var lastRowID int64
	var total int64
	for {
		rows, err := db.Query(`
			SELECT rowid, id, name, followers_total, popularity
			FROM artists WHERE rowid > ? ORDER BY rowid LIMIT ?
		`, lastRowID, batchSize)
		if err != nil {
			return fmt.Errorf("query artists: %w", err)
		}

		count := 0
		batch := make([]Artist, 0, batchSize)
		for rows.Next() {
			var a Artist
			if err := rows.Scan(&a.RowID, &a.ID, &a.Name, &a.Followers, &a.Popularity); err != nil {
				rows.Close()
				return fmt.Errorf("scan artist: %w", err)
			}
			batch = append(batch, a)
			lastRowID = a.RowID
			count++
		}
		rows.Close()

		if len(batch) > 0 {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write artists: %w", err)
			}
			total += int64(len(batch))
		}

		if count < batchSize {
			break
		}
		slog.Info("artists progress", "exported", total)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("artists exported", "total", total)
	return nil
}

func exportAlbumImages(db *sql.DB, outDir string) error {
	w, f, err := createWriter[AlbumImage](outDir, "album_images")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`SELECT album_rowid, url, width, height FROM album_images`)
	if err != nil {
		return fmt.Errorf("query album_images: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]AlbumImage, 0, batchSize)
	for rows.Next() {
		var img AlbumImage
		if err := rows.Scan(&img.AlbumRowID, &img.URL, &img.Width, &img.Height); err != nil {
			return fmt.Errorf("scan album_image: %w", err)
		}
		batch = append(batch, img)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write album_images: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("album_images progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write album_images: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("album_images exported", "total", total)
	return nil
}

func exportArtistImages(db *sql.DB, outDir string) error {
	w, f, err := createWriter[ArtistImage](outDir, "artist_images")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`SELECT artist_rowid, url, width, height FROM artist_images`)
	if err != nil {
		return fmt.Errorf("query artist_images: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]ArtistImage, 0, batchSize)
	for rows.Next() {
		var img ArtistImage
		if err := rows.Scan(&img.ArtistRowID, &img.URL, &img.Width, &img.Height); err != nil {
			return fmt.Errorf("scan artist_image: %w", err)
		}
		batch = append(batch, img)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write artist_images: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("artist_images progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write artist_images: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("artist_images exported", "total", total)
	return nil
}

func exportTrackArtists(db *sql.DB, outDir string) error {
	w, f, err := createWriter[TrackArtist](outDir, "track_artists")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`SELECT track_rowid, artist_rowid FROM track_artists`)
	if err != nil {
		return fmt.Errorf("query track_artists: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]TrackArtist, 0, batchSize)
	for rows.Next() {
		var ta TrackArtist
		if err := rows.Scan(&ta.TrackRowID, &ta.ArtistRowID); err != nil {
			return fmt.Errorf("scan track_artist: %w", err)
		}
		batch = append(batch, ta)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write track_artists: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("track_artists progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write track_artists: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("track_artists exported", "total", total)
	return nil
}

func exportArtistAlbums(db *sql.DB, outDir string) error {
	w, f, err := createWriter[ArtistAlbum](outDir, "artist_albums")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`SELECT artist_rowid, album_rowid, index_in_album FROM artist_albums`)
	if err != nil {
		return fmt.Errorf("query artist_albums: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]ArtistAlbum, 0, batchSize)
	for rows.Next() {
		var aa ArtistAlbum
		var idx sql.NullInt64
		if err := rows.Scan(&aa.ArtistRowID, &aa.AlbumRowID, &idx); err != nil {
			return fmt.Errorf("scan artist_album: %w", err)
		}
		aa.IndexInAlbum = nullInt32(idx)
		batch = append(batch, aa)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write artist_albums: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("artist_albums progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write artist_albums: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("artist_albums exported", "total", total)
	return nil
}

func exportArtistGenres(db *sql.DB, outDir string) error {
	w, f, err := createWriter[ArtistGenre](outDir, "artist_genres")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`SELECT artist_rowid, genre FROM artist_genres`)
	if err != nil {
		return fmt.Errorf("query artist_genres: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]ArtistGenre, 0, batchSize)
	for rows.Next() {
		var ag ArtistGenre
		if err := rows.Scan(&ag.ArtistRowID, &ag.Genre); err != nil {
			return fmt.Errorf("scan artist_genre: %w", err)
		}
		batch = append(batch, ag)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write artist_genres: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("artist_genres progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write artist_genres: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("artist_genres exported", "total", total)
	return nil
}

func exportTrackFiles(db *sql.DB, outDir string) error {
	w, f, err := createWriter[TrackFile](outDir, "track_files")
	if err != nil {
		return err
	}
	defer f.Close()

	rows, err := db.Query(`
		SELECT track_id, has_lyrics, original_title, version_title,
		       language_of_performance, artist_roles
		FROM track_files
	`)
	if err != nil {
		return fmt.Errorf("query track_files: %w", err)
	}
	defer rows.Close()

	var total int64
	batch := make([]TrackFile, 0, batchSize)
	for rows.Next() {
		var tf TrackFile
		var hasLyrics sql.NullInt64
		var origTitle, versionTitle, langJSON, rolesJSON sql.NullString
		if err := rows.Scan(&tf.TrackID, &hasLyrics, &origTitle, &versionTitle, &langJSON, &rolesJSON); err != nil {
			return fmt.Errorf("scan track_file: %w", err)
		}
		tf.HasLyrics = nullInt32(hasLyrics)
		tf.OrigTitle = nullStr(origTitle)
		tf.VersionTitle = nullStr(versionTitle)
		tf.Languages = nullStr(langJSON)
		tf.ArtistRoles = nullStr(rolesJSON)
		batch = append(batch, tf)
		if len(batch) >= batchSize {
			if _, err := w.Write(batch); err != nil {
				return fmt.Errorf("write track_files: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
			slog.Info("track_files progress", "exported", total)
		}
	}
	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return fmt.Errorf("write track_files: %w", err)
		}
		total += int64(len(batch))
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	slog.Info("track_files exported", "total", total)
	return nil
}
