package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"metadata-api/internal/models"

	_ "modernc.org/sqlite"
)

type DB struct {
	main       *sql.DB
	trackFiles *sql.DB
	backend    string // "sqlite" or "parquet"
}

// likeNoCase returns the SQL clause for case-insensitive LIKE matching.
// SQLite uses "LIKE ? COLLATE NOCASE", DuckDB uses "ILIKE ?".
func (d *DB) likeNoCase() string {
	if d.backend == "parquet" {
		return "ILIKE ?"
	}
	return "LIKE ? COLLATE NOCASE"
}

func Open(dbPath string) (*DB, error) {
	// Conservative PRAGMAs for NAS: 64MB cache, 1GB mmap
	pragmas := "?mode=ro&_journal_mode=off&_cache_size=-65536&_mmap_size=1073741824&_query_only=true"

	main, err := sql.Open("sqlite", dbPath+pragmas)
	if err != nil {
		return nil, fmt.Errorf("open main db: %w", err)
	}
	main.SetMaxOpenConns(8)

	dir := filepath.Dir(dbPath)
	trackFilesPath := filepath.Join(dir, "track_files.sqlite3")
	trackFiles, err := sql.Open("sqlite", trackFilesPath+pragmas)
	if err != nil {
		main.Close()
		return nil, fmt.Errorf("open track_files db: %w", err)
	}
	trackFiles.SetMaxOpenConns(8)

	return &DB{main: main, trackFiles: trackFiles, backend: "sqlite"}, nil
}

func (d *DB) Close() error {
	d.trackFiles.Close()
	return d.main.Close()
}

func (d *DB) LookupISRC(ctx context.Context, isrc string) ([]models.Track, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT t.id, t.name, t.external_id_isrc, t.duration_ms, t.explicit,
		       t.track_number, t.disc_number, t.popularity, t.preview_url,
		       a.id, a.name, a.album_type, a.label, a.release_date, a.release_date_precision,
		       a.external_id_upc, a.total_tracks, a.copyright_c, a.copyright_p, a.rowid
		FROM tracks t
		JOIN albums a ON t.album_rowid = a.rowid
		WHERE t.external_id_isrc = ?
		ORDER BY t.popularity DESC
	`, isrc)
	if err != nil {
		return nil, fmt.Errorf("query isrc: %w", err)
	}
	defer rows.Close()

	var tracks []models.Track
	for rows.Next() {
		t, err := d.scanTrackWithAlbum(ctx, rows)
		if err != nil {
			return nil, err
		}
		tracks = append(tracks, *t)
	}
	return tracks, rows.Err()
}

func (d *DB) LookupTrack(ctx context.Context, id string) (*models.Track, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT t.id, t.name, t.external_id_isrc, t.duration_ms, t.explicit,
		       t.track_number, t.disc_number, t.popularity, t.preview_url,
		       a.id, a.name, a.album_type, a.label, a.release_date, a.release_date_precision,
		       a.external_id_upc, a.total_tracks, a.copyright_c, a.copyright_p, a.rowid
		FROM tracks t
		JOIN albums a ON t.album_rowid = a.rowid
		WHERE t.id = ?
	`, id)
	if err != nil {
		return nil, fmt.Errorf("query track: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}
	return d.scanTrackWithAlbum(ctx, rows)
}

func (d *DB) scanTrackWithAlbum(ctx context.Context, rows *sql.Rows) (*models.Track, error) {
	var t models.Track
	var alb models.Album
	var isrcNull, upcNull, copyCNull, copyPNull, previewNull sql.NullString
	var albumRowID int64

	err := rows.Scan(
		&t.ID, &t.Name, &isrcNull, &t.DurationMs, &t.Explicit,
		&t.TrackNum, &t.DiscNum, &t.Popularity, &previewNull,
		&alb.ID, &alb.Name, &alb.Type, &alb.Label, &alb.ReleaseDate, &alb.ReleaseDatePrecision,
		&upcNull, &alb.TotalTracks, &copyCNull, &copyPNull, &albumRowID,
	)
	if err != nil {
		return nil, fmt.Errorf("scan track: %w", err)
	}

	t.ISRC = isrcNull.String
	t.PreviewURL = previewNull.String
	alb.UPC = upcNull.String
	alb.CopyrightC = copyCNull.String
	alb.CopyrightP = copyPNull.String

	albumImages, err := d.getAlbumImages(ctx, albumRowID)
	if err != nil {
		slog.Error("get album images", "err", err, "rowid", albumRowID)
	}
	alb.Images = albumImages

	albumArtists, err := d.getAlbumArtists(ctx, albumRowID)
	if err != nil {
		slog.Error("get album artists", "err", err, "rowid", albumRowID)
	}
	alb.Artists = albumArtists

	t.Album = &alb

	artists, _ := d.getTrackArtists(ctx, t.ID)
	t.Artists = artists

	d.enrichTrackFromFiles(ctx, &t)

	return &t, nil
}

func (d *DB) enrichTrackFromFiles(ctx context.Context, t *models.Track) {
	row := d.trackFiles.QueryRowContext(ctx, `
		SELECT has_lyrics, original_title, version_title, language_of_performance, artist_roles
		FROM track_files WHERE track_id = ?
	`, t.ID)

	var hasLyrics sql.NullInt64
	var origTitle, versionTitle, langJSON, rolesJSON sql.NullString

	if err := row.Scan(&hasLyrics, &origTitle, &versionTitle, &langJSON, &rolesJSON); err != nil {
		return
	}

	if hasLyrics.Valid {
		val := hasLyrics.Int64 == 1
		t.HasLyrics = &val
	}
	t.OriginalTitle = origTitle.String
	t.VersionTitle = versionTitle.String

	if langJSON.String != "" {
		json.Unmarshal([]byte(langJSON.String), &t.Languages)
	}
	if rolesJSON.String != "" {
		json.Unmarshal([]byte(rolesJSON.String), &t.ArtistRoles)
	}
}

func (d *DB) LookupArtist(ctx context.Context, id string) (*models.Artist, error) {
	row := d.main.QueryRowContext(ctx, `
		SELECT id, name, followers_total, popularity, rowid FROM artists WHERE id = ?
	`, id)

	var a models.Artist
	var rowid int64
	err := row.Scan(&a.ID, &a.Name, &a.Followers, &a.Popularity, &rowid)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan artist: %w", err)
	}

	a.Genres, _ = d.getArtistGenres(ctx, rowid)
	images, err := d.getArtistImages(ctx, rowid)
	if err != nil {
		slog.Error("get artist images", "err", err, "rowid", rowid)
	}
	a.Images = images

	return &a, nil
}

func (d *DB) LookupAlbum(ctx context.Context, id string) (*models.Album, error) {
	row := d.main.QueryRowContext(ctx, `
		SELECT id, name, album_type, label, release_date, release_date_precision,
		       external_id_upc, total_tracks, copyright_c, copyright_p, rowid
		FROM albums WHERE id = ?
	`, id)

	var a models.Album
	var upcNull, copyCNull, copyPNull sql.NullString
	var rowid int64

	err := row.Scan(&a.ID, &a.Name, &a.Type, &a.Label, &a.ReleaseDate, &a.ReleaseDatePrecision,
		&upcNull, &a.TotalTracks, &copyCNull, &copyPNull, &rowid)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan album: %w", err)
	}

	a.UPC = upcNull.String
	a.CopyrightC = copyCNull.String
	a.CopyrightP = copyPNull.String
	a.Images, _ = d.getAlbumImages(ctx, rowid)
	a.Artists, _ = d.getAlbumArtists(ctx, rowid)

	return &a, nil
}

func (d *DB) GetAlbumTracks(ctx context.Context, albumID string) ([]models.Track, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT t.id, t.name, t.external_id_isrc, t.duration_ms, t.explicit,
		       t.track_number, t.disc_number, t.popularity, t.preview_url
		FROM tracks t
		JOIN albums a ON t.album_rowid = a.rowid
		WHERE a.id = ?
		ORDER BY t.disc_number, t.track_number
	`, albumID)
	if err != nil {
		return nil, fmt.Errorf("get album tracks: %w", err)
	}
	defer rows.Close()

	var tracks []models.Track
	for rows.Next() {
		var t models.Track
		var isrcNull, previewNull sql.NullString
		err := rows.Scan(&t.ID, &t.Name, &isrcNull, &t.DurationMs, &t.Explicit,
			&t.TrackNum, &t.DiscNum, &t.Popularity, &previewNull)
		if err != nil {
			return nil, fmt.Errorf("scan track: %w", err)
		}
		t.ISRC = isrcNull.String
		t.PreviewURL = previewNull.String

		artists, _ := d.getTrackArtists(ctx, t.ID)
		t.Artists = artists

		d.enrichTrackFromFiles(ctx, &t)

		tracks = append(tracks, t)
	}
	return tracks, rows.Err()
}

func (d *DB) SearchArtist(ctx context.Context, query string, limit int) ([]models.Artist, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	// Use case-insensitive substring search with LIMIT for safety
	rows, err := d.main.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, name, followers_total, popularity, rowid FROM artists
		WHERE name %s
		ORDER BY followers_total DESC
		LIMIT ?
	`, d.likeNoCase()), "%"+query+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("search artist: %w", err)
	}
	defer rows.Close()

	var artists []models.Artist
	for rows.Next() {
		var a models.Artist
		var rowid int64
		if err := rows.Scan(&a.ID, &a.Name, &a.Followers, &a.Popularity, &rowid); err != nil {
			return nil, fmt.Errorf("scan artist: %w", err)
		}
		a.Genres, _ = d.getArtistGenres(ctx, rowid)
		a.Images, _ = d.getArtistImages(ctx, rowid)
		artists = append(artists, a)
	}
	return artists, rows.Err()
}

func (d *DB) SearchTrack(ctx context.Context, query string, limit int) ([]models.Track, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	// Use case-insensitive substring search with LIMIT for safety
	rows, err := d.main.QueryContext(ctx, fmt.Sprintf(`
		SELECT t.id, t.name, t.external_id_isrc, t.duration_ms, t.explicit,
		       t.track_number, t.disc_number, t.popularity, t.preview_url,
		       a.id, a.name, a.album_type, a.label, a.release_date, a.release_date_precision,
		       a.external_id_upc, a.total_tracks, a.copyright_c, a.copyright_p, a.rowid
		FROM tracks t
		JOIN albums a ON t.album_rowid = a.rowid
		WHERE t.name %s
		ORDER BY t.popularity DESC
		LIMIT ?
	`, d.likeNoCase()), "%"+query+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("search track: %w", err)
	}
	defer rows.Close()

	var tracks []models.Track
	for rows.Next() {
		t, err := d.scanTrackWithAlbum(ctx, rows)
		if err != nil {
			return nil, err
		}
		tracks = append(tracks, *t)
	}
	return tracks, rows.Err()
}

func (d *DB) getTrackArtists(ctx context.Context, trackID string) ([]models.Artist, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT a.id, a.name, a.followers_total, a.popularity, a.rowid
		FROM artists a
		JOIN track_artists ta ON a.rowid = ta.artist_rowid
		JOIN tracks t ON ta.track_rowid = t.rowid
		WHERE t.id = ?
	`, trackID)
	if err != nil {
		return nil, fmt.Errorf("get track artists: %w", err)
	}
	defer rows.Close()

	var artists []models.Artist
	for rows.Next() {
		var a models.Artist
		var rowid int64
		if err := rows.Scan(&a.ID, &a.Name, &a.Followers, &a.Popularity, &rowid); err != nil {
			return nil, fmt.Errorf("scan artist: %w", err)
		}
		a.Genres, _ = d.getArtistGenres(ctx, rowid)
		a.Images, _ = d.getArtistImages(ctx, rowid)
		artists = append(artists, a)
	}
	return artists, rows.Err()
}

func (d *DB) getAlbumArtists(ctx context.Context, albumRowID int64) ([]models.Artist, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT a.id, a.name, a.followers_total, a.popularity, a.rowid, MIN(aa.index_in_album) as idx
		FROM artists a
		JOIN artist_albums aa ON a.rowid = aa.artist_rowid
		WHERE aa.album_rowid = ? AND aa.index_in_album IS NOT NULL
		GROUP BY a.id
		ORDER BY idx
	`, albumRowID)
	if err != nil {
		return nil, fmt.Errorf("get album artists: %w", err)
	}
	defer rows.Close()

	var artists []models.Artist
	for rows.Next() {
		var a models.Artist
		var rowid int64
		var idx int
		if err := rows.Scan(&a.ID, &a.Name, &a.Followers, &a.Popularity, &rowid, &idx); err != nil {
			return nil, fmt.Errorf("scan artist: %w", err)
		}
		a.Genres, _ = d.getArtistGenres(ctx, rowid)
		a.Images, _ = d.getArtistImages(ctx, rowid)
		artists = append(artists, a)
	}
	return artists, rows.Err()
}

func (d *DB) getArtistGenres(ctx context.Context, artistRowID int64) ([]string, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT genre FROM artist_genres WHERE artist_rowid = ?
	`, artistRowID)
	if err != nil {
		return nil, fmt.Errorf("get artist genres: %w", err)
	}
	defer rows.Close()

	var genres []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, fmt.Errorf("scan genre: %w", err)
		}
		genres = append(genres, g)
	}
	return genres, rows.Err()
}

func (d *DB) getAlbumImages(ctx context.Context, albumRowID int64) ([]models.Image, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT DISTINCT url, width, height FROM album_images
		WHERE album_rowid = ? ORDER BY width DESC
	`, albumRowID)
	if err != nil {
		return nil, fmt.Errorf("get album images: %w", err)
	}
	defer rows.Close()

	var images []models.Image
	for rows.Next() {
		var img models.Image
		if err := rows.Scan(&img.URL, &img.Width, &img.Height); err != nil {
			return nil, fmt.Errorf("scan image: %w", err)
		}
		images = append(images, img)
	}
	return images, rows.Err()
}

func (d *DB) getArtistImages(ctx context.Context, artistRowID int64) ([]models.Image, error) {
	rows, err := d.main.QueryContext(ctx, `
		SELECT url, width, height FROM artist_images
		WHERE artist_rowid = ? ORDER BY width DESC
	`, artistRowID)
	if err != nil {
		return nil, fmt.Errorf("get artist images: %w", err)
	}
	defer rows.Close()

	var images []models.Image
	for rows.Next() {
		var img models.Image
		if err := rows.Scan(&img.URL, &img.Width, &img.Height); err != nil {
			return nil, fmt.Errorf("scan image: %w", err)
		}
		images = append(images, img)
	}
	return images, rows.Err()
}

func (d *DB) BatchLookupTracks(ctx context.Context, ids []string) (map[string]*models.Track, error) {
	result := make(map[string]*models.Track)

	for _, id := range ids {
		track, err := d.LookupTrack(ctx, id)
		if err != nil {
			slog.Error("batch lookup track", "id", id, "err", err)
			continue
		}
		if track != nil {
			result[id] = track
		}
	}

	return result, nil
}

func (d *DB) BatchLookupArtists(ctx context.Context, ids []string) (map[string]*models.Artist, error) {
	result := make(map[string]*models.Artist)

	for _, id := range ids {
		artist, err := d.LookupArtist(ctx, id)
		if err != nil {
			slog.Error("batch lookup artist", "id", id, "err", err)
			continue
		}
		if artist != nil {
			result[id] = artist
		}
	}

	return result, nil
}

func (d *DB) BatchLookupAlbums(ctx context.Context, ids []string) (map[string]*models.Album, error) {
	result := make(map[string]*models.Album)

	for _, id := range ids {
		album, err := d.LookupAlbum(ctx, id)
		if err != nil {
			slog.Error("batch lookup album", "id", id, "err", err)
			continue
		}
		if album != nil {
			result[id] = album
		}
	}

	return result, nil
}

func (d *DB) BatchLookupISRCs(ctx context.Context, isrcs []string) (map[string][]models.Track, error) {
	if len(isrcs) == 0 {
		return make(map[string][]models.Track), nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(isrcs))
	args := make([]interface{}, len(isrcs))
	for i, isrc := range isrcs {
		placeholders[i] = "?"
		args[i] = isrc
	}
	inClause := strings.Join(placeholders, ",")

	// 1. Fetch all tracks + albums in one query
	query := fmt.Sprintf(`
		SELECT t.id, t.name, t.external_id_isrc, t.duration_ms, t.explicit,
		       t.track_number, t.disc_number, t.popularity, t.preview_url, t.rowid,
		       a.id, a.name, a.album_type, a.label, a.release_date, a.release_date_precision,
		       a.external_id_upc, a.total_tracks, a.copyright_c, a.copyright_p, a.rowid
		FROM tracks t
		JOIN albums a ON t.album_rowid = a.rowid
		WHERE t.external_id_isrc IN (%s)
		ORDER BY t.external_id_isrc, t.popularity DESC
	`, inClause)

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch query isrcs: %w", err)
	}
	defer rows.Close()

	type trackInfo struct {
		track      models.Track
		albumRowID int64
		trackRowID int64
	}

	var trackInfos []trackInfo
	albumRowIDs := make(map[int64]bool)
	trackIDs := make([]string, 0)

	for rows.Next() {
		var t models.Track
		var alb models.Album
		var isrcNull, upcNull, copyCNull, copyPNull, previewNull sql.NullString
		var albumRowID, trackRowID int64

		err := rows.Scan(
			&t.ID, &t.Name, &isrcNull, &t.DurationMs, &t.Explicit,
			&t.TrackNum, &t.DiscNum, &t.Popularity, &previewNull, &trackRowID,
			&alb.ID, &alb.Name, &alb.Type, &alb.Label, &alb.ReleaseDate, &alb.ReleaseDatePrecision,
			&upcNull, &alb.TotalTracks, &copyCNull, &copyPNull, &albumRowID,
		)
		if err != nil {
			return nil, fmt.Errorf("scan track: %w", err)
		}

		t.ISRC = isrcNull.String
		t.PreviewURL = previewNull.String
		alb.UPC = upcNull.String
		alb.CopyrightC = copyCNull.String
		alb.CopyrightP = copyPNull.String
		t.Album = &alb

		trackInfos = append(trackInfos, trackInfo{track: t, albumRowID: albumRowID, trackRowID: trackRowID})
		albumRowIDs[albumRowID] = true
		trackIDs = append(trackIDs, t.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(trackInfos) == 0 {
		return make(map[string][]models.Track), nil
	}

	// 2. Batch fetch album images
	albumImages, err := d.batchGetAlbumImages(ctx, albumRowIDs)
	if err != nil {
		slog.Error("batch get album images", "err", err)
	}

	// 3. Batch fetch album artists (and their artist rowids)
	albumArtists, artistRowIDs, err := d.batchGetAlbumArtists(ctx, albumRowIDs)
	if err != nil {
		slog.Error("batch get album artists", "err", err)
	}

	// 4. Batch fetch track artists (and their artist rowids)
	trackArtists, trackArtistRowIDs, err := d.batchGetTrackArtists(ctx, trackIDs)
	if err != nil {
		slog.Error("batch get track artists", "err", err)
	}

	// Merge artist rowids
	for rowid := range trackArtistRowIDs {
		artistRowIDs[rowid] = true
	}

	// 5. Batch fetch artist genres and images
	artistGenres, err := d.batchGetArtistGenres(ctx, artistRowIDs)
	if err != nil {
		slog.Error("batch get artist genres", "err", err)
	}
	artistImages, err := d.batchGetArtistImages(ctx, artistRowIDs)
	if err != nil {
		slog.Error("batch get artist images", "err", err)
	}

	// 6. Batch fetch track_files enrichment
	trackFilesData, err := d.batchEnrichTrackFiles(ctx, trackIDs)
	if err != nil {
		slog.Error("batch enrich track files", "err", err)
	}

	// Assemble results
	result := make(map[string][]models.Track)
	for i := range trackInfos {
		ti := &trackInfos[i]

		// Attach album images
		ti.track.Album.Images = albumImages[ti.albumRowID]

		// Attach album artists with genres/images
		if artists, ok := albumArtists[ti.albumRowID]; ok {
			for j := range artists {
				artists[j].Genres = artistGenres[artists[j].rowid]
				artists[j].Images = artistImages[artists[j].rowid]
			}
			ti.track.Album.Artists = toArtists(artists)
		}

		// Attach track artists with genres/images
		if artists, ok := trackArtists[ti.track.ID]; ok {
			for j := range artists {
				artists[j].Genres = artistGenres[artists[j].rowid]
				artists[j].Images = artistImages[artists[j].rowid]
			}
			ti.track.Artists = toArtists(artists)
		}

		// Attach track_files enrichment
		if tf, ok := trackFilesData[ti.track.ID]; ok {
			ti.track.HasLyrics = tf.HasLyrics
			ti.track.OriginalTitle = tf.OriginalTitle
			ti.track.VersionTitle = tf.VersionTitle
			ti.track.Languages = tf.Languages
			ti.track.ArtistRoles = tf.ArtistRoles
		}

		result[ti.track.ISRC] = append(result[ti.track.ISRC], ti.track)
	}

	return result, nil
}

// artistWithRowID holds artist data plus rowid for later lookups
type artistWithRowID struct {
	models.Artist
	rowid int64
}

func toArtists(awrs []artistWithRowID) []models.Artist {
	artists := make([]models.Artist, len(awrs))
	for i, a := range awrs {
		artists[i] = a.Artist
	}
	return artists
}

func (d *DB) batchGetAlbumImages(ctx context.Context, albumRowIDs map[int64]bool) (map[int64][]models.Image, error) {
	if len(albumRowIDs) == 0 {
		return make(map[int64][]models.Image), nil
	}

	placeholders := make([]string, 0, len(albumRowIDs))
	args := make([]interface{}, 0, len(albumRowIDs))
	for rowid := range albumRowIDs {
		placeholders = append(placeholders, "?")
		args = append(args, rowid)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT album_rowid, url, width, height FROM album_images
		WHERE album_rowid IN (%s) ORDER BY album_rowid, width DESC
	`, strings.Join(placeholders, ","))

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64][]models.Image)
	for rows.Next() {
		var rowid int64
		var img models.Image
		if err := rows.Scan(&rowid, &img.URL, &img.Width, &img.Height); err != nil {
			return nil, err
		}
		result[rowid] = append(result[rowid], img)
	}
	return result, rows.Err()
}

func (d *DB) batchGetAlbumArtists(ctx context.Context, albumRowIDs map[int64]bool) (map[int64][]artistWithRowID, map[int64]bool, error) {
	if len(albumRowIDs) == 0 {
		return make(map[int64][]artistWithRowID), make(map[int64]bool), nil
	}

	placeholders := make([]string, 0, len(albumRowIDs))
	args := make([]interface{}, 0, len(albumRowIDs))
	for rowid := range albumRowIDs {
		placeholders = append(placeholders, "?")
		args = append(args, rowid)
	}

	query := fmt.Sprintf(`
		SELECT aa.album_rowid, a.id, a.name, a.followers_total, a.popularity, a.rowid, MIN(aa.index_in_album) as idx
		FROM artists a
		JOIN artist_albums aa ON a.rowid = aa.artist_rowid
		WHERE aa.album_rowid IN (%s) AND aa.index_in_album IS NOT NULL
		GROUP BY aa.album_rowid, a.id
		ORDER BY aa.album_rowid, idx
	`, strings.Join(placeholders, ","))

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[int64][]artistWithRowID)
	artistRowIDs := make(map[int64]bool)
	for rows.Next() {
		var albumRowID int64
		var a artistWithRowID
		var idx int
		if err := rows.Scan(&albumRowID, &a.ID, &a.Name, &a.Followers, &a.Popularity, &a.rowid, &idx); err != nil {
			return nil, nil, err
		}
		result[albumRowID] = append(result[albumRowID], a)
		artistRowIDs[a.rowid] = true
	}
	return result, artistRowIDs, rows.Err()
}

func (d *DB) batchGetTrackArtists(ctx context.Context, trackIDs []string) (map[string][]artistWithRowID, map[int64]bool, error) {
	if len(trackIDs) == 0 {
		return make(map[string][]artistWithRowID), make(map[int64]bool), nil
	}

	placeholders := make([]string, len(trackIDs))
	args := make([]interface{}, len(trackIDs))
	for i, id := range trackIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT t.id, a.id, a.name, a.followers_total, a.popularity, a.rowid
		FROM artists a
		JOIN track_artists ta ON a.rowid = ta.artist_rowid
		JOIN tracks t ON ta.track_rowid = t.rowid
		WHERE t.id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[string][]artistWithRowID)
	artistRowIDs := make(map[int64]bool)
	for rows.Next() {
		var trackID string
		var a artistWithRowID
		if err := rows.Scan(&trackID, &a.ID, &a.Name, &a.Followers, &a.Popularity, &a.rowid); err != nil {
			return nil, nil, err
		}
		result[trackID] = append(result[trackID], a)
		artistRowIDs[a.rowid] = true
	}
	return result, artistRowIDs, rows.Err()
}

func (d *DB) batchGetArtistGenres(ctx context.Context, artistRowIDs map[int64]bool) (map[int64][]string, error) {
	if len(artistRowIDs) == 0 {
		return make(map[int64][]string), nil
	}

	placeholders := make([]string, 0, len(artistRowIDs))
	args := make([]interface{}, 0, len(artistRowIDs))
	for rowid := range artistRowIDs {
		placeholders = append(placeholders, "?")
		args = append(args, rowid)
	}

	query := fmt.Sprintf(`
		SELECT artist_rowid, genre FROM artist_genres WHERE artist_rowid IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64][]string)
	for rows.Next() {
		var rowid int64
		var genre string
		if err := rows.Scan(&rowid, &genre); err != nil {
			return nil, err
		}
		result[rowid] = append(result[rowid], genre)
	}
	return result, rows.Err()
}

func (d *DB) batchGetArtistImages(ctx context.Context, artistRowIDs map[int64]bool) (map[int64][]models.Image, error) {
	if len(artistRowIDs) == 0 {
		return make(map[int64][]models.Image), nil
	}

	placeholders := make([]string, 0, len(artistRowIDs))
	args := make([]interface{}, 0, len(artistRowIDs))
	for rowid := range artistRowIDs {
		placeholders = append(placeholders, "?")
		args = append(args, rowid)
	}

	query := fmt.Sprintf(`
		SELECT artist_rowid, url, width, height FROM artist_images
		WHERE artist_rowid IN (%s) ORDER BY artist_rowid, width DESC
	`, strings.Join(placeholders, ","))

	rows, err := d.main.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64][]models.Image)
	for rows.Next() {
		var rowid int64
		var img models.Image
		if err := rows.Scan(&rowid, &img.URL, &img.Width, &img.Height); err != nil {
			return nil, err
		}
		result[rowid] = append(result[rowid], img)
	}
	return result, rows.Err()
}

type trackFileData struct {
	HasLyrics     *bool
	OriginalTitle string
	VersionTitle  string
	Languages     []string
	ArtistRoles   []string
}

func (d *DB) batchEnrichTrackFiles(ctx context.Context, trackIDs []string) (map[string]trackFileData, error) {
	if len(trackIDs) == 0 {
		return make(map[string]trackFileData), nil
	}

	placeholders := make([]string, len(trackIDs))
	args := make([]interface{}, len(trackIDs))
	for i, id := range trackIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT track_id, has_lyrics, original_title, version_title, language_of_performance, artist_roles
		FROM track_files WHERE track_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := d.trackFiles.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]trackFileData)
	for rows.Next() {
		var trackID string
		var hasLyrics sql.NullInt64
		var origTitle, versionTitle, langJSON, rolesJSON sql.NullString

		if err := rows.Scan(&trackID, &hasLyrics, &origTitle, &versionTitle, &langJSON, &rolesJSON); err != nil {
			return nil, err
		}

		tf := trackFileData{
			OriginalTitle: origTitle.String,
			VersionTitle:  versionTitle.String,
		}
		if hasLyrics.Valid {
			val := hasLyrics.Int64 == 1
			tf.HasLyrics = &val
		}
		if langJSON.String != "" {
			json.Unmarshal([]byte(langJSON.String), &tf.Languages)
		}
		if rolesJSON.String != "" {
			json.Unmarshal([]byte(rolesJSON.String), &tf.ArtistRoles)
		}
		result[trackID] = tf
	}
	return result, rows.Err()
}
