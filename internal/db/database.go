package db

import (
	"context"

	"metadata-api/internal/models"
)

// Database defines the interface for querying music metadata.
// Both SQLite and Parquet/DuckDB backends implement this interface.
type Database interface {
	Close() error
	LookupISRC(ctx context.Context, isrc string) ([]models.Track, error)
	LookupTrack(ctx context.Context, id string) (*models.Track, error)
	LookupArtist(ctx context.Context, id string) (*models.Artist, error)
	LookupAlbum(ctx context.Context, id string) (*models.Album, error)
	GetAlbumTracks(ctx context.Context, albumID string) ([]models.Track, error)
	SearchArtist(ctx context.Context, query string, limit int) ([]models.Artist, error)
	SearchTrack(ctx context.Context, query string, limit int) ([]models.Track, error)
	BatchLookupTracks(ctx context.Context, ids []string) (map[string]*models.Track, error)
	BatchLookupArtists(ctx context.Context, ids []string) (map[string]*models.Artist, error)
	BatchLookupAlbums(ctx context.Context, ids []string) (map[string]*models.Album, error)
	BatchLookupISRCs(ctx context.Context, isrcs []string) (map[string][]models.Track, error)
}
