package api

import (
	"context"
	"embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"metadata-api/internal/db"
	"metadata-api/internal/models"
)

//go:embed openapi.yaml
var openapiSpec embed.FS

type Handler struct {
	db db.Database
}

func New(database db.Database) *Handler {
	return &Handler{db: database}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /batch/lookup", h.batchLookup)
	mux.HandleFunc("GET /lookup/isrc/{isrc}", h.lookupISRC)
	mux.HandleFunc("GET /lookup/track/{id}", h.lookupTrack)
	mux.HandleFunc("GET /lookup/artist/{id}", h.lookupArtist)
	mux.HandleFunc("GET /lookup/album/{id}", h.lookupAlbum)
	mux.HandleFunc("GET /lookup/album/{id}/tracks", h.albumTracks)
	mux.HandleFunc("GET /search/artist", h.searchArtist)
	mux.HandleFunc("GET /search/track", h.searchTrack)
	mux.HandleFunc("GET /health", h.health)

	mux.HandleFunc("GET /openapi.yaml", h.openapiSpec)
	mux.HandleFunc("GET /docs", h.swaggerUI)
	mux.HandleFunc("GET /", h.swaggerUI)

	return mux
}

func (h *Handler) openapiSpec(w http.ResponseWriter, r *http.Request) {
	data, err := openapiSpec.ReadFile("openapi.yaml")
	if err != nil {
		http.Error(w, "spec not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/yaml")
	w.Write(data)
}

func (h *Handler) swaggerUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
  <title>Music Metadata API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    body { margin: 0; }
    .swagger-ui .topbar { display: none; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: '/openapi.yaml',
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: 'BaseLayout'
    });
  </script>
</body>
</html>`))
}

func (h *Handler) lookupISRC(w http.ResponseWriter, r *http.Request) {
	isrc := r.PathValue("isrc")
	if isrc == "" {
		http.Error(w, "isrc required", http.StatusBadRequest)
		return
	}

	tracks, err := h.db.LookupISRC(r.Context(), isrc)
	if err != nil {
		slog.Error("lookup isrc", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, tracks)
}

func (h *Handler) lookupTrack(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id required", http.StatusBadRequest)
		return
	}

	track, err := h.db.LookupTrack(r.Context(), id)
	if err != nil {
		slog.Error("lookup track", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if track == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, track)
}

func (h *Handler) lookupArtist(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id required", http.StatusBadRequest)
		return
	}

	artist, err := h.db.LookupArtist(r.Context(), id)
	if err != nil {
		slog.Error("lookup artist", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if artist == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, artist)
}

func (h *Handler) lookupAlbum(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id required", http.StatusBadRequest)
		return
	}

	album, err := h.db.LookupAlbum(r.Context(), id)
	if err != nil {
		slog.Error("lookup album", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if album == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, album)
}

func (h *Handler) albumTracks(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id required", http.StatusBadRequest)
		return
	}

	tracks, err := h.db.GetAlbumTracks(r.Context(), id)
	if err != nil {
		slog.Error("album tracks", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, tracks)
}

func (h *Handler) searchArtist(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "q parameter required", http.StatusBadRequest)
		return
	}

	// Validate minimum query length to prevent expensive searches
	if len(q) < 2 {
		http.Error(w, "query must be at least 2 characters", http.StatusBadRequest)
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	// Add timeout for search queries
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	artists, err := h.db.SearchArtist(ctx, q, limit)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "search timeout - try a more specific query", http.StatusRequestTimeout)
			return
		}
		slog.Error("search artist", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, artists)
}

func (h *Handler) searchTrack(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "q parameter required", http.StatusBadRequest)
		return
	}

	// Validate minimum query length to prevent expensive searches
	if len(q) < 2 {
		http.Error(w, "query must be at least 2 characters", http.StatusBadRequest)
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil {
			limit = parsed
		}
	}

	// Add timeout for search queries
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	tracks, err := h.db.SearchTrack(ctx, q, limit)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "search timeout - try a more specific query", http.StatusRequestTimeout)
			return
		}
		slog.Error("search track", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, tracks)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) batchLookup(w http.ResponseWriter, r *http.Request) {
	var req models.BatchLookupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request limits (max 100 items per type)
	totalItems := len(req.Tracks) + len(req.Artists) + len(req.Albums) + len(req.ISRCs)
	if totalItems == 0 {
		http.Error(w, "at least one lookup type required", http.StatusBadRequest)
		return
	}
	if totalItems > 400 {
		http.Error(w, "maximum 400 total items allowed", http.StatusBadRequest)
		return
	}

	resp := models.BatchLookupResponse{
		Errors: make(map[string]string),
	}

	if len(req.Tracks) > 0 {
		tracks, err := h.db.BatchLookupTracks(r.Context(), req.Tracks)
		if err != nil {
			slog.Error("batch lookup tracks", "err", err)
			resp.Errors["tracks"] = "failed to lookup some tracks"
		}
		resp.Tracks = tracks
	}

	if len(req.Artists) > 0 {
		artists, err := h.db.BatchLookupArtists(r.Context(), req.Artists)
		if err != nil {
			slog.Error("batch lookup artists", "err", err)
			resp.Errors["artists"] = "failed to lookup some artists"
		}
		resp.Artists = artists
	}

	if len(req.Albums) > 0 {
		albums, err := h.db.BatchLookupAlbums(r.Context(), req.Albums)
		if err != nil {
			slog.Error("batch lookup albums", "err", err)
			resp.Errors["albums"] = "failed to lookup some albums"
		}
		resp.Albums = albums
	}

	if len(req.ISRCs) > 0 {
		isrcs, err := h.db.BatchLookupISRCs(r.Context(), req.ISRCs)
		if err != nil {
			slog.Error("batch lookup isrcs", "err", err)
			resp.Errors["isrcs"] = "failed to lookup some isrcs"
		}
		resp.ISRCs = isrcs
	}

	// Remove errors field if empty
	if len(resp.Errors) == 0 {
		resp.Errors = nil
	}

	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode json", "err", err)
	}
}
