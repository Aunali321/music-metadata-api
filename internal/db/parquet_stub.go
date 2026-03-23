//go:build !duckdb

package db

import "fmt"

// OpenParquet is a stub for builds without the duckdb build tag.
// Build with -tags duckdb to enable parquet support.
func OpenParquet(dir string) (*DB, error) {
	return nil, fmt.Errorf("parquet support not compiled in; rebuild with -tags duckdb")
}
