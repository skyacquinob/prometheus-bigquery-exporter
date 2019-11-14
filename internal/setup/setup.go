package setup

import (
	"fmt"
	"os"

	"github.com/m-lab/prometheus-bigquery-exporter/sql"
	"github.com/prometheus/client_golang/prometheus"
)

// File represents a query file and related metadata to keep it up to date and
// registered with the prometheus collector registry.
type File struct {
	Name string
	stat os.FileInfo
	c    *sql.Collector
}

// IsModified reports true if the file has been modified since the last call.
// The first call should almost always return false.
func (f *File) IsModified() (bool, error) {
	var err error
	if f.stat == nil {
		f.stat, err = os.Stat(f.Name)
		if err != nil {
			// TODO: what causes this?
			return false, err
		}
	}
	curr, err := os.Stat(f.Name)
	if err != nil {
		// TODO: best way to handle this?
		return false, err
	}
	return curr.ModTime().After(f.stat.ModTime()), nil
}

// Register the given collector. If a collector was previously registered with
// this file, then it is unregistered first. If either registration or unregister fails, then the error is returned.
// NOTE: it is possible
func (f *File) Register(c *sql.Collector) error {
	if f.c != nil {
		ok := prometheus.Unregister(f.c)
		if !ok {
			// TODO: how to handle this?
			return fmt.Errorf("Failed to unregister %q", f.Name)
		}
		f.c = nil
	}
	// Register runs c.Update().
	err := prometheus.Register(c)
	if err != nil {
		// TODO: how to handle this?
		return err
	}
	// Preserve the registration.
	f.c = c
	return nil
}

// Update runs the collector query again.
func (f *File) Update() error {
	if f.c != nil {
		return f.c.Update()
	}
	return nil
}
