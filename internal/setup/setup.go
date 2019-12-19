package setup

import (
	"fmt"
	"log"
	"os"

	"github.com/m-lab/go/logx"

	"github.com/m-lab/prometheus-bigquery-exporter/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/afero"
)

var fs = afero.NewOsFs()

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
		f.stat, err = fs.Stat(f.Name)
		// Return true on the first successful Stat(), or the error otherwise.
		logx.Debug.Println("Stat: first:", err == nil, err)
		return err == nil, err
	}
	curr, err := fs.Stat(f.Name)
	if err != nil {
		log.Printf("Failed to stat %q: %v", f.Name, err)
		return false, err
	}
	logx.Debug.Println("Stat: modtimes:", curr.ModTime(), f.stat.ModTime(), curr.ModTime().After(f.stat.ModTime()))
	if curr.ModTime().After(f.stat.ModTime()) {
		logx.Debug.Println("Stat: update:", curr)
		f.stat = curr
	}
	return curr.ModTime().After(f.stat.ModTime()), nil
}

// Register the given collector. If a collector was previously registered with
// this file, then it is unregistered first. If either registration or
// unregister fails, then the error is returned.
func (f *File) Register(c *sql.Collector) error {
	if f.c != nil {
		ok := prometheus.Unregister(f.c)
		logx.Debug.Println("Unregister:", f.c.String(), ok)
		if !ok {
			// This is a fatal error. If the
			return fmt.Errorf("Failed to unregister %q", f.Name)
		}
		f.c = nil
	}
	// Register runs c.Update().
	logx.Debug.Println("Register:", c.String())
	err := prometheus.Register(c)
	if err != nil {
		// While collector Update could fail transiently, this may be a fatal error.
		logx.Debug.Println("Register error:", err)
		return err
	}
	// Save the registered collector.
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
