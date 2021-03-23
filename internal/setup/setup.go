package setup

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/prometheus-bigquery-exporter/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

var (
	defaultCron = "* * * * *"
	fs          = afero.NewOsFs()
)

// File represents a query file and related metadata to keep it up to date and
// registered with the prometheus collector registry.
type File struct {
	Name       string
	stat       os.FileInfo
	c          *sql.Collector
	CronString string
	Kind       prometheus.ValueType
}

// IsModified reports true if the file has been modified since the last call.
// The first call should almost always return false.
func (f *File) IsModified() (bool, error) {
	var err error
	if f.stat == nil {
		f.stat, err = fs.Stat(f.Name)
		if err != nil {
			log.Printf("Failed to stat %q: %v", f.Name, err)
		}
		// Return true on the first successful Stat(), or the error otherwise.
		return err == nil, err
	}
	curr, err := fs.Stat(f.Name)
	if err != nil {
		log.Printf("Failed to stat %q: %v", f.Name, err)
		return false, err
	}
	logx.Debug.Println("IsModified:stat2:", f.Name, curr.ModTime(), f.stat.ModTime(),
		curr.ModTime().After(f.stat.ModTime()))
	modified := curr.ModTime().After(f.stat.ModTime())
	if modified {
		// Update the stat cache to the latest version.
		f.stat = curr
	}
	return modified, nil
}

// Register the given collector. If a collector was previously registered with
// this file, then it is unregistered first. If either registration or
// unregister fails, then the error is returned.
func (f *File) Register(c *sql.Collector) error {
	if f.c != nil {
		ok := prometheus.Unregister(f.c)
		logx.Debug.Println("Unregister:", ok)
		if !ok {
			// This is a fatal error. If the
			return fmt.Errorf("Failed to unregister %q", f.Name)
		}
		f.c = nil
	}
	// Register runs c.Update().
	err := prometheus.Register(c)
	if err != nil {
		// While collector Update could fail transiently, this may be a fatal error.
		return err
	}
	logx.Debug.Println("Register: success:", f.Name)
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

type Query struct {
	File string
	Kind string
	Cron string
}

type Configs struct {
	Queries []Query
}

func ReadConfig(configPath string) []File {
	if configPath != "" {
		configBytes, err := ioutil.ReadFile(configPath)
		rtx.Must(err, "Failed to open %q", configPath)
		configs := Configs{}
		err = yaml.Unmarshal(configBytes, &configs)
		rtx.Must(err, "Failed to parse yaml %q", configPath)
		Files := make([]File, len(configs.Queries))
		for i, config := range configs.Queries {
			log.Printf("Conifg fetched, file: %q, kind: %q, cron: %q",
				config.File, config.Kind, config.Cron)
			Files[i].Name = config.File
			Files[i].Kind = KindSelector(config.Kind)
			if config.Cron == "" {
				Files[i].CronString = defaultCron
			} else {
				Files[i].CronString = config.Cron
			}
		}
		return Files
	} else {
		return make([]File, 0)
	}
}

func KindSelector(kind string) prometheus.ValueType {
	if strings.ToLower(kind) == "gauge" {
		return prometheus.GaugeValue
	} else if strings.ToLower(kind) == "counter" {
		return prometheus.CounterValue
	} else {
		panic("'kind' string must be 'gauge' or 'counter'")
	}
}

func FilesFromSources(gaugeSources []string, counterSources []string) []File {
	Files := make([]File, len(gaugeSources)+len(counterSources))

	for i := range gaugeSources {
		Files[i].Name = gaugeSources[i]
		Files[i].Kind = prometheus.GaugeValue
		Files[i].CronString = defaultCron
	}

	for i := range counterSources {
		i_counter := i + len(gaugeSources)
		Files[i_counter].Name = gaugeSources[i]
		Files[i_counter].Kind = prometheus.GaugeValue
		Files[i_counter].CronString = defaultCron
	}
	return Files
}
