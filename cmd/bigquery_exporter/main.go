// bigquery_exporter runs structured bigquery SQL and converts the results into
// prometheus metrics. bigquery_exporter can process multiple queries.
// Because BigQuery queries can have long run times and high cost, Query results
// are cached and updated every refresh interval, not on every scrape of
// prometheus metrics.
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/prometheus-bigquery-exporter/internal/setup"
	"github.com/m-lab/prometheus-bigquery-exporter/query"
	"github.com/m-lab/prometheus-bigquery-exporter/sql"

	flag "github.com/spf13/pflag"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	counterSources = []string{}
	gaugeSources   = []string{}
	project        = flag.String("project", "", "GCP project name.")
	port           = flag.String("port", ":9050", "Exporter port.")
	refresh        = flag.Duration("refresh", 5*time.Minute, "Interval between updating metrics.")
)

func init() {
	flag.StringArrayVar(&counterSources, "counter-query", nil, "Name of file containing a counter query.")
	flag.StringArrayVar(&gaugeSources, "gauge-query", nil, "Name of file containing a gauge query.")

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// sleepUntilNext finds the nearest future time that is a multiple of the given
// duration and sleeps until that time.
func sleepUntilNext(d time.Duration) {
	next := time.Now().Truncate(d).Add(d)
	time.Sleep(time.Until(next))
}

// fileToMetric extracts the base file name to use as a prometheus metric name.
func fileToMetric(filename string) string {
	fname := filepath.Base(filename)
	return strings.TrimSuffix(fname, filepath.Ext(fname))
}

// fileToQuery reads the content of the given file and returns the query with template values repalced with those in vars.
func fileToQuery(filename string, vars map[string]string) string {
	queryBytes, err := ioutil.ReadFile(filename)
	rtx.Must(err, "Failed to open %q", filename)

	q := string(queryBytes)
	q = strings.Replace(q, "UNIX_START_TIME", vars["UNIX_START_TIME"], -1)
	q = strings.Replace(q, "REFRESH_RATE_SEC", vars["REFRESH_RATE_SEC"], -1)
	return q
}

func reloadRegisterUpdate(ctx context.Context, client *bigquery.Client, files []setup.File, vars map[string]string, refresh time.Duration) {
	for ctx.Err() == nil {
		for _, file := range files {
			modified, err := file.IsModified()
			if modified && err == nil {
				c := sql.NewCollector(
					query.NewBQRunner(client),
					prometheus.GaugeValue,
					fileToMetric(file.Name),
					fileToQuery(file.Name, vars))

				err = file.Register(c)
			} else {
				err = file.Update()
			}
			if err != nil {
				log.Println(err)
			}
		}
		sleepUntilNext(refresh)
	}
}

func main() {
	flag.Parse()

	files := make([]setup.File, len(gaugeSources))
	for i := range files {
		files[i].Name = gaugeSources[i]
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	rtx.Must(err, "Failed to allocate a new bigquery.Client")

	vars := map[string]string{
		"UNIX_START_TIME":  fmt.Sprintf("%d", time.Now().UTC().Unix()),
		"REFRESH_RATE_SEC": fmt.Sprintf("%d", int(refresh.Seconds())),
	}
	prometheusx.MustStartPrometheus(*port)
	reloadRegisterUpdate(ctx, client, files, vars, *refresh)
}
