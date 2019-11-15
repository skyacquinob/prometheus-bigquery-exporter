// bigquery_exporter runs structured bigquery SQL and converts the results into
// prometheus metrics. bigquery_exporter can process multiple queries.
// Because BigQuery queries can have long run times and high cost, Query results
// are cached and updated every refresh interval, not on every scrape of
// prometheus metrics.
package main

import (
	"context"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/prometheus-bigquery-exporter/sql"
)

func init() {
	log.SetOutput(ioutil.Discard)
}

type fakeRunner struct {
	updated int
}

func (f *fakeRunner) Query(query string) ([]sql.Metric, error) {
	r := []sql.Metric{
		{
			LabelKeys:   []string{"key"},
			LabelValues: []string{"value"},
			Values: map[string]float64{
				"okay": 1.23,
			},
		},
	}
	f.updated++
	return r, nil
}

func Test_main(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "success",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeRunner{}
			newRunner = func(*bigquery.Client) sql.QueryRunner {
				return f
			}
			*refresh = time.Second
			gaugeSources.Set("testdata/test.query")
			mainCtx, mainCancel = context.WithTimeout(mainCtx, time.Second)
			defer mainCancel()
			main()
			if f.updated != 2 {
				t.Errorf("main() failed to update; got %d, want 2", f.updated)
			}
		})
	}
}

func Test_defaultRunner(t *testing.T) {
	defaultRunner(nil)
}
