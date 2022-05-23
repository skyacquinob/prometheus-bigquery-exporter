package error

import (
	"fmt"
	"log"
	"strconv"

	"github.com/m-lab/prometheus-bigquery-exporter/internal/setup"
	//"github.com/m-lab/prometheus-bigquery-exporter/query"
	//"github.com/m-lab/prometheus-bigquery-exporter/sql"

	"cloud.google.com/go/bigquery"

	"github.com/prometheus/client_golang/prometheus"
)

type ExportError struct {
	Err   error
	File  *setup.File
	Fatal bool
}

func (r *ExportError) Error() string {
	return fmt.Sprintf("Failed to export metric %v - err: %v", r.File.Name, r.Err)
}

func exportErrorCount(errCount int, client *bigquery.Client) {
	prometheus.NewDesc("bqe_error_count", "Defines how many errors where throwed while collecting queries", []string{strconv.Itoa(errCount)}, nil)
	//runner := query.NewBQRunner(client)
	//query := "select " + strconv.Itoa(errCount) + " as value"
	//log.Println(query)
	//c := sql.NewCollector(runner, prometheus.CounterValue, "bqe_error_count", query)
	//prometheus.Register(c)
}

func HandleErrors(errList []ExportError, client *bigquery.Client) {
	errCounter := 0
	for _, err := range errList {
		if err.Err != nil {
			errCounter++
			log.Println("Error: ", err.Error())
		}
	}
	log.Println("Registering error count: ", errCounter)
	exportErrorCount(errCounter, client)
}
