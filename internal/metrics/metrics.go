package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct {
	Published, Fetched, PublishErrors, FetchErrors, Bytes, Commits, Retries, DLQ, Requests, Errors, Nanos atomic.Int64
	Buckets                                                                                               [8]atomic.Int64
}

var bounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1}

func (m *Metrics) Observe(d time.Duration, err error) {
	m.Requests.Add(1)
	m.Nanos.Add(int64(d))
	if err != nil {
		m.Errors.Add(1)
	}
	s := d.Seconds()
	for i, b := range bounds {
		if s <= b {
			m.Buckets[i].Add(1)
		}
	}
}
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	counters := []struct {
		name string
		v    *atomic.Int64
	}{{"messages_published_total", &m.Published}, {"messages_fetched_total", &m.Fetched}, {"publish_errors_total", &m.PublishErrors}, {"fetch_errors_total", &m.FetchErrors}, {"bytes_written_total", &m.Bytes}, {"consumer_offset_commits_total", &m.Commits}, {"retry_messages_total", &m.Retries}, {"dlq_messages_total", &m.DLQ}, {"broker_requests_total", &m.Requests}, {"broker_errors_total", &m.Errors}}
	for _, c := range counters {
		fmt.Fprintf(w, "# TYPE streamforge_%s counter\nstreamforge_%s %d\n", c.name, c.name, c.v.Load())
	}
	fmt.Fprintln(w, "# TYPE streamforge_broker_request_duration_seconds histogram")
	for i, b := range bounds {
		fmt.Fprintf(w, "streamforge_broker_request_duration_seconds_bucket{le=\"%g\"} %d\n", b, m.Buckets[i].Load())
	}
	fmt.Fprintf(w, "streamforge_broker_request_duration_seconds_bucket{le=\"+Inf\"} %d\nstreamforge_broker_request_duration_seconds_sum %g\nstreamforge_broker_request_duration_seconds_count %d\n", m.Requests.Load(), float64(m.Nanos.Load())/1e9, m.Requests.Load())
}
