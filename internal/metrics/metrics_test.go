package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrometheusCountersHistogram(t *testing.T) {
	m := new(Metrics)
	m.Published.Add(3)
	m.Observe(2*time.Millisecond, nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	s := w.Body.String()
	for _, v := range []string{"streamforge_messages_published_total 3", "streamforge_broker_request_duration_seconds_bucket{le=\"0.005\"} 1", "streamforge_broker_request_duration_seconds_count 1"} {
		if !strings.Contains(s, v) {
			t.Fatal(s)
		}
	}
}
