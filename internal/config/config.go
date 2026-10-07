package config

import (
	"fmt"
	"os"
	"strconv"
	"streamforge/internal/domain"
	"strings"
	"time"
)

type Config struct {
	ID                                                                       int
	Listen, Metrics, DataDir, PostgresDSN, RedisAddr                         string
	Brokers                                                                  []domain.Broker
	MaxMessage, SegmentBytes, IndexInterval, MaxInflight, MaxBatch, MaxFetch int
	Sync                                                                     bool
	LeaseTTL                                                                 time.Duration
	MaxRetries                                                               int
	RetryBase, RetryMax                                                      time.Duration
	RetentionSegments                                                        int
	RetentionInterval                                                        time.Duration
}

func Default() Config {
	return Config{ID: 0, Listen: "127.0.0.1:9001", Metrics: "127.0.0.1:9101", DataDir: "data/brokers/0", PostgresDSN: "postgres://streamforge@127.0.0.1:5432/streamforge?sslmode=disable", RedisAddr: "127.0.0.1:6379", Brokers: []domain.Broker{{ID: 0, Address: "127.0.0.1:9001"}}, MaxMessage: 1 << 20, SegmentBytes: 16 << 20, IndexInterval: 64, MaxInflight: 128, MaxBatch: 100, MaxFetch: 100, Sync: true, LeaseTTL: 30 * time.Second, MaxRetries: 3, RetryBase: time.Second, RetryMax: time.Minute, RetentionInterval: time.Minute}
}
func Load() (Config, error) {
	c := Default()
	stringsEnv := map[string]*string{"BROKER_ADDRESS": &c.Listen, "METRICS_ADDRESS": &c.Metrics, "DATA_DIR": &c.DataDir, "POSTGRES_DSN": &c.PostgresDSN, "REDIS_ADDR": &c.RedisAddr}
	for k, p := range stringsEnv {
		if v := os.Getenv(k); v != "" {
			*p = v
		}
	}
	ints := map[string]*int{"BROKER_ID": &c.ID, "MAX_MESSAGE_SIZE": &c.MaxMessage, "SEGMENT_MAX_BYTES": &c.SegmentBytes, "INDEX_INTERVAL": &c.IndexInterval, "MAX_INFLIGHT_REQUESTS": &c.MaxInflight, "MAX_BATCH_SIZE": &c.MaxBatch, "MAX_FETCH_SIZE": &c.MaxFetch, "MAX_RETRIES": &c.MaxRetries}
	ints["RETENTION_MAX_SEGMENTS"] = &c.RetentionSegments
	for k, p := range ints {
		if v := os.Getenv(k); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			*p = n
		}
	}
	durations := map[string]*time.Duration{"LEASE_TTL": &c.LeaseTTL, "RETRY_BASE": &c.RetryBase, "RETRY_MAX": &c.RetryMax}
	durations["RETENTION_INTERVAL"] = &c.RetentionInterval
	for k, p := range durations {
		if v := os.Getenv(k); v != "" {
			d, e := time.ParseDuration(v)
			if e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			*p = d
		}
	}
	if v := os.Getenv("FSYNC_MODE"); v != "" {
		if v != "write" && v != "fsync" {
			return c, fmt.Errorf("FSYNC_MODE must be write or fsync")
		}
		c.Sync = v == "fsync"
	}
	if v := os.Getenv("BROKER_LIST"); v != "" {
		c.Brokers = nil
		for i, a := range strings.Split(v, ",") {
			c.Brokers = append(c.Brokers, domain.Broker{ID: i, Address: strings.TrimSpace(a)})
		}
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.RetentionSegments < 0 || c.RetentionSegments > 100000 || c.RetentionInterval <= 0 {
		return fmt.Errorf("invalid retention configuration: max segments must be 0..100000 and interval positive")
	}
	if c.ID < 0 || c.ID >= len(c.Brokers) || c.MaxMessage < 1 || c.MaxMessage > domain.MaxPayloadBytes || c.SegmentBytes < 256 || c.IndexInterval < 1 || c.MaxInflight < 1 || c.MaxBatch < 1 || c.MaxBatch > 1000 || c.MaxFetch < 1 || c.MaxFetch > 1000 || c.LeaseTTL < time.Second || c.MaxRetries < 0 || c.MaxRetries > 30 || c.RetryBase <= 0 || c.RetryMax < c.RetryBase {
		return fmt.Errorf("invalid configuration bounds")
	}
	for i, b := range c.Brokers {
		if b.ID != i || b.Address == "" {
			return fmt.Errorf("invalid broker list")
		}
	}
	return nil
}
