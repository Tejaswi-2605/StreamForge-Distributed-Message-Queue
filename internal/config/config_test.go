package config

import (
	"streamforge/internal/domain"
	"testing"
)

func TestConfigBounds(t *testing.T) {
	c := Default()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	c.IndexInterval = 0
	if e := c.Validate(); e == nil {
		t.Fatal("bad interval")
	}
}
func TestEnvironmentConfig(t *testing.T) {
	t.Setenv("MAX_MESSAGE_SIZE", "2048")
	t.Setenv("FSYNC_MODE", "write")
	c, e := Load()
	if e != nil || c.MaxMessage != 2048 || c.Sync {
		t.Fatalf("%v %v", c, e)
	}
	t.Setenv("FSYNC_MODE", "unknown")
	if _, e = Load(); e == nil {
		t.Fatal("bad mode")
	}
}

func TestPayloadTransportBounds(t *testing.T) {
	c := Default()
	c.MaxMessage = domain.MaxPayloadBytes
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	if c.MaxMessage+domain.RecordHeadroom > domain.FetchBytes || domain.FetchBytes >= domain.MaxResponseBytes || c.MaxMessage+(8<<10) > domain.MaxRequestBytes {
		t.Fatal("maximum legal message exceeds a shared wire/read budget")
	}
	c.MaxMessage++
	if e := c.Validate(); e == nil {
		t.Fatal("payload exceeding shared maximum accepted")
	}
}

func TestRetentionConfig(t *testing.T) {
	t.Setenv("RETENTION_MAX_SEGMENTS", "3")
	t.Setenv("RETENTION_INTERVAL", "25ms")
	c, e := Load()
	if e != nil || c.RetentionSegments != 3 || c.RetentionInterval.String() != "25ms" {
		t.Fatal(c, e)
	}
	c.RetentionSegments = -1
	if e := c.Validate(); e == nil {
		t.Fatal("negative retention accepted")
	}
	c.RetentionSegments = 2
	c.RetentionInterval = 0
	if e := c.Validate(); e == nil {
		t.Fatal("zero retention interval accepted")
	}
}
