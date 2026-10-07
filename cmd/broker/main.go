package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"streamforge/internal/broker"
	"streamforge/internal/config"
	"streamforge/internal/metadata"
	"streamforge/internal/server"
	"syscall"
	"time"
)

func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	c, e := config.Load()
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, done := context.WithTimeout(ctx, 30*time.Second)
	s, e := metadata.Open(startup, c.PostgresDSN, c.RedisAddr, c.LeaseTTL, c.Brokers)
	if e != nil {
		done()
		return e
	}
	defer s.Close()
	b, e := broker.New(startup, c, s)
	done()
	if e != nil {
		return e
	}
	defer b.Close()
	srv, e := server.Start(ctx, b)
	if e != nil {
		return e
	}
	defer srv.Stop()
	slog.Info("broker ready", "broker_id", c.ID, "address", c.Listen, "metrics", c.Metrics, "fsync", c.Sync)
	select {
	case <-ctx.Done():
		return nil
	case e := <-srv.Errs:
		return e
	}
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
