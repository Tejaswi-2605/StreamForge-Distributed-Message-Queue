package broker

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"streamforge/internal/metrics"
)

func TestAdmissionMetricsIncludeOverloadAndCanceledRequests(t *testing.T) {
	b := &Broker{slots: make(chan struct{}, 1), Metrics: new(metrics.Metrics)}
	info := &grpc.UnaryServerInfo{FullMethod: "/test/admission"}
	called := false
	handler := func(context.Context, interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	}
	b.slots <- struct{}{}
	_, err := b.Interceptor(context.Background(), nil, info, handler)
	if status.Code(err) != codes.ResourceExhausted || called {
		t.Fatalf("overload result: %v; called=%v", err, called)
	}
	if b.Metrics.Requests.Load() != 1 || b.Metrics.Errors.Load() != 1 {
		t.Fatal("overload was missing from request/error metrics")
	}
	<-b.slots
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Interceptor(ctx, nil, info, handler)
	if status.Code(err) != codes.Canceled || called || len(b.slots) != 0 {
		t.Fatalf("canceled request: %v; called=%v; slots=%d", err, called, len(b.slots))
	}
	if b.Metrics.Requests.Load() != 2 || b.Metrics.Errors.Load() != 2 {
		t.Fatal("canceled request was missing from metrics")
	}
	_, err = b.Interceptor(context.Background(), nil, info, handler)
	if err != nil || !called || len(b.slots) != 0 || b.Metrics.Requests.Load() != 3 || b.Metrics.Errors.Load() != 2 {
		t.Fatal("successful request metrics or slot release are incorrect")
	}
}

func TestHandlerFailureIsCountedOnceAndReleasesAdmission(t *testing.T) {
	b := &Broker{slots: make(chan struct{}, 1), Metrics: new(metrics.Metrics)}
	want := errors.New("handler failure")
	_, err := b.Interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test/fail"},
		func(context.Context, interface{}) (interface{}, error) { return nil, want })
	if !errors.Is(err, want) || b.Metrics.Requests.Load() != 1 || b.Metrics.Errors.Load() != 1 || len(b.slots) != 0 {
		t.Fatal("handler failure must be counted once without leaking admission slots")
	}
}
