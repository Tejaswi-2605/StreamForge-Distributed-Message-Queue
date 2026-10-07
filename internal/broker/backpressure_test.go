package broker

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"streamforge/internal/metrics"
	"testing"
)

func TestBoundedAdmission(t *testing.T) {
	b := &Broker{slots: make(chan struct{}, 1), Metrics: new(metrics.Metrics)}
	b.slots <- struct{}{}
	called := false
	_, e := b.Interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "test"}, func(context.Context, interface{}) (interface{}, error) { called = true; return nil, nil })
	if status.Code(e) != codes.ResourceExhausted || called {
		t.Fatal("overload not rejected", e)
	}
	<-b.slots
	_, e = b.Interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "test"}, func(context.Context, interface{}) (interface{}, error) { return "ok", nil })
	if e != nil || len(b.slots) != 0 {
		t.Fatal("slot leaked", e)
	}
}
func TestCanceledAdmission(t *testing.T) {
	b := &Broker{slots: make(chan struct{}, 1), Metrics: new(metrics.Metrics)}
	ctx, c := context.WithCancel(context.Background())
	c()
	_, e := b.Interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "test"}, func(context.Context, interface{}) (interface{}, error) { return nil, errors.New("should not run") })
	if status.Code(e) != codes.Canceled || len(b.slots) != 0 {
		t.Fatal(e)
	}
}
