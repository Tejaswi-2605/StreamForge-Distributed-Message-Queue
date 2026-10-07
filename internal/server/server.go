package server

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hp "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"net/http"
	pb "streamforge/api/gen"
	"streamforge/internal/broker"
	"streamforge/internal/domain"
	"sync"
	"time"
)

type Server struct {
	Broker          *broker.Broker
	GRPC            *grpc.Server
	HTTP            *http.Server
	listener        net.Listener
	metricsListener net.Listener
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	once            sync.Once
	done            chan struct{}
	Errs            chan error
}

func Start(parent context.Context, b *broker.Broker) (*Server, error) {
	l, e := net.Listen("tcp", b.C.Listen)
	if e != nil {
		return nil, e
	}
	ml, e := net.Listen("tcp", b.C.Metrics)
	if e != nil {
		l.Close()
		return nil, e
	}
	ctx, cancel := context.WithCancel(parent)
	s := &Server{Broker: b, listener: l, metricsListener: ml, cancel: cancel, done: make(chan struct{}), Errs: make(chan error, 2)}
	s.GRPC = grpc.NewServer(grpc.MaxRecvMsgSize(domain.MaxRequestBytes), grpc.MaxSendMsgSize(domain.MaxResponseBytes), grpc.UnaryInterceptor(b.Interceptor))
	pb.RegisterStreamForgeServer(s.GRPC, b)
	h := health.NewServer()
	hp.RegisterHealthServer(s.GRPC, h)
	h.SetServingStatus("", hp.HealthCheckResponse_SERVING)
	mux := http.NewServeMux()
	mux.Handle("/metrics", b.Metrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		work, c := context.WithTimeout(r.Context(), time.Second)
		defer c()
		if e := b.Store.Pool.Ping(work); e != nil {
			http.Error(w, "metadata unavailable", 503)
			return
		}
		if e := b.Store.Redis.Ping(work).Err(); e != nil {
			http.Error(w, "leases unavailable", 503)
			return
		}
		w.Write([]byte("ok\n"))
	})
	s.HTTP = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	s.wg.Add(4)
	go func() {
		defer s.wg.Done()
		if e := s.GRPC.Serve(l); e != nil && !errors.Is(e, grpc.ErrServerStopped) {
			s.Errs <- e
		}
	}()
	go func() {
		defer s.wg.Done()
		if e := s.HTTP.Serve(ml); e != nil && !errors.Is(e, http.ErrServerClosed) {
			s.Errs <- e
		}
	}()
	go func() { defer s.wg.Done(); b.RunRetries(ctx) }()
	go func() { defer s.wg.Done(); b.RunRetention(ctx) }()
	return s, nil
}

// Stop first rejects/drains RPCs, then joins retry/HTTP workers, then closes disk.
func (s *Server) Stop() {
	s.once.Do(func() {
		s.cancel()
		grace := make(chan struct{})
		go func() { s.GRPC.GracefulStop(); close(grace) }()
		select {
		case <-grace:
		case <-time.After(5 * time.Second):
			s.GRPC.Stop()
			<-grace
		}
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		s.HTTP.Shutdown(ctx)
		s.wg.Wait()
		s.Broker.Close()
		close(s.done)
	})
	<-s.done
}
