// Package server accepts TCP connections, reads size-prefixed Kafka frames, gates request versions
// and writes responses strictly in request order.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

// Handler serves one request whose header is parsed and whose version is supported.
// It returns the response body (without header). respond == false writes nothing (Produce acks=0).
// A non-nil error closes the connection.
type Handler interface {
	Handle(ctx context.Context, h protocol.RequestHeader, body *protocol.Reader) (resp []byte, respond bool, err error)
}

// Options configures a Server. Zero values select the defaults.
type Options struct {
	MaxFrameBytes int32         // default 100 MiB
	IdleTimeout   time.Duration // default 10 min; reset per request
	Logger        *slog.Logger
}

// Server is a framed TCP server.
type Server struct {
	h      Handler
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	shutdown bool
	wg       sync.WaitGroup
}

// New returns a Server that dispatches to h.
func New(h Handler, opts Options) *Server {
	if opts.MaxFrameBytes <= 0 {
		opts.MaxFrameBytes = 100 << 20
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 10 * time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{h: h, opts: opts, ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}}
}

// Serve accepts connections on l until Shutdown; it then returns net.ErrClosed.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		_ = l.Close()
		return net.ErrClosed
	}
	s.listener = l
	s.mu.Unlock()
	for {
		c, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			down := s.shutdown
			s.mu.Unlock()
			if down {
				return net.ErrClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		s.mu.Lock()
		if s.shutdown {
			s.mu.Unlock()
			_ = c.Close()
			return net.ErrClosed
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			s.serveConn(c)
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
		}()
	}
}

// Shutdown closes the listener and every connection and waits for the connection goroutines.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.shutdown = true
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.cancel()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
