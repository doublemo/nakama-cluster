package nakamacluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/lb"
	"github.com/shimingyah/pool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var ErrPoolNotfound = errors.New("ErrPoolNotfound")

type PeerEndpointerCloser struct {
	closeFunc func() error
}

func NewPeerEndpointerCloser(closeFunc func() error) *PeerEndpointerCloser {
	return &PeerEndpointerCloser{closeFunc: closeFunc}
}

func (s *PeerEndpointerCloser) Close() error {
	if s.closeFunc != nil {
		return s.closeFunc()
	}
	return nil
}

type PeerEndpointer struct {
	ctx         context.Context
	ctxCancelFn context.CancelFunc
	instancer   sd.Instancer
	endpointer  sd.Endpointer
	balancer    lb.Balancer
	pools       sync.Map
	once        sync.Once
	sync.RWMutex
}

func NewPeerEndpointer(ctx context.Context, instancer sd.Instancer) *PeerEndpointer {
	ctx, cancel := context.WithCancel(ctx)
	return &PeerEndpointer{
		ctx:         ctx,
		ctxCancelFn: cancel,
		instancer:   instancer,
	}
}

func (pe *PeerEndpointer) Balancer() lb.Balancer {
	pe.RLock()
	defer pe.RUnlock()
	return pe.balancer
}

func (pe *PeerEndpointer) Endpoints() ([]endpoint.Endpoint, error) {
	pe.RLock()
	defer pe.RUnlock()
	return pe.endpointer.Endpoints()
}

func (pe *PeerEndpointer) SetEndpointer(endpointer sd.Endpointer) {
	pe.Lock()
	pe.endpointer = endpointer
	pe.Unlock()
}

func (pe *PeerEndpointer) SetBalancer(balancer lb.Balancer) {
	pe.Lock()
	pe.balancer = balancer
	pe.Unlock()
}

func (pe *PeerEndpointer) NewPool(ed endpoint.Endpoint, config Config) error {
	domain, err := ed.RemoteAddress()
	if err != nil {
		return err
	}

	p, err := pool.New(domain, pool.Options{
		Dial: func(address string) (*grpc.ClientConn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), pool.DialTimeout)
			defer cancel()
			opts := make([]grpc.DialOption, 0)
			opts = append(opts,
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoff.Config{
						BaseDelay:  time.Second * 1,
						Multiplier: 1.6,
						Jitter:     0.2,
						MaxDelay:   pool.BackoffMaxDelay,
					},
					MinConnectTimeout: time.Second * 20,
				}),
				grpc.WithInitialWindowSize(pool.InitialWindowSize),
				grpc.WithInitialConnWindowSize(pool.InitialConnWindowSize),
				grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(pool.MaxSendMsgSize)),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pool.MaxRecvMsgSize)),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:                pool.KeepAliveTime,
					Timeout:             pool.KeepAliveTimeout,
					PermitWithoutStream: true,
				}))

			if len(config.GrpcToken) > 0 {
				opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					ctx = metadata.AppendToOutgoingContext(ctx, "authorization", config.GrpcToken)
					return invoker(ctx, method, req, reply, cc, opts...)
				}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
					ctx = metadata.AppendToOutgoingContext(ctx, "authorization", config.GrpcToken)
					return streamer(ctx, desc, cc, method, opts...)
				}))
			}

			if len(config.GrpcX509CA) > 0 {
				creds, err := credentials.NewClientTLSFromFile(config.GrpcX509CA, config.GrpcServerHostOverride)
				if err != nil {
					return nil, err
				}
				opts = append(opts, grpc.WithTransportCredentials(creds))
			} else {
				opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			}
			return grpc.DialContext(ctx, address, opts...)
		},

		MaxIdle:              config.GrpcPoolMaxIdle,
		MaxActive:            config.GrpcPoolMaxActive,
		MaxConcurrentStreams: config.GrpcPoolMaxConcurrentStreams,
		Reuse:                config.GrpcPoolReuse,
	})

	if err != nil {
		return err
	}

	pe.pools.Store(ed.ID(), p)
	return nil
}

func (pe *PeerEndpointer) NewStream(ed endpoint.Endpoint, messageQueueCh chan *api.Envelope, md metadata.MD, config Config) (*PeerStream, error) {
	domain, err := ed.RemoteAddress()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(pe.ctx, pool.DialTimeout)
	defer cancel()
	opts := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second * 1,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   pool.BackoffMaxDelay,
			},
			MinConnectTimeout: time.Second * 20,
		}),
		grpc.WithInitialWindowSize(pool.InitialWindowSize),
		grpc.WithInitialConnWindowSize(pool.InitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(pool.MaxSendMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pool.MaxRecvMsgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                pool.KeepAliveTime,
			Timeout:             pool.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	if len(config.GrpcToken) > 0 {
		opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", config.GrpcToken)
			return invoker(ctx, method, req, reply, cc, opts...)
		}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", config.GrpcToken)
			return streamer(ctx, desc, cc, method, opts...)
		}))
	}

	if len(config.GrpcX509CA) > 0 {
		creds, err := credentials.NewClientTLSFromFile(config.GrpcX509CA, config.GrpcServerHostOverride)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.DialContext(ctx, domain, opts...)
	if err != nil {
		return nil, err
	}

	ctx = metadata.NewOutgoingContext(pe.ctx, md)
	client := api.NewApiServerClient(conn)
	clientStream, err := client.Stream(ctx)
	if err != nil {
		return nil, err
	}

	stream := NewPeerStream(pe.ctx, messageQueueCh, clientStream)
	return stream, nil
}

func (pe *PeerEndpointer) GetPool(id string) (pool.Conn, error) {
	p, ok := pe.pools.Load(id)
	if !ok || p == nil {
		return nil, ErrPoolNotfound
	}

	return p.(pool.Pool).Get()
}

func (pe *PeerEndpointer) ClosePool(id string) error {
	p, ok := pe.pools.LoadAndDelete(id)
	if !ok || p == nil {
		return nil
	}
	return p.(pool.Pool).Close()
}

func (pe *PeerEndpointer) Close() {
	pe.once.Do(func() {
		if pe.ctxCancelFn != nil {
			pe.ctxCancelFn()
		}

		pe.pools.Range(func(key, value any) bool {
			if m, ok := value.(pool.Pool); ok && m != nil {
				m.Close()
			}

			pe.pools.Delete(key)
			return true
		})

		if m, ok := pe.endpointer.(*sd.DefaultEndpointer); ok && m != nil {
			m.Close()
		}

		pe.instancer.Stop()
		fmt.Println("a ...any", pe)
	})
}
