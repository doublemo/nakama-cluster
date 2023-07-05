package nakamacluster

import (
	"context"
	"errors"
	"net/url"
	"sync"

	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/lb"
	"github.com/shimingyah/pool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	instancer  sd.Instancer
	endpointer sd.Endpointer
	balancer   lb.Balancer
	pools      map[string]pool.Pool
	sync.RWMutex
}

func (pe *PeerEndpointer) NewPool(ed endpoint.Endpoint, config Config) error {
	domain := ed.Var("domain")
	if len(domain) < 1 {
		domain = ed.Address()
	} else {
		uri, err := url.Parse(domain)
		if err != nil {
			return err
		}
		domain = uri.Host + ":" + uri.Port()
	}

	p, err := pool.New(domain, pool.Options{
		Dial: func(address string) (*grpc.ClientConn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), pool.DialTimeout)
			defer cancel()
			opts := make([]grpc.DialOption, 0)
			opts = append(opts, grpc.WithInsecure(),
				grpc.WithBackoffMaxDelay(pool.BackoffMaxDelay),
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

	pe.Lock()
	pe.pools[ed.ID()] = p
	pe.Unlock()
	return nil
}

func (pe *PeerEndpointer) GetPool(id string) (pool.Conn, error) {
	pe.RLock()
	p, ok := pe.pools[id]
	pe.RUnlock()

	if !ok {
		return nil, ErrPoolNotfound
	}

	return p.Get()
}

func (pe *PeerEndpointer) ClosePool(id string) error {
	pe.RLock()
	p, ok := pe.pools[id]
	pe.RUnlock()

	if !ok {
		return nil
	}
	return p.Close()
}

func (pe *PeerEndpointer) Close() error {
	pe.Lock()
	for _, v := range pe.pools {
		pe.Unlock()
		v.Close()
		pe.Lock()
	}

	pe.pools = make(map[string]pool.Pool)
	pe.Unlock()
	pe.instancer.Stop()
	return nil
}
