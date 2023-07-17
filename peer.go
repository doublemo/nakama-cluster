package nakamacluster

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/etcdv3"
	"github.com/doublemo/nakama-cluster/sd/lb"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type Peer interface {
	Endpointer(serviceName string) (*PeerEndpointer, error)
	Do(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error)
	Connect(ctx context.Context, serviceName string, ch chan *api.Envelope, md metadata.MD) (*PeerStream, error)
	Close()
}

type LocalPeer struct {
	ctx         context.Context
	ctxCancelFn context.CancelFunc
	sdclient    etcdv3.Client
	config      *Config
	logger      *zap.Logger
	endpointers map[string]*PeerEndpointer
	once        sync.Once
	sync.RWMutex
}

func NewLocalPeer(ctx context.Context, logger *zap.Logger, sdClient etcdv3.Client, config *Config) *LocalPeer {
	ctx, cancel := context.WithCancel(ctx)
	s := &LocalPeer{
		ctx:         ctx,
		ctxCancelFn: cancel,
		sdclient:    sdClient,
		config:      config,
		logger:      logger,
		endpointers: make(map[string]*PeerEndpointer),
	}

	go func() {

	}()
	return s
}

func (s *LocalPeer) Endpointer(serviceName string) (*PeerEndpointer, error) {
	s.RLock()
	service, ok := s.endpointers[serviceName]
	s.RUnlock()

	if ok {
		return service, nil
	}

	return s.registerSevice(serviceName)
}

func (s *LocalPeer) Do(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error) {
	endpointer, err := s.Endpointer(serviceName)
	if err != nil {
		return nil, err
	}

	response, err := lb.Retry(3, time.Second*30, endpointer.balancer).Process(ctx, in)
	if err != nil {
		return nil, err
	}
	return response.(*api.Envelope), nil
}

func (s *LocalPeer) Connect(ctx context.Context, serviceName string, ch chan *api.Envelope, md metadata.MD) (*PeerStream, error) {
	endpointer, err := s.Endpointer(serviceName)
	if err != nil {
		return nil, err
	}

	balancer := endpointer.Balancer()
	if balancer == nil {
		return nil, errors.New("balancer is nil")
	}

	endpoint, err := balancer.Endpoint()
	if err != nil {
		return nil, err
	}
	return endpointer.NewStream(endpoint, ch, md, *s.config)
}

func (s *LocalPeer) registerSevice(serviceName string) (*PeerEndpointer, error) {
	instancer, err := etcdv3.NewInstancer(s.sdclient, s.config.Prefix+serviceName, s.logger)
	if err != nil {
		return nil, err
	}

	peerEndpointer := NewPeerEndpointer(s.ctx, instancer)
	endpointer := sd.NewEndpointer(instancer, s.factory(instancer, peerEndpointer), s.logger, sd.InvalidateOnError(time.Second*3))
	peerEndpointer.SetEndpointer(endpointer)
	peerEndpointer.SetBalancer(lb.NewRoundRobin(endpointer))
	s.Lock()
	s.endpointers[serviceName] = peerEndpointer
	s.Unlock()
	return peerEndpointer, nil
}

func (s *LocalPeer) factory(instancer sd.Instancer, peerEndpointer *PeerEndpointer) func(instance string) (endpoint.Endpoint, io.Closer, error) {
	return func(instance string) (endpoint.Endpoint, io.Closer, error) {
		s.logger.Info("sd.NewEndpointer", zap.String("instance", instance))
		node := endpoint.New("", "", "", nil, nil)
		if err := node.FromString(instance); err != nil {
			return nil, nil, err
		}

		err := peerEndpointer.NewPool(node, *s.config)
		if err != nil {
			return nil, nil, err
		}

		node.SetProcessFunc(func(ctx context.Context, request interface{}) (response interface{}, err error) {
			conn, err := peerEndpointer.GetPool(node.ID())
			if err != nil {
				return nil, err
			}

			defer conn.Close()
			return api.NewApiServerClient(conn.Value()).Call(ctx, request.(*api.Envelope))
		})

		return node, NewPeerEndpointerCloser(func() error {
			return peerEndpointer.ClosePool(node.ID())
		}), nil
	}
}

func (s *LocalPeer) Close() {
	s.once.Do(func() {
		if s.ctxCancelFn != nil {
			s.ctxCancelFn()
		}
	})
}
