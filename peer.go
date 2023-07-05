package nakamacluster

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/etcdv3"
	"github.com/doublemo/nakama-cluster/sd/lb"
	"github.com/shimingyah/pool"
	"go.uber.org/zap"
)

type Peer interface {
	Do(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error)
}

type LocalPeer struct {
	sdclient    etcdv3.Client
	config      *Config
	logger      *zap.Logger
	endpointers map[string]*PeerEndpointer
	sync.RWMutex
}

func NewLocalPeer(ctx context.Context, logger *zap.Logger, sdClient etcdv3.Client, config *Config) *LocalPeer {
	return &LocalPeer{
		sdclient:    sdClient,
		config:      config,
		logger:      logger,
		endpointers: make(map[string]*PeerEndpointer),
	}
}

func (s *LocalPeer) Do(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error) {
	s.RLock()
	service, ok := s.endpointers[serviceName]
	s.RUnlock()

	var err error
	if !ok {
		service, err = s.registerSevice(serviceName)
		if err != nil {
			return nil, err
		}
	}

	response, err := lb.Retry(10, time.Second*30, service.balancer).Process(ctx, in)
	if err != nil {
		return nil, err
	}

	return response.(*api.Envelope), nil
}

func (s *LocalPeer) registerSevice(serviceName string) (*PeerEndpointer, error) {
	instancer, err := etcdv3.NewInstancer(s.sdclient, s.config.Prefix+serviceName, s.logger)
	if err != nil {
		return nil, err
	}

	peerEndpointer := &PeerEndpointer{
		instancer: instancer,
		pools:     make(map[string]pool.Pool),
	}

	endpointer := sd.NewEndpointer(instancer, s.factory(instancer, peerEndpointer), s.logger, sd.InvalidateOnError(time.Minute))
	peerEndpointer.endpointer = endpointer
	peerEndpointer.balancer = lb.NewRoundRobin(endpointer)
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
