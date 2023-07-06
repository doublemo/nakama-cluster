package nakamacluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/doublemo/nakama-cluster/endpoint"
	"github.com/doublemo/nakama-cluster/sd"
	"github.com/doublemo/nakama-cluster/sd/etcdv3"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	sockaddr "github.com/hashicorp/go-sockaddr"
	"github.com/shimingyah/pool"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	ErrMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	ErrInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
)

type ServerDelegate interface {
	// Call rpc call
	Call(ctx context.Context, in *api.Envelope) (*api.Envelope, error)

	// Stream rpc stream
	Stream(ctx context.Context, client func(out *api.Envelope) bool, in *api.Envelope) error

	// OnStreamClose rpc stream close
	OnStreamClose(ctx context.Context)
}

type Server struct {
	api.UnimplementedApiServerServer
	ctx        context.Context
	cancelFn   context.CancelFunc
	config     *Config
	peers      Peer
	delegate   atomic.Value
	endpoint   endpoint.Endpoint
	registrar  sd.Registrar
	instancer  sd.Instancer
	endpointer *sd.DefaultEndpointer
	grpcServer *grpc.Server
	logger     *zap.Logger
	once       sync.Once
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.cancelFn != nil {
			s.endpointer.Close()
			s.instancer.Stop()
			s.registrar.Deregister()
			s.peers.Close()
			s.cancelFn()
		}
	})
}

func (s *Server) OnDelegate(delegate ServerDelegate) {
	s.delegate.Store(delegate)
}

func (s *Server) GetPeers() Peer {
	return s.peers
}

func (s *Server) Rpc(ctx context.Context, serviceName string, in *api.Envelope) (*api.Envelope, error) {
	return s.peers.Do(ctx, serviceName, in)
}

func (s *Server) Call(ctx context.Context, in *api.Envelope) (*api.Envelope, error) {
	fn, ok := s.delegate.Load().(ServerDelegate)
	if !ok || fn == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Method Call not implemented")
	}

	return fn.Call(ctx, in)
}

func (s *Server) Stream(in api.ApiServer_StreamServer) error {
	fn, ok := s.delegate.Load().(ServerDelegate)
	if !ok || fn == nil {
		return status.Errorf(codes.InvalidArgument, "Method Stream not implemented")
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	incomingCh := make(chan *api.Envelope, s.config.BroadcastQueueSize)
	outgoingCh := make(chan *api.Envelope, s.config.BroadcastQueueSize)

	client := func(out *api.Envelope) bool {
		select {
		case outgoingCh <- out:
		default:
			return false
		}
		return true
	}

	go func() {
		defer func() {
			close(incomingCh)
		}()

		for {
			payload, err := in.Recv()
			if err != nil {
				s.logger.Debug("Error reading message from client", zap.Error(err))
				break
			}

			select {
			case incomingCh <- payload:
			case <-ctx.Done():
				return
			}
		}
	}()

IncomingLoop:
	for {
		select {
		case msg, ok := <-incomingCh:
			if !ok {
				return status.Errorf(codes.Aborted, "Failed read data from incomingCh")
			}

			if err := fn.Stream(in.Context(), client, msg); err != nil {
				s.logger.Warn("Failed handle message", zap.Error(err))
				return status.Errorf(codes.InvalidArgument, err.Error())
			}

		case msg := <-outgoingCh:
			if err := in.Send(msg); err != nil {
				s.logger.Warn("Failed write to stream", zap.Error(err))
			}

		case <-ctx.Done():
			break IncomingLoop
		}
	}

	fn.OnStreamClose(in.Context())
	return nil
}

func NewServer(ctx context.Context, logger *zap.Logger, sdclient etcdv3.Client, id, name string, vars map[string]string, config Config) *Server {
	ctx, cancel := context.WithCancel(ctx)
	local, err := NewGRPCEndpoint(id, name, vars, config)
	if err != nil {
		logger.Panic("Failed to NewGRPCEndpoint", zap.Error(err))
	}

	s := &Server{
		ctx:      ctx,
		cancelFn: cancel,
		peers:    NewLocalPeer(ctx, logger, sdclient, &config),
		logger:   logger,
		config:   &config,
		registrar: etcdv3.NewRegistrar(sdclient, etcdv3.Service{
			Key:   config.Prefix + local.Name() + "/" + local.ID(),
			Value: local.String(),
		}, logger),
		endpoint: local,
	}

	s.grpcServer = newGrpcServer(logger, s, config)
	s.registrar.Register()
	instancer, err := etcdv3.NewInstancer(sdclient, config.Prefix, logger)
	if err != nil {
		s.registrar.Deregister()
		logger.Panic("Failed to etcdv3.NewInstancer", zap.Error(err))
	}
	s.instancer = instancer
	s.endpointer = sd.NewEndpointer(instancer, func(instance string) (endpoint.Endpoint, io.Closer, error) {
		node := endpoint.New("", "", "", nil, func(ctx context.Context, request interface{}) (response interface{}, err error) {
			return nil, nil
		})
		node.FromString(instance)
		return node, nil, nil
	}, logger)
	return s
}

func newGrpcServer(logger *zap.Logger, srv api.ApiServerServer, c Config) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.InitialWindowSize(pool.InitialWindowSize),
		grpc.InitialConnWindowSize(pool.InitialConnWindowSize),
		grpc.MaxSendMsgSize(pool.MaxSendMsgSize),
		grpc.MaxRecvMsgSize(pool.MaxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    pool.KeepAliveTime,
			Timeout: pool.KeepAliveTimeout,
		}),
	}

	if len(c.GrpcToken) > 0 {
		opts = append(opts,
			grpc.ChainStreamInterceptor(ensureStreamValidToken(c), grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(ensureValidToken(c), grpc_prometheus.UnaryServerInterceptor))
	}

	if len(c.GrpcX509Key) > 0 && len(c.GrpcX509Pem) > 0 {
		cert, err := tls.LoadX509KeyPair(c.GrpcX509Pem, c.GrpcX509Key)
		if err != nil {
			logger.Fatal("Failed load x509", zap.Error(err))
		}

		opts = append(opts,
			grpc.Creds(credentials.NewServerTLSFromCert(&cert)),
		)
	}

	listen, err := net.Listen("tcp", net.JoinHostPort(c.Addr, strconv.Itoa(c.Port)))
	if err != nil {
		logger.Fatal("Failed listen from addr", zap.Error(err), zap.String("addr", c.Addr), zap.Int("port", c.Port))
	}

	s := grpc.NewServer(opts...)
	api.RegisterApiServerServer(s, srv)
	service.RegisterChannelzServiceToServer(s)
	grpc_prometheus.Register(s)
	healthpb.RegisterHealthServer(s, health.NewServer())
	go func() {
		logger.Info("Starting API server for gRPC requests", zap.Int("port", c.Port))
		if err := s.Serve(listen); err != nil {
			logger.Fatal("API server listener failed", zap.Error(err))
		}
	}()
	return s
}

func ensureValidToken(config Config) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, ErrMissingMetadata
		}
		// The keys within metadata.MD are normalized to lowercase.
		// See: https://godoc.org/google.golang.org/grpc/metadata#New
		authorization := md["authorization"]
		if len(authorization) < 1 {
			return nil, ErrInvalidToken
		}

		token := strings.TrimPrefix(authorization[0], "Bearer ")
		if token != config.GrpcToken {
			return nil, ErrInvalidToken
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(ctx, req)
	}
}

// func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler)
func ensureStreamValidToken(config Config) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return ErrMissingMetadata
		}

		authorization := md["authorization"]
		if len(authorization) < 1 {
			return ErrInvalidToken
		}

		token := strings.TrimPrefix(authorization[0], "Bearer ")
		if token != config.GrpcToken {
			return ErrInvalidToken
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(srv, ss)
	}
}

func NewGRPCEndpoint(id, name string, vars map[string]string, config Config) (endpoint.Endpoint, error) {
	addr := ""
	ip, err := net.ResolveIPAddr("ip", config.Addr)
	if err == nil && config.Addr != "" && config.Addr != "0.0.0.0" {
		addr = ip.String()
	} else {
		addr, err = sockaddr.GetPrivateIP()
		if err != nil {
			return nil, err
		}
	}

	vars["domian"] = config.Domain
	return endpoint.New(id, name, fmt.Sprintf("%s:%d", addr, config.Port), vars, nil), nil
}
