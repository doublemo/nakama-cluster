package nakamacluster

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"strings"

	"github.com/doublemo/nakama-cluster/api"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
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

type GrpcConfig struct {
	Addr    string `yaml:"addr" json:"addr" usage:"Interface address to bind RPC to for API. By default listening on all interfaces."`
	Port    int    `yaml:"port" json:"port" usage:"Port number to bind RPC to for API. Default value is 7353."`
	X509Pem string `yaml:"x509_pem" json:"x509_pem" usage:"ssl pem"`
	X509Key string `yaml:"x509_key" json:"x509_key" usage:"ssl key"`
	Token   string `yaml:"token" json:"token" usage:"token"`
}

type GrpcHandler func(ctx context.Context, in *api.Envelope) (*api.Envelope, error)
type GrpcStreamHandler func(ctx context.Context, in api.ApiServer_StreamServer) error

type grpcServer struct {
	api.UnimplementedApiServerServer
	ctx           context.Context
	handler       GrpcHandler
	streamHandler GrpcStreamHandler
}

func (s *grpcServer) Call(ctx context.Context, in *api.Envelope) (*api.Envelope, error) {
	if s.handler == nil {
		return nil, status.Errorf(codes.InvalidArgument, "method Call not implemented")
	}
	return s.handler(ctx, in)
}

func (s *grpcServer) Stream(in api.ApiServer_StreamServer) error {
	if s.streamHandler == nil {
		return status.Errorf(codes.InvalidArgument, "method Call not implemented")
	}
	return s.streamHandler(s.ctx, in)
}

func newGrpcServer(ctx context.Context, logger *zap.Logger, handler GrpcHandler, streamHandler GrpcStreamHandler, c GrpcConfig) (*grpc.Server, error) {
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

	if len(c.X509Key) > 0 && len(c.X509Pem) > 0 {
		cert, err := tls.LoadX509KeyPair(c.X509Pem, c.X509Key)
		if err != nil {
			return nil, err
		}

		opts = append(opts,
			grpc.ChainStreamInterceptor(ensureStreamValidToken(c), grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(ensureValidToken(c), grpc_prometheus.UnaryServerInterceptor),
			grpc.Creds(credentials.NewServerTLSFromCert(&cert)),
		)
	}

	listen, err := net.Listen("tcp", net.JoinHostPort(c.Addr, strconv.Itoa(c.Port)))
	if err != nil {
		return nil, err
	}

	s := grpc.NewServer(opts...)
	api.RegisterApiServerServer(s, &grpcServer{
		ctx:           ctx,
		handler:       handler,
		streamHandler: streamHandler,
	})
	service.RegisterChannelzServiceToServer(s)
	grpc_prometheus.Register(s)
	healthpb.RegisterHealthServer(s, health.NewServer())
	go func() {
		logger.Info("Starting API server for gRPC requests", zap.Int("port", c.Port))
		if err := s.Serve(listen); err != nil {
			logger.Fatal("API server listener failed", zap.Error(err))
		}
	}()
	return s, nil
}

func ensureValidToken(config GrpcConfig) grpc.UnaryServerInterceptor {
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
		if token != config.Token {
			return nil, ErrInvalidToken
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(ctx, req)
	}
}

//func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler)
func ensureStreamValidToken(config GrpcConfig) grpc.StreamServerInterceptor {
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
		if token != config.Token {
			return ErrInvalidToken
		}

		// Continue execution of handler after ensuring a valid token.
		return handler(srv, ss)
	}
}
