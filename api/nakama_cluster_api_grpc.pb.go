// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.10
// source: nakama_cluster_api.proto

package api

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ApiServerClient is the client API for ApiServer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ApiServerClient interface {
	Call(ctx context.Context, in *Envelope, opts ...grpc.CallOption) (*Envelope, error)
	Stream(ctx context.Context, opts ...grpc.CallOption) (ApiServer_StreamClient, error)
}

type apiServerClient struct {
	cc grpc.ClientConnInterface
}

func NewApiServerClient(cc grpc.ClientConnInterface) ApiServerClient {
	return &apiServerClient{cc}
}

func (c *apiServerClient) Call(ctx context.Context, in *Envelope, opts ...grpc.CallOption) (*Envelope, error) {
	out := new(Envelope)
	err := c.cc.Invoke(ctx, "/nakama.cluster.ApiServer/Call", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiServerClient) Stream(ctx context.Context, opts ...grpc.CallOption) (ApiServer_StreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &ApiServer_ServiceDesc.Streams[0], "/nakama.cluster.ApiServer/Stream", opts...)
	if err != nil {
		return nil, err
	}
	x := &apiServerStreamClient{stream}
	return x, nil
}

type ApiServer_StreamClient interface {
	Send(*Envelope) error
	Recv() (*Envelope, error)
	grpc.ClientStream
}

type apiServerStreamClient struct {
	grpc.ClientStream
}

func (x *apiServerStreamClient) Send(m *Envelope) error {
	return x.ClientStream.SendMsg(m)
}

func (x *apiServerStreamClient) Recv() (*Envelope, error) {
	m := new(Envelope)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ApiServerServer is the server API for ApiServer service.
// All implementations must embed UnimplementedApiServerServer
// for forward compatibility
type ApiServerServer interface {
	Call(context.Context, *Envelope) (*Envelope, error)
	Stream(ApiServer_StreamServer) error
	mustEmbedUnimplementedApiServerServer()
}

// UnimplementedApiServerServer must be embedded to have forward compatible implementations.
type UnimplementedApiServerServer struct {
}

func (UnimplementedApiServerServer) Call(context.Context, *Envelope) (*Envelope, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Call not implemented")
}
func (UnimplementedApiServerServer) Stream(ApiServer_StreamServer) error {
	return status.Errorf(codes.Unimplemented, "method Stream not implemented")
}
func (UnimplementedApiServerServer) mustEmbedUnimplementedApiServerServer() {}

// UnsafeApiServerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ApiServerServer will
// result in compilation errors.
type UnsafeApiServerServer interface {
	mustEmbedUnimplementedApiServerServer()
}

func RegisterApiServerServer(s grpc.ServiceRegistrar, srv ApiServerServer) {
	s.RegisterService(&ApiServer_ServiceDesc, srv)
}

func _ApiServer_Call_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Envelope)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ApiServerServer).Call(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nakama.cluster.ApiServer/Call",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ApiServerServer).Call(ctx, req.(*Envelope))
	}
	return interceptor(ctx, in, info, handler)
}

func _ApiServer_Stream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ApiServerServer).Stream(&apiServerStreamServer{stream})
}

type ApiServer_StreamServer interface {
	Send(*Envelope) error
	Recv() (*Envelope, error)
	grpc.ServerStream
}

type apiServerStreamServer struct {
	grpc.ServerStream
}

func (x *apiServerStreamServer) Send(m *Envelope) error {
	return x.ServerStream.SendMsg(m)
}

func (x *apiServerStreamServer) Recv() (*Envelope, error) {
	m := new(Envelope)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ApiServer_ServiceDesc is the grpc.ServiceDesc for ApiServer service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ApiServer_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "nakama.cluster.ApiServer",
	HandlerType: (*ApiServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Call",
			Handler:    _ApiServer_Call_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _ApiServer_Stream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "nakama_cluster_api.proto",
}
