//go:build tools
// +build tools

package pb

import (
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)

// go install \
//     google.golang.org/protobuf/cmd/protoc-gen-go \
//     google.golang.org/grpc/cmd/protoc-gen-go-grpc
