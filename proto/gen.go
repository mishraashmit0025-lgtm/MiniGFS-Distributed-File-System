// Package proto holds the protobuf definitions; run `go generate ./proto` to rebuild gfspb.
package proto

//go:generate protoc -I . --go_out=gfspb --go_opt=paths=source_relative --go-grpc_out=gfspb --go-grpc_opt=paths=source_relative minigfs.proto
