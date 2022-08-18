// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.6.1
// source: datanode.proto

package protos

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

// DataserverClient is the client API for Dataserver service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataserverClient interface {
	WriteChunk(ctx context.Context, in *WriteChunkRequest, opts ...grpc.CallOption) (*WriteChunkResponse, error)
	ReadChunk(ctx context.Context, in *ReadChunkRequest, opts ...grpc.CallOption) (*ReadChunkResponse, error)
	GetUuidPath(ctx context.Context, in *UuidRequest, opts ...grpc.CallOption) (*UuidResponse, error)
}

type dataserverClient struct {
	cc grpc.ClientConnInterface
}

func NewDataserverClient(cc grpc.ClientConnInterface) DataserverClient {
	return &dataserverClient{cc}
}

func (c *dataserverClient) WriteChunk(ctx context.Context, in *WriteChunkRequest, opts ...grpc.CallOption) (*WriteChunkResponse, error) {
	out := new(WriteChunkResponse)
	err := c.cc.Invoke(ctx, "/protos.dataserver/WriteChunk", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataserverClient) ReadChunk(ctx context.Context, in *ReadChunkRequest, opts ...grpc.CallOption) (*ReadChunkResponse, error) {
	out := new(ReadChunkResponse)
	err := c.cc.Invoke(ctx, "/protos.dataserver/ReadChunk", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dataserverClient) GetUuidPath(ctx context.Context, in *UuidRequest, opts ...grpc.CallOption) (*UuidResponse, error) {
	out := new(UuidResponse)
	err := c.cc.Invoke(ctx, "/protos.dataserver/GetUuidPath", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DataserverServer is the server API for Dataserver service.
// All implementations must embed UnimplementedDataserverServer
// for forward compatibility
type DataserverServer interface {
	WriteChunk(context.Context, *WriteChunkRequest) (*WriteChunkResponse, error)
	ReadChunk(context.Context, *ReadChunkRequest) (*ReadChunkResponse, error)
	GetUuidPath(context.Context, *UuidRequest) (*UuidResponse, error)
	mustEmbedUnimplementedDataserverServer()
}

// UnimplementedDataserverServer must be embedded to have forward compatible implementations.
type UnimplementedDataserverServer struct {
}

func (UnimplementedDataserverServer) WriteChunk(context.Context, *WriteChunkRequest) (*WriteChunkResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteChunk not implemented")
}
func (UnimplementedDataserverServer) ReadChunk(context.Context, *ReadChunkRequest) (*ReadChunkResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadChunk not implemented")
}
func (UnimplementedDataserverServer) GetUuidPath(context.Context, *UuidRequest) (*UuidResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetUuidPath not implemented")
}
func (UnimplementedDataserverServer) mustEmbedUnimplementedDataserverServer() {}

// UnsafeDataserverServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataserverServer will
// result in compilation errors.
type UnsafeDataserverServer interface {
	mustEmbedUnimplementedDataserverServer()
}

func RegisterDataserverServer(s grpc.ServiceRegistrar, srv DataserverServer) {
	s.RegisterService(&Dataserver_ServiceDesc, srv)
}

func _Dataserver_WriteChunk_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteChunkRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataserverServer).WriteChunk(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.dataserver/WriteChunk",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataserverServer).WriteChunk(ctx, req.(*WriteChunkRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Dataserver_ReadChunk_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadChunkRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataserverServer).ReadChunk(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.dataserver/ReadChunk",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataserverServer).ReadChunk(ctx, req.(*ReadChunkRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Dataserver_GetUuidPath_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UuidRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataserverServer).GetUuidPath(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protos.dataserver/GetUuidPath",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataserverServer).GetUuidPath(ctx, req.(*UuidRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Dataserver_ServiceDesc is the grpc.ServiceDesc for Dataserver service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Dataserver_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "protos.dataserver",
	HandlerType: (*DataserverServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WriteChunk",
			Handler:    _Dataserver_WriteChunk_Handler,
		},
		{
			MethodName: "ReadChunk",
			Handler:    _Dataserver_ReadChunk_Handler,
		},
		{
			MethodName: "GetUuidPath",
			Handler:    _Dataserver_GetUuidPath_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "datanode.proto",
}