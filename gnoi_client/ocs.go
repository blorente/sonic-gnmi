package main

import (
	"context"
	"fmt"

	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"
	json "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
)

type ocsInterface interface {
	setConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.SetConnectionsRequest) (*ocs_pb.SetConnectionsResponse, error)
	getConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.GetConnectionsRequest) (*ocs_pb.GetConnectionsResponse, error)
}

type ocsImpl struct{}

func (t ocsImpl) setConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.SetConnectionsRequest) (*ocs_pb.SetConnectionsResponse, error) {
	return oc.SetConnections(ctx, req)
}

func (t ocsImpl) getConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.GetConnectionsRequest) (*ocs_pb.GetConnectionsResponse, error) {
	return oc.GetConnections(ctx, req)
}

var oImpl ocsInterface

func init() {
	oImpl = ocsImpl{}
}

func setConnections(lc ocs_pb.OcsClient, ctx context.Context) {
	fmt.Println("Run OCS command.")
	ctx = setUserCreds(ctx)
	req := &ocs_pb.SetConnectionsRequest{}
	switch {
	case *jsonArgs != "":
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	case *protoArgs != "":
		if err := prototext.Unmarshal([]byte(*protoArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	default:
		panic("--jsonin/--protoin must be set.")
	}

	resp, err := oImpl.setConnections(lc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func getConnections(lc ocs_pb.OcsClient, ctx context.Context) {
	fmt.Println("Run OCS command.")
	ctx = setUserCreds(ctx)
	req := &ocs_pb.GetConnectionsRequest{}
	switch {
	case *jsonArgs != "":
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	case *protoArgs != "":
		if err := prototext.Unmarshal([]byte(*protoArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	default:
		panic("--jsonin/--protoin must be set.")
	}

	resp, err := oImpl.getConnections(lc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
