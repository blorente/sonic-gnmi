package main

import (
	"context"
	"fmt"

	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"

	"github.com/golang/protobuf/proto"
	json "google.golang.org/protobuf/encoding/protojson"
)

type ldsInterface interface {
	ldsCommand(dc ocs_pb.LdsClient, ctx context.Context, req *ocs_pb.LdsCommandRequest) (*ocs_pb.LdsCommandResponse, error)
}

type ldsImpl struct{}

func (t ldsImpl) ldsCommand(dc ocs_pb.LdsClient, ctx context.Context, req *ocs_pb.LdsCommandRequest) (*ocs_pb.LdsCommandResponse, error) {
	return dc.LdsCommand(ctx, req)
}

var lImpl ldsInterface

func init() {
	lImpl = ldsImpl{}
}

func ldsCommand(lc ocs_pb.LdsClient, ctx context.Context) {
	fmt.Println("Run LDS command.")
	ctx = setUserCreds(ctx)
	req := &ocs_pb.LdsCommandRequest{}
	switch {
	case *jsonArgs != "":
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	case *protoArgs != "":
		if err := proto.UnmarshalText(*protoArgs, req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	default:
		panic("--jsonin/--protoin must be set.")
	}

	resp, err := lImpl.ldsCommand(lc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
