package main

import (
	"context"
	"encoding/json"
	"fmt"

	bbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"
	"github.com/golang/protobuf/proto"
)

type bBoxInterface interface {
	setXcvrState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetTransceiverStateRequest) (*bbpb.SetTransceiverStateResponse, error)
	setHwLinkState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetHardwareLinkStateRequest) (*bbpb.SetHardwareLinkStateResponse, error)
	setAlarm(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetAlarmRequest) (*bbpb.SetAlarmResponse, error)
}

type bBoxImpl struct{}

func (t bBoxImpl) setXcvrState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetTransceiverStateRequest) (*bbpb.SetTransceiverStateResponse, error) {
	return dc.SetTransceiverState(ctx, req)
}

func (t bBoxImpl) setHwLinkState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetHardwareLinkStateRequest) (*bbpb.SetHardwareLinkStateResponse, error) {
	return dc.SetHardwareLinkState(ctx, req)
}

func (t bBoxImpl) setAlarm(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetAlarmRequest) (*bbpb.SetAlarmResponse, error) {
	return dc.SetAlarm(ctx, req)
}

var bbImpl bBoxInterface

func init() {
	bbImpl = bBoxImpl{}
}

func setTransceiverState(bc bbpb.BlackBoxTestClient, ctx context.Context) {
	fmt.Println("Set Transceiver State.")
	ctx = setUserCreds(ctx)
	req := &bbpb.SetTransceiverStateRequest{}
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

	resp, err := bbImpl.setXcvrState(bc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func setHardwareLinkState(bc bbpb.BlackBoxTestClient, ctx context.Context) {
	fmt.Println("Set Hardware Link State.")
	ctx = setUserCreds(ctx)
	req := &bbpb.SetHardwareLinkStateRequest{}
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

	resp, err := bbImpl.setHwLinkState(bc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func setAlarm(bc bbpb.BlackBoxTestClient, ctx context.Context) {
	fmt.Println("Set Alarm.")
	ctx = setUserCreds(ctx)
	req := &bbpb.SetAlarmRequest{}
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

	resp, err := bbImpl.setAlarm(bc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}