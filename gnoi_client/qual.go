package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	qualpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"
)

type qualInterface interface {
	startPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StartPacketQualificationRequest) (*qualpb.StartPacketQualificationResponse, error)
	stopPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StopPacketQualificationRequest) (*qualpb.StopPacketQualificationResponse, error)
	getPktQualResult(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.GetPacketQualificationResultRequest) (*qualpb.GetPacketQualificationResultResponse, error)
}

type qualImpl struct{}

func (t qualImpl) startPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StartPacketQualificationRequest) (*qualpb.StartPacketQualificationResponse, error) {
	return dc.StartPacketQualification(ctx, req)
}

func (t qualImpl) stopPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StopPacketQualificationRequest) (*qualpb.StopPacketQualificationResponse, error) {
	return dc.StopPacketQualification(ctx, req)
}

func (t qualImpl) getPktQualResult(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.GetPacketQualificationResultRequest) (*qualpb.GetPacketQualificationResultResponse, error) {
	return dc.GetPacketQualificationResult(ctx, req)
}

var qImpl qualInterface

func init() {
	qImpl = qualImpl{}
}

func startPktQual(qc qualpb.PacketLinkQualClient, ctx context.Context) {
	fmt.Println("Start Packet Link Qualification.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &qualpb.StartPacketQualificationRequest{}
	if *jsonArgs != "" {
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	} else {
		if err := proto.UnmarshalText(*protoArgs, req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	}
	resp, err := qImpl.startPktQual(qc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func stopPktQual(qc qualpb.PacketLinkQualClient, ctx context.Context) {
	fmt.Println("Stop Packet Link Qualification.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &qualpb.StopPacketQualificationRequest{}
	if *jsonArgs != "" {
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	} else {
		if err := proto.UnmarshalText(*protoArgs, req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	}
	resp, err := qImpl.stopPktQual(qc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func getPktQualResult(qc qualpb.PacketLinkQualClient, ctx context.Context) {
	fmt.Println("Get Packet Link Qualification Result.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &qualpb.GetPacketQualificationResultRequest{}
	if *jsonArgs != "" {
		if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	} else {
		if err := proto.UnmarshalText(*protoArgs, req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	}
	resp, err := qImpl.getPktQualResult(qc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
