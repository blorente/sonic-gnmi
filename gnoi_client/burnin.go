package main

import (
	"context"
	"fmt"

	burnin_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/burnin"
	"github.com/golang/protobuf/proto"
	json "google.golang.org/protobuf/encoding/protojson"
)

func startBurnin(bc burnin_pb.BurninClient, ctx context.Context) {
	fmt.Println("Start Burnin.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &burnin_pb.StartBurninRequest{}
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
	resp, err := bc.StartBurnin(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func stopBurnin(bc burnin_pb.BurninClient, ctx context.Context) {
	fmt.Println("Stop Burnin.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &burnin_pb.StopBurninRequest{}
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
	resp, err := bc.StopBurnin(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func getBurninResult(bc burnin_pb.BurninClient, ctx context.Context) {
	fmt.Println("Get Burnin Result.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &burnin_pb.GetBurninResultRequest{}
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
	resp, err := bc.GetBurninResult(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}