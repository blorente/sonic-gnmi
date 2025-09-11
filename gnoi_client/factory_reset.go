package main

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	factory_reset_pb "github.com/openconfig/gnoi/factory_reset"
	json "google.golang.org/protobuf/encoding/protojson"
)

func startFactoryReset(frc factory_reset_pb.FactoryResetClient, ctx context.Context) {
	fmt.Println("Start Factory Reset.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &factory_reset_pb.StartRequest{}
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
	resp, err := frc.Start(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
