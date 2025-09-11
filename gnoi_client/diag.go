package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	dpb "github.com/openconfig/gnoi/diag"
)

type diagInterface interface {
	startBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error)
	stopBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error)
	getBERTResult(dc dpb.DiagClient, ctx context.Context, req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error)
}

type diagImpl struct{}

func (t diagImpl) startBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error) {
	return dc.StartBERT(ctx, req)
}

func (t diagImpl) stopBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error) {
	return dc.StopBERT(ctx, req)
}

func (t diagImpl) getBERTResult(dc dpb.DiagClient, ctx context.Context, req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error) {
	return dc.GetBERTResult(ctx, req)
}

var dImpl diagInterface

func init() {
	dImpl = diagImpl{}
}

func startBert(dc dpb.DiagClient, ctx context.Context) {
	fmt.Println("Start BERT.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &dpb.StartBERTRequest{}
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
	resp, err := dImpl.startBERT(dc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func stopBert(dc dpb.DiagClient, ctx context.Context) {
	fmt.Println("Stop BERT.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &dpb.StopBERTRequest{}
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
	resp, err := dImpl.stopBERT(dc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func getBertResult(dc dpb.DiagClient, ctx context.Context) {
	fmt.Println("Get BERT Result.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin must be set.")
	}
	req := &dpb.GetBERTResultRequest{}
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
	resp, err := dImpl.getBERTResult(dc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
