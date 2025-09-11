package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/openconfig/gnoi/healthz"
	"github.com/openconfig/gnoi/types"
)

type healthzInterface interface {
	getHealth(dc healthz.HealthzClient, ctx context.Context, req *healthz.GetRequest) (*healthz.GetResponse, error)
}

type healthzImpl struct{}

func (t healthzImpl) getHealth(dc healthz.HealthzClient, ctx context.Context, req *healthz.GetRequest) (*healthz.GetResponse, error) {
	return dc.Get(ctx, req)
}

var hImpl healthzInterface

func init() {
	hImpl = healthzImpl{}
}

func populateIntfPath(req *healthz.GetRequest) error {
	if req == nil {
		return fmt.Errorf("Request cannot be nil!")
	}
	req.Path = &types.Path{
		Origin: "openconfig",
		Elem: []*types.PathElem{
			{
				Name: "interfaces",
			},
			{
				Name: "interface",
				Key: map[string]string{
					"name": *intf,
				},
			},
		},
	}
	return nil
}

func populateXcvrPath(req *healthz.GetRequest) error {
	if req == nil {
		return fmt.Errorf("Request cannot be nil!")
	}
	req.Path = &types.Path{
		Origin: "openconfig",
		Elem: []*types.PathElem{
			{
				Name: "components",
			},
			{
				Name: "component",
				Key: map[string]string{
					"name": *xcvr,
				},
			},
		},
	}
	return nil
}

func populateNSFPath(req *healthz.GetRequest) error {
	if req == nil {
		return fmt.Errorf("Request cannot be nil!")
	}
	req.Path = &types.Path{
		Origin: "openconfig",
		Elem: []*types.PathElem{
			{
				Name: "components",
			},
			{
				Name: "component",
				Key: map[string]string{
					"name": *nsf,
				},
			},
		},
	}
	return nil
}

func getHealth(dc healthz.HealthzClient, ctx context.Context) {
	fmt.Println("Get Health Information.")
	ctx = setUserCreds(ctx)
	if *intf == "" && *xcvr == "" && *nsf == "" && *jsonArgs == "" && *protoArgs == "" {
		panic("--jsonin/--protoin/--intf/--xcvr/--nsf must be set.")
	}
	req := &healthz.GetRequest{}
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
	case *intf != "":
		if err := populateIntfPath(req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	case *xcvr != "":
		if err := populateXcvrPath(req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	case *nsf != "":
		if err := populateNSFPath(req); err != nil {
			fmt.Println("Unable to parse request: ", err)
			panic("Unable to parse request.")
		}
	}
	resp, err := hImpl.getHealth(dc, ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
