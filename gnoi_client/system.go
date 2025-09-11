package main

import (
	"context"
	"encoding/json"
	"fmt"

	syspb "github.com/openconfig/gnoi/system"
)

// System Services.
func systemReboot(sc syspb.SystemClient, ctx context.Context) {
	fmt.Println("System Reboot.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" {
		panic("--jsonin must be set.")
	}
	req := &syspb.RebootRequest{}
	if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
		panic(err)
	}
	resp, err := sc.Reboot(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func systemRebootStatus(sc syspb.SystemClient, ctx context.Context) {
	fmt.Println("System RebootStatus.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" {
		panic("--jsonin must be set.")
	}
	req := &syspb.RebootStatusRequest{}
	if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
		panic(err)
	}
	resp, err := sc.RebootStatus(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func systemCancelReboot(sc syspb.SystemClient, ctx context.Context) {
	fmt.Println("System CancelReboot.")
	ctx = setUserCreds(ctx)
	if *jsonArgs == "" {
		panic("--jsonin must be set.")
	}
	req := &syspb.CancelRebootRequest{}
	if err := json.Unmarshal([]byte(*jsonArgs), req); err != nil {
		panic(err)
	}
	resp, err := sc.CancelReboot(ctx, req)
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}

func systemTime(sc syspb.SystemClient, ctx context.Context) {
	fmt.Println("System Time.")
	ctx = setUserCreds(ctx)
	resp, err := sc.Time(ctx, &syspb.TimeRequest{})
	if err != nil {
		panic(err)
	}
	respstr, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(respstr))
}
