package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"testing"

	bbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Fake interface implementation that returns success!
type fakeBBoxSuccess struct{}

func (t fakeBBoxSuccess) setXcvrState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetTransceiverStateRequest) (*bbpb.SetTransceiverStateResponse, error) {
	return &bbpb.SetTransceiverStateResponse{}, nil
}

func (t fakeBBoxSuccess) setHwLinkState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetHardwareLinkStateRequest) (*bbpb.SetHardwareLinkStateResponse, error) {
	return &bbpb.SetHardwareLinkStateResponse{}, nil
}

func (t fakeBBoxSuccess) setAlarm(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetAlarmRequest) (*bbpb.SetAlarmResponse, error) {
	return &bbpb.SetAlarmResponse{}, nil
}

var testBlackBoxSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient)
}{
	{
		desc: "setTransceiverStatePanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setTransceiverState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setTransceiverStateSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setTransceiverState(dc, ctx)
		},
	},
	{
		desc: "setTransceiverStatePanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			setTransceiverState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setHardwareLinkStatePanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setHardwareLinkState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setHardwareLinkStateSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setHardwareLinkState(dc, ctx)
		},
	},
	{
		desc: "setHardwareLinkStatePanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			setHardwareLinkState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setAlarmPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setAlarm(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setAlarmSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setAlarm(dc, ctx)
		},
	},
	{
		desc: "setAlarmPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			setAlarm(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Fake interface implementation that returns error!
type fakeBBoxFailure struct{}

func (t fakeBBoxFailure) setXcvrState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetTransceiverStateRequest) (*bbpb.SetTransceiverStateResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeBBoxFailure) setHwLinkState(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetHardwareLinkStateRequest) (*bbpb.SetHardwareLinkStateResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeBBoxFailure) setAlarm(dc bbpb.BlackBoxTestClient, ctx context.Context, req *bbpb.SetAlarmRequest) (*bbpb.SetAlarmResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testBlackBoxFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient)
}{
	{
		desc: "setTransceiverStatePanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setTransceiverState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setTransceiverStatePanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setTransceiverState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setHardwareLinkStatePanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setHardwareLinkState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setHardwareLinkStatePanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setHardwareLinkState(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setAlarmPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setAlarm(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setAlarmPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc bbpb.BlackBoxTestClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setAlarm(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for BlackBoxTest.
func TestBlackBoxTestCLI(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := "127.0.0.1:8081"
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc := bbpb.NewBlackBoxTestClient(conn)

	// Simulate service success!
	bbImpl = fakeBBoxSuccess{}
	for _, test := range testBlackBoxSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}
	// Simulate service failure!
	bbImpl = fakeBBoxFailure{}
	for _, test := range testBlackBoxFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}
}
