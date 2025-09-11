package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"testing"

	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Fake interface implementation that returns success!
type fakeLdsSuccess struct{}

func (t fakeLdsSuccess) ldsCommand(lc ocs_pb.LdsClient, ctx context.Context, req *ocs_pb.LdsCommandRequest) (*ocs_pb.LdsCommandResponse, error) {
	return &ocs_pb.LdsCommandResponse{}, nil
}

var testLdsSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient)
}{
	{
		desc: "ldsCommandPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			ldsCommand(lc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "ldsCommandSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			ldsCommand(lc, ctx)
		},
	},
	{
		desc: "ldsCommandPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			ldsCommand(lc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Fake interface implementation that returns error!
type fakeLdsFailure struct{}

func (t fakeLdsFailure) ldsCommand(lc ocs_pb.LdsClient, ctx context.Context, req *ocs_pb.LdsCommandRequest) (*ocs_pb.LdsCommandResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testLdsFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient)
}{
	{
		desc: "ldsCommandPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			ldsCommand(lc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "ldsCommandPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, lc ocs_pb.LdsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			ldsCommand(lc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for LdsTest.
func TestLdsTestCLI(t *testing.T) {
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

	lc := ocs_pb.NewLdsClient(conn)

	// Simulate service success!
	lImpl = fakeLdsSuccess{}
	for _, test := range testLdsSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, lc)
		})
	}
	// Simulate service failure!
	lImpl = fakeLdsFailure{}
	for _, test := range testLdsFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, lc)
		})
	}
}
