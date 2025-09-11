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
type fakeOcsSuccess struct{}

func (t fakeOcsSuccess) setConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.SetConnectionsRequest) (*ocs_pb.SetConnectionsResponse, error) {
	return &ocs_pb.SetConnectionsResponse{}, nil
}

func (t fakeOcsSuccess) getConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.GetConnectionsRequest) (*ocs_pb.GetConnectionsResponse, error) {
	return &ocs_pb.GetConnectionsResponse{}, nil
}

var testOcsSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient)
}{
	{
		desc: "setConnectionsPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setConnectionsSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setConnections(oc, ctx)
		},
	},
	{
		desc: "setConnectionsPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			setConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getConnectionsPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getConnectionsSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getConnections(oc, ctx)
		},
	},
	{
		desc: "getConnectionsPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			getConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Fake interface implementation that returns error!
type fakeOcsFailure struct{}

func (t fakeOcsFailure) setConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.SetConnectionsRequest) (*ocs_pb.SetConnectionsResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeOcsFailure) getConnections(oc ocs_pb.OcsClient, ctx context.Context, req *ocs_pb.GetConnectionsRequest) (*ocs_pb.GetConnectionsResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testOcsFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient)
}{
	{
		desc: "setConnectionsPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			setConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "setConnectionsPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			setConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getConnectionsPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getConnectionsPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, oc ocs_pb.OcsClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getConnections(oc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for OcsTest.
func TestOcsTestCLI(t *testing.T) {
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

	oc := ocs_pb.NewOcsClient(conn)

	// Simulate service success!
	oImpl = fakeOcsSuccess{}
	for _, test := range testOcsSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, oc)
		})
	}
	// Simulate service failure!
	oImpl = fakeOcsFailure{}
	for _, test := range testOcsFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, oc)
		})
	}
}
