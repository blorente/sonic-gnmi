package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"testing"

	"github.com/openconfig/gnoi/healthz"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Fake interface implementation that returns success!
type fakeHealthzSuccess struct{}

func (t fakeHealthzSuccess) getHealth(dc healthz.HealthzClient, ctx context.Context, req *healthz.GetRequest) (*healthz.GetResponse, error) {
	return &healthz.GetResponse{}, nil
}

var testHealthzSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc healthz.HealthzClient)
}{
	{
		desc: "getHealthPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getHealthSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)
		},
	},
	{
		desc: "getHealthSucceedsWithIntfInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Set("intf", "Ethernet0")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)
		},
	},
	{
		desc: "getHealthSucceedsWithXcvrInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "Ethernet0")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)
		},
	},
	{
		desc: "getHealthSucceedsWithNSFInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "Ethernet0")
			flag.Set("nsf", "NSF")
			flag.Parse()

			getHealth(dc, ctx)
		},
	},
	{
		desc: "getHealthPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Fake interface implementation that returns error!
type fakeHealthzFailure struct{}

func (t fakeHealthzFailure) getHealth(dc healthz.HealthzClient, ctx context.Context, req *healthz.GetRequest) (*healthz.GetResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testHealthzFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc healthz.HealthzClient)
}{
	{
		desc: "getHealthPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getHealthPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc healthz.HealthzClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Set("intf", "")
			flag.Set("xcvr", "")
			flag.Set("nsf", "")
			flag.Parse()

			getHealth(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for Healthz.
func TestHealthzCLI(t *testing.T) {
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

	dc := healthz.NewHealthzClient(conn)

	// Simulate service success!
	hImpl = fakeHealthzSuccess{}
	for _, test := range testHealthzSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}

	// Simulate service failure!
	hImpl = fakeHealthzFailure{}
	for _, test := range testHealthzFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}
}