package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"testing"

	dpb "github.com/openconfig/gnoi/diag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Fake interface implementation that returns success!
type fakeDiagSuccess struct{}

func (t fakeDiagSuccess) startBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error) {
	return &dpb.StartBERTResponse{}, nil
}

func (t fakeDiagSuccess) stopBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error) {
	return &dpb.StopBERTResponse{}, nil
}

func (t fakeDiagSuccess) getBERTResult(dc dpb.DiagClient, ctx context.Context, req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error) {
	return &dpb.GetBERTResultResponse{}, nil
}

var testDiagSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc dpb.DiagClient)
}{
	{
		desc: "startBertPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			startBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startBertSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			startBert(dc, ctx)
		},
	},
	{
		desc: "startBertPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			startBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startBertSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "")
			req := &dpb.StartBERTRequest{BertOperationId: "Test ID"}
			flag.Set("protoin", req.String())
			flag.Parse()

			startBert(dc, ctx)
		},
	},
	{
		desc: "stopBertPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			stopBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopBertSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			stopBert(dc, ctx)
		},
	},
	{
		desc: "stopBertPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			stopBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopBertSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "")
			req := &dpb.StopBERTRequest{BertOperationId: "Test ID"}
			flag.Set("protoin", req.String())
			flag.Parse()

			stopBert(dc, ctx)
		},
	},
	{
		desc: "getBertResultPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getBertResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getBertResultSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getBertResult(dc, ctx)
		},
	},
	{
		desc: "getBertResultPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			getBertResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getBertResultSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			flag.Set("jsonin", "")
			req := &dpb.GetBERTResultRequest{ResultFromAllPorts: true}
			flag.Set("protoin", req.String())
			flag.Parse()

			getBertResult(dc, ctx)
		},
	},
}

// Fake interface implementation that returns error!
type fakeDiagFailure struct{}

func (t fakeDiagFailure) startBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeDiagFailure) stopBERT(dc dpb.DiagClient, ctx context.Context, req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeDiagFailure) getBERTResult(dc dpb.DiagClient, ctx context.Context, req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testDiagFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc dpb.DiagClient)
}{
	{
		desc: "startBertPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			startBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startBertPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			startBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startBertPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &dpb.StartBERTRequest{BertOperationId: "Test ID"}
			flag.Set("protoin", req.String())
			flag.Parse()

			startBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopBertPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			stopBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopBertPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			stopBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopBertPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &dpb.StopBERTRequest{BertOperationId: "Test ID"}
			flag.Set("protoin", req.String())
			flag.Parse()

			stopBert(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getBertResultPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getBertResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getBertResultPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getBertResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getBertResultPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc dpb.DiagClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &dpb.GetBERTResultRequest{ResultFromAllPorts: true}
			flag.Set("protoin", req.String())
			flag.Parse()

			getBertResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for DIAG.
func TestDiagCLI(t *testing.T) {
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

	dc := dpb.NewDiagClient(conn)

	// Simulate service success!
	dImpl = fakeDiagSuccess{}
	for _, test := range testDiagSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}

	// Simulate service failure!
	dImpl = fakeDiagFailure{}
	for _, test := range testDiagFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}
}
