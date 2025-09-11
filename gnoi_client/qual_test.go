package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"testing"

	qualpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"

	types "github.com/openconfig/gnoi/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Fake interface implementation that returns success!
type fakeQualSuccess struct{}

func (t fakeQualSuccess) startPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StartPacketQualificationRequest) (*qualpb.StartPacketQualificationResponse, error) {
	return &qualpb.StartPacketQualificationResponse{}, nil
}

func (t fakeQualSuccess) stopPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StopPacketQualificationRequest) (*qualpb.StopPacketQualificationResponse, error) {
	return &qualpb.StopPacketQualificationResponse{}, nil
}

func (t fakeQualSuccess) getPktQualResult(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.GetPacketQualificationResultRequest) (*qualpb.GetPacketQualificationResultResponse, error) {
	return &qualpb.GetPacketQualificationResultResponse{}, nil
}

var testQualSuccessCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient)
}{
	{
		desc: "startPktQualPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			startPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startPktQualSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			startPktQual(dc, ctx)
		},
	},
	{
		desc: "startPktQualPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			startPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startPktQualSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "")
			req := &qualpb.StartPacketQualificationRequest{
				Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
					{
						Id: "test",
						Interface: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "interfaces",
								},
								{
									Name: "interface",
									Key: map[string]string{
										"name": "Ethernet0",
									},
								},
							},
						},
						MinimumWaitBeforePreparationSeconds: 10,
						NumPreparationPackets:               20,
						PreparationTimeoutSeconds:           30,
						QualificationDurationSeconds:        40,
						QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
					},
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			startPktQual(dc, ctx)
		},
	},
	{
		desc: "stopPktQualPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			stopPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopPktQualSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			stopPktQual(dc, ctx)
		},
	},
	{
		desc: "stopPktQualPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			stopPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopPktQualSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "")
			req := &qualpb.StopPacketQualificationRequest{
				Ids: []string{
					"test",
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			stopPktQual(dc, ctx)
		},
	},
	{
		desc: "getPktQualResultPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getPktQualResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getPktQualResultSucceedsWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getPktQualResult(dc, ctx)
		},
	},
	{
		desc: "getPktQualResultPanicksWithInvalidJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{")
			flag.Set("protoin", "")
			flag.Parse()

			getPktQualResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getPktQualResultSucceedsWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			flag.Set("jsonin", "")
			req := &qualpb.GetPacketQualificationResultRequest{
				Ids: []string{
					"test",
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			getPktQualResult(dc, ctx)
		},
	},
}

// Fake interface implementation that returns error!
type fakeQualFailure struct{}

func (t fakeQualFailure) startPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StartPacketQualificationRequest) (*qualpb.StartPacketQualificationResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeQualFailure) stopPktQual(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.StopPacketQualificationRequest) (*qualpb.StopPacketQualificationResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

func (t fakeQualFailure) getPktQualResult(dc qualpb.PacketLinkQualClient, ctx context.Context, req *qualpb.GetPacketQualificationResultRequest) (*qualpb.GetPacketQualificationResultResponse, error) {
	return nil, fmt.Errorf("Service returns an error!")
}

var testQualFailureCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient)
}{
	{
		desc: "startPktQualPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			startPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startPktQualPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			startPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "startPktQualPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &qualpb.StartPacketQualificationRequest{
				Configs: []*qualpb.StartPacketQualificationRequest_QualificationConfiguration{
					{
						Id: "test",
						Interface: &types.Path{
							Origin: "openconfig",
							Elem: []*types.PathElem{
								{
									Name: "interfaces",
								},
								{
									Name: "interface",
									Key: map[string]string{
										"name": "Ethernet0",
									},
								},
							},
						},
						MinimumWaitBeforePreparationSeconds: 10,
						NumPreparationPackets:               20,
						PreparationTimeoutSeconds:           30,
						QualificationDurationSeconds:        40,
						QualificationEnd:                    qualpb.StartPacketQualificationRequest_NEAR_END,
					},
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			startPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopPktQualPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			stopPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopPktQualPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			stopPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "stopPktQualPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &qualpb.StopPacketQualificationRequest{
				Ids: []string{
					"test",
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			stopPktQual(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getPktQualResultPanicksForEmptyArgs",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			flag.Set("protoin", "")
			flag.Parse()

			getPktQualResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getPktQualResultPanicksWithJSONInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "{}")
			flag.Set("protoin", "")
			flag.Parse()

			getPktQualResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
	{
		desc: "getPktQualResultPanicksWithProtobufInput",
		f: func(ctx context.Context, t *testing.T, dc qualpb.PacketLinkQualClient) {
			defer func() { recover() }()

			flag.Set("jsonin", "")
			req := &qualpb.GetPacketQualificationResultRequest{
				Ids: []string{
					"test",
				},
			}
			flag.Set("protoin", req.String())
			flag.Parse()

			getPktQualResult(dc, ctx)

			t.Fatalf("Should have panicked!")
		},
	},
}

// Tests CLI commands for Packet Link Qualification.
func TestPktQualCLI(t *testing.T) {
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

	dc := qualpb.NewPacketLinkQualClient(conn)

	// Simulate service success!
	qImpl = fakeQualSuccess{}
	for _, test := range testQualSuccessCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}

	// Simulate service failure!
	qImpl = fakeQualFailure{}
	for _, test := range testQualFailureCases {
		t.Run(test.desc, func(t *testing.T) {
			test.f(ctx, t, dc)
		})
	}
}
