package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/protobuf/proto"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnsi/pathz"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	// Pathz is a location of the Pathz Policy
	pathzTestPolicyFile = "../testdata/gnsi/pathz_policy.pb.txt"
	pathzTestMetaFile   = "../testdata/gnsi/pathz-version.json"
)

func createPathzServer(t *testing.T) *Server {
	t.Helper()
	cfg := testServerConfig(testSrvType)
	cfg.PathzPolicy = true
	cfg.PathzPolicyFile = pathzTestPolicyFile
	cfg.PathzMetaFile = pathzTestMetaFile

	resetPathzPolicyFile(cfg.PathzPolicyFile)

	return createCustomServer(t, cfg)
}

var pathzRotationTestCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server)
}{
	{
		desc: "RotateOpenClose",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			stream.CloseSend()
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotatePolicyEmptyRequest",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if err = stream.Send(&pathz.RotateRequest{}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotatePolicyWrongPolicyProto",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy: &pathz.AuthorizationPolicy{
							Rules: []*pathz.AuthorizationRule{
								&pathz.AuthorizationRule{
									Id:        "Rule1",
									Principal: &pathz.AuthorizationRule_User{User: "User1"},
									Path: &gnmipb.Path{
										Elem: []*gnmipb.PathElem{
											&gnmipb.PathElem{
												Name: "a",
											},
											&gnmipb.PathElem{
												Name: "b",
												Key: map[string]string{
													"k1": "v1",
													"k2": "v2",
												},
											},
										},
									},
									Action: pathz.Action_ACTION_PERMIT,
									Mode:   pathz.Mode_MODE_READ,
								},
								&pathz.AuthorizationRule{
									Id:        "Rule2",
									Principal: &pathz.AuthorizationRule_User{User: "User1"},
									Path: &gnmipb.Path{
										Elem: []*gnmipb.PathElem{
											&gnmipb.PathElem{
												Name: "a",
											},
											&gnmipb.PathElem{
												Name: "b",
												Key: map[string]string{
													"k1": "v1",
													"k3": "v3",
												},
											},
										},
									},
									Action: pathz.Action_ACTION_PERMIT,
									Mode:   pathz.Mode_MODE_READ,
								},
							},
						},
					},
				},
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotatePolicyNoVersion",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						CreatedOn: generateCreatedOn(),
						Policy: &pathz.AuthorizationPolicy{
							Rules: []*pathz.AuthorizationRule{
								&pathz.AuthorizationRule{
									Id:        "Rule1",
									Principal: &pathz.AuthorizationRule_User{User: "User1"},
									Path: &gnmipb.Path{
										Elem: []*gnmipb.PathElem{
											&gnmipb.PathElem{
												Name: "a",
											},
											&gnmipb.PathElem{
												Name: "b",
												Key: map[string]string{
													"k1": "v1",
													"k2": "v2",
												},
											},
										},
									},
									Action: pathz.Action_ACTION_PERMIT,
									Mode:   pathz.Mode_MODE_READ,
								},
							},
						},
					},
				},
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotatePolicySuccess",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			policy := &pathz.AuthorizationPolicy{}
			if err = proto.UnmarshalText(string(pathzTestPolicyDeny), policy); err != nil {
				t.Fatal(err.Error())
			}
			if err = stream.Send(&pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy:    policy,
					},
				},
			}); err != nil {
				t.Fatal(err.Error())
			}
			if resp, err := stream.Recv(); err != nil || resp.GetUpload() == nil {
				t.Fatalf("Did not receive expected UploadResponse response; err: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			if err = stream.Send(&pathz.RotateRequest{RotateRequest: &pathz.RotateRequest_FinalizeRotation{}}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err := resetPathzPolicyFile(s.config.PathzPolicyFile); err != nil {
				t.Errorf("Error when reverting to V1: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotatePolicyNoFinalize",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			policy := &pathz.AuthorizationPolicy{}
			if err = proto.UnmarshalText(string(pathzTestPolicyDeny), policy); err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy:    policy,
					},
				},
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if cfm := resp.GetUpload(); cfm == nil {
				t.Fatal("Did not receive expected UploadResponse response")
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			stream.CloseSend()
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting premature closure of the stream.")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err := resetPathzPolicyFile(s.config.PathzPolicyFile); err != nil {
				t.Errorf("Error when reverting to V1: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "FinalizeNoRotate",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := stream.Send(&pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_FinalizeRotation{},
			}); err != nil {
				t.Fatal(err.Error())
			}

			if _, err := stream.Recv(); status.Code(err) != codes.Aborted {
				t.Fatalf("unexpected error; want Arborted, got: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotateTheSamePolicyTwice",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			policy := &pathz.AuthorizationPolicy{}
			if err = proto.UnmarshalText(string(pathzTestPolicyDeny), policy); err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy:    policy,
					},
				},
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if cfm := resp.GetUpload(); cfm == nil {
				t.Fatal("Did not receive expected UploadResponse response")
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			if err = stream.Send(&pathz.RotateRequest{RotateRequest: &pathz.RotateRequest_FinalizeRotation{}}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Send the same pathz policy to the switch.
			stream, err = sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if status.Code(err) != codes.AlreadyExists {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			if err := resetPathzPolicyFile(s.config.PathzPolicyFile); err != nil {
				t.Errorf("Error when reverting to V1: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "RotateTheSamePolicyTwiceWithForceOverwrite",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			policy := &pathz.AuthorizationPolicy{}
			if err = proto.UnmarshalText(string(pathzTestPolicyDeny), policy); err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy:    policy,
					},
				},
				ForceOverwrite: true,
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if cfm := resp.GetUpload(); cfm == nil {
				t.Fatal("Did not receive expected UploadResponse response")
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			if err = stream.Send(&pathz.RotateRequest{RotateRequest: &pathz.RotateRequest_FinalizeRotation{}}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Send the same pathz policy to the switch with force overwrite.
			stream, err = sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if cfm := resp.GetUpload(); cfm == nil {
				t.Fatal("Did not receive expected UploadResponse response")
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyDeny)
			if err = stream.Send(&pathz.RotateRequest{RotateRequest: &pathz.RotateRequest_FinalizeRotation{}}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err := resetPathzPolicyFile(s.config.PathzPolicyFile); err != nil {
				t.Errorf("Error when reverting to V1: %v", err)
			}
			expectPolicyMatch(t, pathzTestPolicyFile, pathzTestPolicyPermit)
		},
	},
	{
		desc: "ParallelRotationCalls",
		f: func(ctx context.Context, t *testing.T, sc pathz.PathzClient, s *Server) {
			stream, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			policy := &pathz.AuthorizationPolicy{}
			if err = proto.UnmarshalText(string(pathzTestPolicyDeny), policy); err != nil {
				t.Fatal(err.Error())
			}
			req := &pathz.RotateRequest{
				RotateRequest: &pathz.RotateRequest_UploadRequest{
					UploadRequest: &pathz.UploadRequest{
						Version:   generateVersion(),
						CreatedOn: generateCreatedOn(),
						Policy:    policy,
					},
				},
			}
			if err = stream.Send(req); err != nil {
				t.Fatal(err.Error())
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if cfm := resp.GetUpload(); cfm == nil {
				t.Fatal("Did not receive expected UploadResponse response")
			}
			expectPolicyMatch(t, s.config.PathzPolicyFile, pathzTestPolicyDeny)
			// Attempt to send the same pathz policy to the switch.
			stream2, err := sc.Rotate(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}
			stream2.Send(req)
			if _, err = stream2.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if status.Code(err) != codes.Aborted {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Finalize the operation.
			if err = stream.Send(&pathz.RotateRequest{RotateRequest: &pathz.RotateRequest_FinalizeRotation{}}); err != nil {
				t.Fatal(err.Error())
			}
			if _, err = stream.Recv(); err == nil {
				t.Fatal("Expected an error")
			}
			if err != io.EOF {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectPolicyMatch(t, s.config.PathzPolicyFile, pathzTestPolicyDeny)
			if err := resetPathzPolicyFile(s.config.PathzPolicyFile); err != nil {
				t.Errorf("Error when reverting to V1: %v", err)
			}
			expectPolicyMatch(t, s.config.PathzPolicyFile, pathzTestPolicyPermit)
		},
	},
}

// TestPathzRotation tests implementation of pathz rotate service.
func TestGnsiPathzRotation(t *testing.T) {
	s := createPathzServer(t)
	defer os.Remove(pathzTestPolicyFile)
	go runServer(t, s)
	defer s.Stop()

	// Create a gNSI.pathz client and connect it to the gNSI.pathz server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	sc := pathz.NewPathzClient(conn)
	var mu sync.Mutex
	for _, tc := range pathzRotationTestCases {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		t.Run(tc.desc, func(t *testing.T) {
			mu.Lock()
			defer mu.Unlock()
			tc.f(ctx, t, sc, s)
		})
		cancel()
	}

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("RotateUnavailableDuringFreeze", func(t *testing.T) {
		stream, err := sc.Rotate(context.Background())
		if err != nil {
			t.Fatal(err.Error())
		}
		if err = stream.Send(&pathz.RotateRequest{}); err != nil {
			t.Fatal(err.Error())
		}
		_, err = stream.Recv()
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)
	s.gnsiPathz.savePathzFileFreshess(s.config.PathzMetaFile)
}

func resetPathzPolicyFile(path string) error {
	return attemptWrite(path, []byte(pathzTestPolicyPermit), 0600)
}

func TestGnsiPathzUnimplemented(t *testing.T) {
	srv := createPathzServer(t)
	defer srv.Stop()
	defer os.Remove(pathzTestPolicyFile)
	s := NewGNSIPathzServer(srv)

	t.Run("PathzGetUnimplemented", func(t *testing.T) {
		if _, err := s.Get(context.Background(), &pathz.GetRequest{}); status.Code(err) != codes.Unimplemented {
			t.Error("expected: Unimplemented, got: %+v", err)
		}
	})
	t.Run("PathzProbeUnimplemented", func(t *testing.T) {
		if _, err := s.Probe(context.Background(), &pathz.ProbeRequest{}); status.Code(err) != codes.Unimplemented {
			t.Error("expected: Unimplemented, got: %+v", err)
		}
	})

	t.Run("PathzMissingMetaFile", func(t *testing.T) {
		cfg := testServerConfig(testSrvType)
		cfg.PathzPolicyFile = ""
		cfg.PathzPolicy = true
		cs := createCustomServer(t, cfg)
		go runServer(t, cs)
		defer cs.Stop()

		mockAuthenticate := gomonkey.ApplyFunc(getUsername, func(ctx context.Context) (string, error) {
			return "SomeUser", nil
		})
		defer mockAuthenticate.Reset()

		// Connect our client
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
		targetAddr := fmt.Sprintf("127.0.0.1:%d", cs.config.Port)
		conn, err := grpc.Dial(targetAddr, opts...)
		if err != nil {
			t.Fatalf("Dialing to %q failed: %v", targetAddr, err)
		}
		defer conn.Close()
		gClient := gnmipb.NewGNMIClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		// Prepare Get request, the specific path doesn't matter, they should all
		// fail due to the pathz policy
		pathTgt := "OC_YANG"
		pbPath := pathToPb("/openconfig-system:system/state/boot-time")
		reqDataType := gnmipb.GetRequest_ALL
		expRetCode := codes.PermissionDenied
		runTestGet(t, ctx, gClient, pathTgt, pbPath, reqDataType, gnmipb.Encoding_PROTO, expRetCode, nil, false)
	})

	t.Run("PathzRevertFailure", func(t *testing.T) {
		pSrv := createPathzServer(t)
		defer pSrv.Stop()
		sFail := NewGNSIPathzServer(pSrv)
		sFail.policyUpdated = true
		sFail.policyCopy = nil
		if err := sFail.revertPolicy(); err == nil {
			t.Error("expected: error, got: nil")
		}
	})

}

const pathzTestPolicyPermit = `rules: <
  id: "Rule1"
  user: "User1"
  path: <
  >
  action: ACTION_PERMIT
  mode: MODE_READ
>
groups: <
  name: "Group1"
  users: <
    name: "User1"
  >
  users: <
    name: "User2"
  >
>
`

const pathzTestPolicyDeny = `rules: <
  id: "Rule1"
  user: "User1"
  path: <
  >
  action: ACTION_DENY
  mode: MODE_READ
>
groups: <
  name: "Group1"
  users: <
    name: "User1"
  >
  users: <
    name: "User2"
  >
>
`
