package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	ospb "github.com/openconfig/gnoi/os"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	json "google.golang.org/protobuf/encoding/protojson"
)

// ProcessFakeTrfReady responds with the TrancontrollerrReady response.
func ProcessFakeTrfReady(req string) (string, error) {
	// Fake response.
	resp := &ospb.InstallResponse{
		Response: &ospb.InstallResponse_TrancontrollerrReady{},
	}

	respStr, err := json.Marshal(resp)
	if err != nil {
		log.Errorln("Cannot marshal TrancontrollerrReady response!")
		return "", fmt.Errorf("Cannot marshal TrancontrollerrReady response!")
	}

	return string(respStr), nil
}

// ProcessFakeTrfEnd responds with the Validated response.
func ProcessFakeTrfEnd(req string) (string, error) {
	// Fake response.
	resp := &ospb.InstallResponse{
		Response: &ospb.InstallResponse_Validated{},
	}

	respStr, err := json.Marshal(resp)
	if err != nil {
		log.Errorln("Cannot marshal TrancontrollerrEnd response!")
		return "", fmt.Errorf("Cannot marshal TrancontrollerrEnd response!")
	}

	return string(respStr), nil
}

// TODO(b/328077908) Alarms to be implemented later
// func expectAlarm(t *testing.T) {
// 	defer clearAlarm(t)

// 	// Check for alarm
// 	sh, sErr := common_utils.NewSystemStateHelper()
// 	if sErr != nil {
// 		t.Fatalf("Failed to create system state helper: %v", sErr)
// 	}
// 	defer sh.Close()
// 	cstates := sh.AllComponentStates()
// 	if state, ok := cstates[common_utils.Telemetry]; !ok || state.State != common_utils.ComponentMinor {
// 		t.Fatalf("Expected ComponentMinor alarm, got: %v", state)
// 	}
// 	t.Logf("ComponentMinor alarm is present")
// }

func clearAlarm(t *testing.T) {
	rc := getRedisClient(t, "STATE_DB")
	defer db.CloseRedisClient(rc)
	if err := rc.Del(context.Background(), "COMPONENT_STATE_TABLE|Telemetry").Err(); err != nil {
		t.Fatalf("Failed to clear component state information in DB: %v\n", err)
	}
}

var testOSCases = []struct {
	desc string
	f    func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer)
}{
	// TODO(b/328077908) Alarms to be implemented later
	// {
	// 	desc: "OSActivateFailsAsBackEndIsUnimplemented",
	// 	f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
	// 		_, err := sc.Activate(ctx, &ospb.ActivateRequest{})
	// 		expectAlarm(err)
	// 		testErr(err, codes.Internal, "Internal SONiC HostService failure", t)
	// 	},
	// },
	// TODO(b/328077908) Alarms to be implemented later
	// {
	// 	desc: "OSVerifyFailsAsBackEndIsUnimplemented",
	// 	f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
	// 		_, err := sc.Verify(ctx, &ospb.VerifyRequest{})
	// 		expectAlarm(err)
	// 		testErr(err, codes.Internal, "Internal SONiC HostService failure", t)
	// 	},
	// },
	{
		desc: "OSInstallFailsIfTrancontrollerrRequestIsMissingVersion",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Send TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive InstallError due to missing version.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			instErr := resp.GetInstallError()
			if instErr == nil {
				t.Fatal("Expected InstallError!")
			}
			if instErr.GetType() != ospb.InstallError_PARSE_FAIL {
				t.Fatal("Expected InstallError type: PARSE_FAIL!")
			}
			// Receive error reporting.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected error!")
			}
			// TODO(b/328077908) Alarms to be implemented later
			// expectAlarm(err)
			testErr(err, codes.Aborted, "Failed to process TrancontrollerrRequest.", t)
		},
	},
	{
		desc: "OSInstallFailsForConcurrentOperations",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Send TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: "os1.1",
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrReady response.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetTrancontrollerrReady() == nil {
				t.Fatal("Did not receive expected TrancontrollerrReady response")
			}

			targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)

			// Create a new client.
			tlsConfig := &tls.Config{InsecureSkipVerify: true}
			opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
			conn, err := grpc.Dial(targetAddr, opts...)
			if err != nil {
				t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
			}
			defer conn.Close()

			newsc := ospb.NewOSClient(conn)

			newctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			newstream, err := newsc.Install(newctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive InstallError due to Install in progress.
			resp, err = newstream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			instErr := resp.GetInstallError()
			if instErr == nil {
				t.Fatal("Expected InstallError!")
			}
			if instErr.GetType() != ospb.InstallError_INSTALL_IN_PROGRESS {
				t.Fatal("Expected InstallError type: INSTALL_IN_PROGRESS!")
			}

			_, err = newstream.Recv()
			if err == nil {
				t.Fatal("Expected error!")
			}
			testErr(err, codes.Aborted, "Concurrent Install RPCs", t)

			// Continue with the existing stream.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrEnd{},
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			// Receive Validated response.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetValidated() == nil {
				t.Fatal("Did not receive expected Validated response.")
			}
		},
	},
	{
		desc: "OSInstallFailsIfWrongMessageIsSent",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Send TrancontrollerrEnd; server expects TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrEnd{},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive error reporting.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected error!")
			}
			// TODO(b/328077908) Alarms to be implemented later
			// expectAlarm(err)
			testErr(err, codes.InvalidArgument, "Expected TrancontrollerrRequest", t)
		},
	},
	{
		desc: "OSInstallAbortedImmediately",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Close the stream immediately.
			stream.CloseSend()

			// Receive error reporting premature closure of the stream.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting on premature closure of the stream.")
			}
			// TODO(b/328077908) Alarms to be implemented later
			// expectAlarm(err)
		},
	},
	{
		desc: "OSInstallFailsIfImageExistsWhenTrancontrollerrBegins",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Send TrancontrollerrRequest.
			version := "os1.1"
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: version,
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrReady response.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetTrancontrollerrReady() == nil {
				t.Fatal("Did not receive expected TrancontrollerrReady response")
			}

			// TrancontrollerrReady initiates trancontrollerrring content. Image must not exist at this point!
			imgPath := s.getVersionPath(version)
			f, err := os.OpenFile(imgPath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatal(err.Error())
			}
			if err := f.Close(); err != nil {
				t.Fatal(err.Error())
			}
			// Cleanup
			defer func() {
				if err := os.Remove(imgPath); err != nil {
					t.Errorf("Error while deleting temporary test file: %v\n", err)
				}
			}()

			// Send TrancontrollerrContent.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrContent{
					TrancontrollerrContent: []byte("unimportant string"),
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive InstallError.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			instErr := resp.GetInstallError()
			if instErr == nil {
				t.Fatal("Expected InstallError!")
			}

			// Receive error reporting.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected error!")
			}
			// TODO(b/328077908) Alarms to be implemented later
			// expectAlarm(err)
		},
	},
	{
		desc: "OSInstallFailsIfStreamClosesInTheMiddleOfTrancontrollerr",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			version := "os1.1"
			// Send TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: version,
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrReady response.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfReady := resp.GetTrancontrollerrReady(); trfReady == nil {
				t.Fatal("Did not receive expected TrancontrollerrReady response")
			}

			// Send TrancontrollerrContent.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrContent{
					TrancontrollerrContent: []byte("unimportant string"),
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrProgress response.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfProg := resp.GetTrancontrollerrProgress(); trfProg == nil {
				t.Fatal("Did not receive expected TrancontrollerrProgress response")
			}

			// Close the stream immediately.
			stream.CloseSend()

			// Receive error reporting premature closure of the stream.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting on premature closure of the stream.")
			}

			// Check incomplete trancontrollerr is removed!
			if s.imageExists(s.getVersionPath(version)) {
				t.Fatal("Incomplete image should have been deleted!")
			}
			// TODO(b/328077908) Alarms to be implemented later
			// expectAlarm(err)
		},
	},
	{
		desc: "OSInstallFailsIfWrongMsgIsSentInTheMiddleOfTrancontrollerr",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			version := "os1.1"
			// Send TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: version,
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrReady response.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfReady := resp.GetTrancontrollerrReady(); trfReady == nil {
				t.Fatal("Did not receive expected TrancontrollerrReady response")
			}

			// Send TrancontrollerrContent.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrContent{
					TrancontrollerrContent: []byte("unimportant string"),
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrProgress response.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfProg := resp.GetTrancontrollerrProgress(); trfProg == nil {
				t.Fatal("Did not receive expected TrancontrollerrProgress response")
			}

			// Send TrancontrollerrRequest again. This is unexpected!
			// Server should send error message, clean up incomplete trancontrollerr.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: version,
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive error reporting.
			_, err = stream.Recv()
			if err == nil {
				t.Fatal("Expected an error reporting on premature closure of the stream.")
			}

			// Check incomplete trancontrollerr is removed!
			if s.imageExists(s.getVersionPath(version)) {
				t.Fatal("Incomplete image should have been deleted!")
			}
		},
	},
	{
		desc: "OSInstallSucceeds",
		f: func(ctx context.Context, t *testing.T, sc ospb.OSClient, s *OSServer) {
			stream, err := sc.Install(ctx, grpc.EmptyCallOption{})
			if err != nil {
				t.Fatal(err.Error())
			}

			version := "os1.1"
			// Send TrancontrollerrRequest.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrRequest{
					TrancontrollerrRequest: &ospb.TrancontrollerrRequest{
						Version: version,
					},
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrReady response.
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfReady := resp.GetTrancontrollerrReady(); trfReady == nil {
				t.Fatal("Did not receive expected TrancontrollerrReady response")
			}

			data := []byte("unimportant string")
			// Send TrancontrollerrContent.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrContent{
					TrancontrollerrContent: data,
				},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive TrancontrollerrProgress response.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if trfProg := resp.GetTrancontrollerrProgress(); trfProg == nil {
				t.Fatal("Did not receive expected TrancontrollerrProgress response")
			}

			// Send TrancontrollerrEnd.
			err = stream.Send(&ospb.InstallRequest{
				Request: &ospb.InstallRequest_TrancontrollerrEnd{},
			})
			if err != nil {
				t.Fatal(err.Error())
			}

			// Receive Validated response.
			resp, err = stream.Recv()
			if err != nil {
				t.Fatal(err.Error())
			}
			if resp.GetValidated() == nil {
				t.Fatal("Did not receive expected Validated response.")
			}

			// Sanity check!
			imgPath := s.getVersionPath(version)
			dataRead, err := os.ReadFile(imgPath)
			if err != nil {
				t.Fatal(err.Error())
			}
			if string(data) != string(dataRead) {
				t.Fatal("Content doesn't match!")
			}

			// Cleanup
			if err = os.Remove(imgPath); err != nil {
				t.Errorf("Error while deleting temporary test file: %v\n", err)
			}
		},
	},
}

// TestOSServer tests implementation of gnoi.OS server.
func TestOSServer(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()

	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range testOSCases {
		t.Run(test.desc, func(t *testing.T) {
			conn, err := grpc.Dial(targetAddr, opts...)
			if err != nil {
				t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
			}
			defer conn.Close()

			sc := ospb.NewOSClient(conn)
			test.f(ctx, t, sc, &OSServer{Server: s})
		})
	}

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("InstallUnavailableDuringFreeze", func(t *testing.T) {
		sc := ospb.NewOSClient(conn)
		stream, err := sc.Install(context.Background(), grpc.EmptyCallOption{})
		if err != nil {
			t.Fatal(err.Error())
		}
		if err = stream.Send(&ospb.InstallRequest{}); err != nil {
			t.Fatal(err.Error())
		}
		_, err = stream.Recv()
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("ActivateUnavailableDuringFreeze", func(t *testing.T) {
		sc := ospb.NewOSClient(conn)
		_, err = sc.Activate(context.Background(), &ospb.ActivateRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("VerifyUnavailableDuringFreeze", func(t *testing.T) {
		sc := ospb.NewOSClient(conn)
		_, err = sc.Verify(context.Background(), &ospb.VerifyRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)
}

func testErr(err error, code codes.Code, pattern string, t *testing.T) {
	t.Helper()
	if err == nil {
		t.Fatal("Expected error condition.")
	}
	e, _ := status.FromError(err)
	if e.Code() != code {
		t.Error("Error code: expected ", code, ", received ", e.Code())
	}
	res, _ := regexp.MatchString(pattern, e.Message())
	if !res {
		t.Error("Error message: expected ", pattern, ", received ", e.Message())
	}
}
