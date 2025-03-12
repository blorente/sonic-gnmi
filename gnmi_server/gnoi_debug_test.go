package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	debug_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/debug"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debugTests = []struct {
	desc       string
	dbusCaller ssc.Caller
	f          func(t *testing.T, ctx context.Context, c debug_pb.DebugClient)
}{
	{
		desc:       "Invalid commands",
		dbusCaller: &ssc.FakeDbusCaller{},
		f: func(t *testing.T, ctx context.Context, c debug_pb.DebugClient) {
			_, err := c.TunnelCommand(ctx, &debug_pb.TunnelCommandRequest{
				Command: "Invalid",
			})
			if err == nil {
				t.Fatal("Expected an error for invalid command.")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Unexpected error: %v", err)
			}

			_, err = c.TunnelCommand(ctx, &debug_pb.TunnelCommandRequest{
				Command: "show",
				Arg:     [][]byte{[]byte("interfaces"), []byte("&&"), []byte("reboot")},
			})
			if err == nil {
				t.Fatal("Expected an error for invalid command.")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Unexpected error: %v", err)
			}

			_, err = c.TunnelCommand(ctx, &debug_pb.TunnelCommandRequest{
				Command: "show",
				Arg:     [][]byte{[]byte("`reboot`")},
			})
			if err == nil {
				t.Fatal("Expected an error for invalid command.")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Unexpected error: %v", err)
			}
		},
	},
	{
		desc:       "Command output success",
		dbusCaller: &ssc.FakeDbusCaller{},
		f: func(t *testing.T, ctx context.Context, c debug_pb.DebugClient) {
			const expOutput = "org.SONiC.HostService.gpins_infra_host.exec_cmd [show ?]"

			resp, err := c.TunnelCommand(ctx, &debug_pb.TunnelCommandRequest{
				Command: "show",
				Arg:     [][]byte{[]byte("?")},
			})
			if err != nil {
				t.Fatalf("TunnelCommand failed: %v", err)
			}
			if resp.GetStatus() != 0 {
				t.Fatalf("Wrong status. Expect %v got %v", 0, resp.GetStatus())
			}
			if string(resp.GetStdout()) != expOutput {
				t.Fatalf("Wrong stdout. Expect %v got %v", expOutput, string(resp.GetStdout()))
			}
			if string(resp.GetStderr()) != "" {
				t.Fatalf("Wrong stderr. Expect %v got %v", "", string(resp.GetStderr()))
			}

		},
	},
	{
		desc:       "Command output error",
		dbusCaller: &ssc.FailDbusCaller{},
		f: func(t *testing.T, ctx context.Context, c debug_pb.DebugClient) {
			const expOutput = "org.SONiC.HostService.gpins_infra_host.exec_cmd [show --help]"

			resp, err := c.TunnelCommand(ctx, &debug_pb.TunnelCommandRequest{
				Command: "show",
				Arg:     [][]byte{[]byte("--help")},
			})
			if err != nil {
				t.Fatalf("TunnelCommand failed: %v", err)
			}
			if resp.GetStatus() != 1 {
				t.Fatalf("Wrong status. Expect %v got %v", 1, resp.GetStatus())
			}
			if string(resp.GetStdout()) != "" {
				t.Fatalf("Wrong stdout. Expect %v got %v", "", string(resp.GetStdout()))
			}
			if string(resp.GetStderr()) != expOutput {
				t.Fatalf("Wrong stderr. Expect %v got %v", expOutput, string(resp.GetStderr()))
			}
			time.Sleep(time.Second * 2)
		},
	},
}

// TestDebugServer tests implementation of gnoi.Debug server.
func TestGnoiDebugServer(t *testing.T) {
	s := createServer(t)
	go runServer(t, s)
	defer s.Stop()
	defer resetDbusCaller()

	// Create a gNOI.Debug client and connect it to the gNOI.Debug server.
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}
	targetAddr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	conn, err := grpc.Dial(targetAddr, opts...)
	if err != nil {
		t.Fatalf("Dialing to %s failed: %v", targetAddr, err)
	}
	defer conn.Close()
	c := debug_pb.NewDebugClient(conn)

	var mu sync.Mutex
	for _, tc := range debugTests {
		ctx := context.Background()
		t.Run(tc.desc, func(t *testing.T) {
			mu.Lock()
			defer mu.Unlock()
			dbusCaller = tc.dbusCaller
			tc.f(t, ctx, c)
		})
	}

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("TunnelCommandUnavailableDuringFreeze", func(t *testing.T) {
		_, err = c.TunnelCommand(context.Background(), &debug_pb.TunnelCommandRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)
}
