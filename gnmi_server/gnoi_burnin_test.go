package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/sonic-net/sonic-gnmi/common_utils"
	burnin_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/burnin"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

// Mock interface implementation that returns critical state!
type mockSystemStateHelperFailure struct{}

func (t mockSystemStateHelperFailure) Close() {}

func (t mockSystemStateHelperFailure) GetSystemState() common_utils.SystemState {
	return common_utils.SystemCritical
}

func (t mockSystemStateHelperFailure) IsSystemCritical() bool {
	return true
}

func (t mockSystemStateHelperFailure) GetSystemCriticalReason() string {
	return "test"
}

func (t mockSystemStateHelperFailure) AllComponentStates() map[common_utils.SystemComponent]common_utils.ComponentStateInfo {
	return map[common_utils.SystemComponent]common_utils.ComponentStateInfo{}
}

// Tests Burnin services.
func TestBurnin(t *testing.T) {
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

	sc := burnin_pb.NewBurninClient(conn)

	t.Run("StartBurninFailsIfTestDurationIsTooLong", func(t *testing.T) {
		var duration uint64 = 123456
		req := &burnin_pb.StartBurninRequest{
			DurationSeconds:   duration,
			BurninOperationId: testId,
			OnlineBurnin:      false,
		}

		_, err := sc.StartBurnin(ctx, req)
		testErr(err, codes.InvalidArgument, fmt.Sprintf("Duration %d is too long.", duration), t)
	})
	t.Run("StartBurninFailsIfIDIsEmpty", func(t *testing.T) {
		req := &burnin_pb.StartBurninRequest{
			DurationSeconds: 10,
			OnlineBurnin:    false,
		}

		_, err := sc.StartBurnin(ctx, req)
		testErr(err, codes.InvalidArgument, "Burnin operation ID is empty.", t)
	})
	t.Run("StartBurninFailsIfOnlineOperationIsRequested", func(t *testing.T) {
		req := &burnin_pb.StartBurninRequest{
			DurationSeconds:   10,
			BurninOperationId: testId,
			OnlineBurnin:      true,
		}

		_, err := sc.StartBurnin(ctx, req)
		testErr(err, codes.InvalidArgument, "Online burnin operation has not been implemented yet.", t)
	})
	t.Run("StartBurninFailsAsBackEndIsUnimplemented", func(t *testing.T) {
		req := &burnin_pb.StartBurninRequest{
			DurationSeconds:   10,
			BurninOperationId: testId,
			OnlineBurnin:      false,
		}

		_, err := sc.StartBurnin(ctx, req)
		// TODO(b/328077908) Alarms to be implemented later
		// testErr(err, codes.Internal, "Internal SONiC HostService failure", t)
		testErr(err, codes.Internal, "dial unix /var/run/dbus/system_bus_socket: connect: no such file or directory", t)
	})
	t.Run("StopBurninFailsIfIDIsEmpty", func(t *testing.T) {
		_, err := sc.StopBurnin(ctx, &burnin_pb.StopBurninRequest{})
		testErr(err, codes.InvalidArgument, "Burnin operation ID is empty.", t)
	})
	t.Run("StopBurninFailsAsBackEndIsUnimplemented", func(t *testing.T) {
		req := &burnin_pb.StopBurninRequest{
			BurninOperationId: testId,
		}

		_, err := sc.StopBurnin(ctx, req)
		// TODO(b/328077908) Alarms to be implemented later
		// testErr(err, codes.Internal, "Internal SONiC HostService failure", t)
		testErr(err, codes.Internal, "dial unix /var/run/dbus/system_bus_socket: connect: no such file or directory", t)
	})
	t.Run("GetBurninResultFailsIfIDIsEmpty", func(t *testing.T) {
		_, err := sc.GetBurninResult(ctx, &burnin_pb.GetBurninResultRequest{})
		testErr(err, codes.InvalidArgument, "Burnin operation ID is empty.", t)
	})
	t.Run("GetBurninResultFailsAsBackEndIsUnimplemented", func(t *testing.T) {
		req := &burnin_pb.GetBurninResultRequest{
			BurninOperationId: testId,
		}

		_, err := sc.GetBurninResult(ctx, req)
		// TODO(b/328077908) Alarms to be implemented later
		// testErr(err, codes.Internal, "Internal SONiC HostService failure", t)
		testErr(err, codes.Internal, "dial unix /var/run/dbus/system_bus_socket: connect: no such file or directory", t)
	})

	// Simulate helper failure - returns critical error!
	savedSsHelper := s.SsHelper
	s.SsHelper = mockSystemStateHelperFailure{}

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("StartBurninUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StartBurnin(ctx, &burnin_pb.StartBurninRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("StopBurninUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.StopBurnin(ctx, &burnin_pb.StopBurninRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	t.Run("GetBurninResultUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.GetBurninResult(ctx, &burnin_pb.GetBurninResultRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)

	// Simulate helper failure - returns critical error!
	s.SsHelper = mockSystemStateHelperFailure{}

	t.Run("StartBurninFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StartBurnin(ctx, &burnin_pb.StartBurninRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("StopBurninFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.StopBurnin(ctx, &burnin_pb.StopBurninRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})
	t.Run("GetBurninResultFailsIfSystemIsCritical", func(t *testing.T) {
		_, err := sc.GetBurninResult(ctx, &burnin_pb.GetBurninResultRequest{})
		testErr(err, codes.Internal, "System is in critical state: test", t)
	})

	s.SsHelper.Close()
	s.SsHelper = savedSsHelper
}
