package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	wbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/whitebox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func failureInjectionResponse(t *testing.T, sc *redis.Client, expectedResponse codes.Code, fvs map[string]string, done chan bool, key string) {
	sub := sc.Subscribe(context.Background(), "FAILURE_INJECTION_REQUEST_CHANNEL")
	if _, err := sub.Receive(context.Background()); err != nil {
		t.Errorf("failureInjectionResponse failed to subscribe to request channel: %v", err)
		return
	}
	defer sub.Close()
	channel := sub.Channel()

	np, err := common_utils.NewNotificationProducer("FAILURE_INJECTION_RESPONSE_CHANNEL")
	if err != nil {
		t.Errorf("failureInjectionResponse failed to create notification producer: %v", err)
		return
	}
	defer np.Close()

	tc := time.After(5 * time.Second)
	select {
	case msg := <-channel:
		t.Logf("failureInjectionResponse received request: %v", msg)
		// Respond to the request
		if err := np.Send(key, errorCodeToSwss(expectedResponse), fvs); err != nil {
			t.Errorf("failureInjectionResponse failed to send response: %v", err)
			return
		}
	case <-done:
		return
	case <-tc:
		t.Error("failureInjectionResponse timed out waiting for request")
		return
	}
}

// Tests WhiteBox Test services.
func TestGnoiWhitebox(t *testing.T) {
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

	sc := wbpb.NewWhiteBoxTestClient(conn)
	rclient := db.TransactionalRedisClient(db.StateDB)
	defer db.CloseRedisClient(rclient)

	t.Run("SetControllerConnectionStateFailsAsBackEndIsUnimplemented", func(t *testing.T) {
		if _, err := sc.SetControllerConnectionState(ctx, &wbpb.SetControllerConnectionStateRequest{}); err == nil {
			t.Errorf("SetControllerConnectionState should have failed")
		}
	})

	// Test RPCs during NSF freeze mode.
	s.WarmRestartHelper.SetFreezeStatus(true)
	t.Run("SetControllerConnectionStateUnavailableDuringFreeze", func(t *testing.T) {
		_, err := sc.SetControllerConnectionState(ctx, &wbpb.SetControllerConnectionStateRequest{})
		testErr(err, codes.Unavailable, "RPC disabled since NSF is ongoing!", t)
	})
	s.WarmRestartHelper.SetFreezeStatus(false)

	t.Run("FailureInjectionFailsWithTimeout", func(t *testing.T) {
		req := &wbpb.InjectFailureRequest{
			Failures: []*wbpb.InjectFailureRequest_Failure{},
		}

		_, err := sc.InjectFailure(ctx, req)
		testErr(err, codes.Internal, "Response Notification timeout from failure injection daemon!", t)
	})
	t.Run("FailureInjectionFailsWithWrongKey", func(t *testing.T) {
		// Start goroutine for mock Failure Injection Daemon to respond to RebootStatus requests
		done := make(chan bool, 1)
		fvs := make(map[string]string)
		fvs["MESSAGE"] = "{}"
		go failureInjectionResponse(t, rclient, codes.OK, fvs, done, "testKey")
		defer func() { done <- true }()

		req := &wbpb.InjectFailureRequest{
			Failures: []*wbpb.InjectFailureRequest_Failure{},
		}
		_, err := sc.InjectFailure(ctx, req)
		testErr(err, codes.Internal, "Op: testKey doesn't match for FailureInject!", t)
	})
	t.Run("FailureInjectionFailsWithExpectedBackendErrorCode", func(t *testing.T) {
		fvs := make(map[string]string)
		fvs["MESSAGE"] = "{}"
		for _, code := range []codes.Code{codes.Unavailable, codes.InvalidArgument, codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.ResourceExhausted, codes.Unimplemented, codes.Internal, codes.FailedPrecondition, codes.Unavailable} {
			// Start goroutine for mock Failure Injection Daemon to respond to RebootStatus requests
			done := make(chan bool, 1)
			go failureInjectionResponse(t, rclient, code, fvs, done, fIKey)
			defer func() { done <- true }()

			req := &wbpb.InjectFailureRequest{
				Failures: []*wbpb.InjectFailureRequest_Failure{},
			}
			_, err := sc.InjectFailure(ctx, req)
			testErr(err, code, "Failure Injection Notification returned SWSS Error code", t)
		}
	})
	t.Run("FailureInjectionFailsWithUnexpectedBackendErrorCode", func(t *testing.T) {
		fvs := make(map[string]string)
		fvs["MESSAGE"] = "{}"
		// Start goroutine for mock Failure Injection Daemon to respond to RebootStatus requests
		done := make(chan bool, 1)
		go failureInjectionResponse(t, rclient, codes.Unauthenticated, fvs, done, fIKey)
		defer func() { done <- true }()

		req := &wbpb.InjectFailureRequest{
			Failures: []*wbpb.InjectFailureRequest_Failure{},
		}
		_, err := sc.InjectFailure(ctx, req)
		testErr(err, codes.Internal, "Failure Injection Notification returned SWSS Error code: Internal", t)
	})
	t.Run("InjectFailureReturnsSuccess", func(t *testing.T) {
		// Start goroutine for mock Failure Injection Daemon to respond to RebootStatus requests
		done := make(chan bool, 1)
		fvs := make(map[string]string)
		fvs["MESSAGE"] = "Injecting Failure Complete"
		go failureInjectionResponse(t, rclient, codes.OK, fvs, done, fIKey)
		defer func() { done <- true }()

		req := &wbpb.InjectFailureRequest{
			Failures: []*wbpb.InjectFailureRequest_Failure{},
		}
		_, err := sc.InjectFailure(ctx, req)
		if err != nil {
			t.Fatal("Expected success, got error: ", err.Error())
		}
	})
}
