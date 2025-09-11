package gnmi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"

	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ocsSubscriptionRespChan chan *redis.Message

// Mock subscription channel provider
type mockOcsSubscriptionChannelProvider struct{}

func (t mockOcsSubscriptionChannelProvider) Channel(pubsub *redis.PubSub) <-chan *redis.Message {
	return ocsSubscriptionRespChan
}

func TestOcs(t *testing.T) {
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

	oc := ocs_pb.NewOcsClient(conn)

	t.Run("SetConnectionsSuccess", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		expectedResp := &ocs_pb.SetConnectionsResponse{
			Success: proto.Bool(true),
			Results: []*ocs_pb.SetConnectionsResponse_Result{
				{
					Update: &ocs_pb.Update{
						Action: ocs_pb.Update_CONNECT.Enum(),
						Connection: &ocs_pb.Connection{
							North: proto.String("1N"),
							South: proto.String("1S"),
						},
					},
					Status: &status.Status{Code: *proto.Int32(int32(codes.OK))},
				},
			},
		}
		respJsonBytes, err := protojson.Marshal(expectedResp)
		respPayload, err := json.Marshal([]string{"SetConnections", "SWSS_RC_SUCCESS", "MESSAGE", string(respJsonBytes)})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		resp, err := oc.SetConnections(ctx, req)
		if err != nil {
			t.Fatal("Expected success, got error instead: %e", err)
		}

		if !proto.Equal(resp, expectedResp) {
			t.Fatal("Expected ocs response proto message to match expected response. Returned message was '%s'. Expected '%s'", resp, expectedResp)
		}
	})

	t.Run("SetConnectionsFailsProcessMsgPayload", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: "not]a{json[array}",
		}

		_, err := oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "processing message payload", t)
	})

	t.Run("SetConnectionsIgnoresOpMismatchThenReturnsNextResponse", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		firstRespPayload, _ := json.Marshal([]string{"GetConnections", "SWSS_RC_SUCCESS", "MESSAGE", ""})

		expectedResp := &ocs_pb.SetConnectionsResponse{
			Success: proto.Bool(true),
			Results: []*ocs_pb.SetConnectionsResponse_Result{
				{
					Update: &ocs_pb.Update{
						Action: ocs_pb.Update_CONNECT.Enum(),
						Connection: &ocs_pb.Connection{
							North: proto.String("1N"),
							South: proto.String("1S"),
						},
					},
					Status: &status.Status{Code: *proto.Int32(int32(codes.OK))},
				},
			},
		}
		respJsonBytes, _ := protojson.Marshal(expectedResp)
		secondRespPayload, _ := json.Marshal([]string{"SetConnections", "SWSS_RC_SUCCESS", "MESSAGE", string(respJsonBytes)})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 2)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(firstRespPayload),
		}
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(secondRespPayload),
		}

		resp, err := oc.SetConnections(ctx, req)
		if err != nil {
			t.Fatal("Expected success, got error instead: %e", err)
		}

		if !proto.Equal(resp, expectedResp) {
			t.Fatal("Expected ocs response proto message to match expected response. Returned message was '%s'. Expected '%s'", resp, expectedResp)
		}
	})

	t.Run("SetConnectionsFailBadSwssCodeInData", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		respPayload, _ := json.Marshal([]string{"SetConnections", "foobar"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}
		_, err = oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "SWSS Error code", t)
	})

	t.Run("SetConnectionsFailsBackendError", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		respPayload, err := json.Marshal([]string{"SetConnections", "SWSS_RC_INTERNAL", "MESSAGE", "Internal backend error"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "Internal backend error", t)
	})

	t.Run("SetConnectionsFailsEmptyResponseMessage", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		respPayload, err := json.Marshal([]string{"SetConnections", "SWSS_RC_SUCCESS", "MESSAGE", ""})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "empty response", t)
	})

	t.Run("SetConnectionsFailsResponseUnmarshalFailure", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		respPayload, err := json.Marshal([]string{"SetConnections", "SWSS_RC_SUCCESS", "MESSAGE", "garbage"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "unmarshalling response protojson", t)
	})

	t.Run("SetConnectionsResponseTimeout", func(t *testing.T) {
		req := &ocs_pb.SetConnectionsRequest{
			Target: proto.String("switch"),
			Updates: []*ocs_pb.Update{
				{
					Action: ocs_pb.Update_CONNECT.Enum(),
					Connection: &ocs_pb.Connection{
						North: proto.String("1N"),
						South: proto.String("1S"),
					},
				},
			},
		}

		_, err := oc.SetConnections(ctx, req)
		testErr(err, codes.Internal, "timeout", t)
	})

	t.Run("GetConnectionsSuccess", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		expectedResp := &ocs_pb.GetConnectionsResponse{
			Connection: []*ocs_pb.Connection{
				{
					North: proto.String("1N"),
					South: proto.String("1S"),
				},
			},
		}
		respJsonBytes, err := protojson.Marshal(expectedResp)
		respPayload, err := json.Marshal([]string{"GetConnections", "SWSS_RC_SUCCESS", "MESSAGE", string(respJsonBytes)})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		resp, err := oc.GetConnections(ctx, req)
		if err != nil {
			t.Fatal("Expected success, got error instead: %e", err)
		}

		if !proto.Equal(resp, expectedResp) {
			t.Fatal("Expected ocs response proto message to match expected response. Returned message was '%s'. Expected '%s'", resp, expectedResp)
		}
	})

	t.Run("GetConnectionsFailsProcessMsgPayload", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: "not]a{json[array}",
		}

		_, err := oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "processing message payload", t)
	})

	t.Run("GetConnectionsIgnoresOpMismatchThenReturnsNextResponse", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		firstRespPayload, _ := json.Marshal([]string{"SetConnections", "SWSS_RC_SUCCESS", "MESSAGE", ""})

		expectedResp := &ocs_pb.GetConnectionsResponse{
			Connection: []*ocs_pb.Connection{
				{
					North: proto.String("1N"),
					South: proto.String("1S"),
				},
			},
		}
		respJsonBytes, _ := protojson.Marshal(expectedResp)
		secondRespPayload, _ := json.Marshal([]string{"GetConnections", "SWSS_RC_SUCCESS", "MESSAGE", string(respJsonBytes)})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 2)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(firstRespPayload),
		}
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(secondRespPayload),
		}

		resp, err := oc.GetConnections(ctx, req)
		if err != nil {
			t.Fatalf("Expected success, got error instead: %v", err)
		}

		if !proto.Equal(resp, expectedResp) {
			t.Fatalf("Expected ocs response proto message to match expected response. Returned message was '%s'. Expected '%s'", resp, expectedResp)
		}
	})

	t.Run("GetConnectionsFailBadSwssCodeInData", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		respPayload, _ := json.Marshal([]string{"GetConnections", "foobar"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}
		_, err := oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "SWSS Error code", t)
	})

	t.Run("GetConnectionsFailsBackendError", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		respPayload, err := json.Marshal([]string{"GetConnections", "SWSS_RC_INTERNAL", "MESSAGE", "Internal backend error"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "Internal backend error", t)
	})

	t.Run("GetConnectionsFailsEmptyResponseMessage", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		respPayload, err := json.Marshal([]string{"GetConnections", "SWSS_RC_SUCCESS", "MESSAGE", ""})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "empty response", t)
	})

	t.Run("GetConnectionsFailsResponseUnmarshalFailure", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		respPayload, err := json.Marshal([]string{"GetConnections", "SWSS_RC_SUCCESS", "MESSAGE", "garbage"})

		ocsSubscriptionChannelProvider = mockOcsSubscriptionChannelProvider{}
		ocsSubscriptionRespChan = make(chan *redis.Message, 1)
		ocsSubscriptionRespChan <- &redis.Message{
			Channel: ocsRespCh,
			Pattern: ocsRespCh,
			Payload: string(respPayload),
		}

		_, err = oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "unmarshalling response protojson", t)
	})

	t.Run("GetConnectionsResponseTimeout", func(t *testing.T) {
		req := &ocs_pb.GetConnectionsRequest{
			Target: proto.String("switch"),
		}

		_, err := oc.GetConnections(ctx, req)
		testErr(err, codes.Internal, "timeout", t)
	})
}
