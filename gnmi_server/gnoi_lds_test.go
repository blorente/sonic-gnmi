package gnmi

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	ocs_pb "github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

var ldsSubscriptionRespChan chan *redis.Message

// Mock subscription channel provider
type mockLdsSubscriptionChannelProvider struct{}

func (t mockLdsSubscriptionChannelProvider) Channel(pubsub *redis.PubSub) <-chan *redis.Message {
	return ldsSubscriptionRespChan
}

func TestLds(t *testing.T) {
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

	sc := ocs_pb.NewLdsClient(conn)

	t.Run("LdsCommandSuccess", func(t *testing.T) {
		req := &ocs_pb.LdsCommandRequest{
			Command: &ocs_pb.LdsCommandRequest_MeasurementCommand{
				Operation:     ocs_pb.LdsCommandRequest_MEASURE_RETURN_LOSS.Enum(),
				PortNumber:    proto.Int32(5),
				LdsPortNumber: proto.Int32(5),
			},
		}

		expectedResp := &ocs_pb.LdsCommandResponse{
			Result: &ocs_pb.LdsCommandResponse_MeasurementResult{
				CommandResult: &ocs_pb.LdsCommandResponse_LdsCommandResult{
					Success: proto.Bool(true),
					RxPower: proto.Float64(10.0),
					LdsPortPair: &ocs_pb.LdsCommandResponse_LdsCommandResult_LdsPortPair{
						PortNumber:    proto.Int32(5),
						LdsPortNumber: proto.Int32(5),
					},
				},
			},
		}
		encodedResponseProtoBytes, err := proto.Marshal(expectedResp)

		ldsSubscriptionChannelProvider = mockLdsSubscriptionChannelProvider{}
		ldsSubscriptionRespChan = make(chan *redis.Message, 1)
		ldsSubscriptionRespChan <- &redis.Message{
			Channel: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Pattern: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Payload: base64.StdEncoding.EncodeToString(encodedResponseProtoBytes),
		}

		resp, err := sc.LdsCommand(ctx, req)

		if err != nil {
			t.Fatal("Expected success, got error instead: %e", err)
		}

		if !proto.Equal(resp, expectedResp) {
			t.Fatal("Expected lds response proto message to match expected response. Returned message was '%s'. Expected '%s'", resp, expectedResp)
		}
	})

	t.Run("LdsCommandFailsBase64Decode", func(t *testing.T) {
		req := &ocs_pb.LdsCommandRequest{
			Command: &ocs_pb.LdsCommandRequest_MeasurementCommand{
				Operation:     ocs_pb.LdsCommandRequest_MEASURE_RETURN_LOSS.Enum(),
				PortNumber:    proto.Int32(5),
				LdsPortNumber: proto.Int32(5),
			},
		}

		ldsSubscriptionChannelProvider = mockLdsSubscriptionChannelProvider{}
		ldsSubscriptionRespChan = make(chan *redis.Message, 1)
		ldsSubscriptionRespChan <- &redis.Message{
			Channel: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Pattern: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Payload: "NotAValidBase64String)*&^%$##@@",
		}

		_, err := sc.LdsCommand(ctx, req)
		testErr(err, codes.InvalidArgument, "base64", t)
	})

	t.Run("LdsCommandFailsPrototextUnmarshal", func(t *testing.T) {
		req := &ocs_pb.LdsCommandRequest{
			Command: &ocs_pb.LdsCommandRequest_MeasurementCommand{
				Operation:     ocs_pb.LdsCommandRequest_MEASURE_RETURN_LOSS.Enum(),
				PortNumber:    proto.Int32(5),
				LdsPortNumber: proto.Int32(5),
			},
		}
		ldsSubscriptionChannelProvider = mockLdsSubscriptionChannelProvider{}
		ldsSubscriptionRespChan = make(chan *redis.Message, 1)
		ldsSubscriptionRespChan <- &redis.Message{
			Channel: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Pattern: fmt.Sprintf(ldsCommandRespChFmt, 1),
			Payload: base64.StdEncoding.EncodeToString([]byte("Not a valid proto binary")),
		}

		_, err := sc.LdsCommand(ctx, req)
		testErr(err, codes.InvalidArgument, "marshal", t)
	})

	t.Run("LdsCommandResponseTimeout", func(t *testing.T) {
		req := &ocs_pb.LdsCommandRequest{
			Command: &ocs_pb.LdsCommandRequest_MeasurementCommand{
				Operation:     ocs_pb.LdsCommandRequest_MEASURE_RETURN_LOSS.Enum(),
				PortNumber:    proto.Int32(5),
				LdsPortNumber: proto.Int32(5),
			},
		}

		_, err := sc.LdsCommand(ctx, req)
		testErr(err, codes.Internal, "timeout", t)
	})
}
