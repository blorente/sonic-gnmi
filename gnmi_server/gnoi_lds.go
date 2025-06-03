package gnmi

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	"github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	ldsCommandReqChFmt  string = "LDS_Request_Channel_Internal.%d"
	ldsCommandRespChFmt string = "LDS_Response_Channel_Internal.%d"

	ldsCommandTimeout time.Duration = 30 * time.Second
)

type GNOILdsServer struct {
	*Server
	ldsRequestIndex uint32
	mu              sync.Mutex
}

func NewGNOILdsServer(srv *Server) *GNOILdsServer {
	return &GNOILdsServer{
		Server:          srv,
		ldsRequestIndex: 0,
	}
}

// Interface for providing a subscription channel from a redis pubsub. Used to mock the subscription channel responses in testing.
type subscriptionChannelProviderInterface interface {
	Channel(*redis.PubSub) <-chan *redis.Message
}

type subscriptionChannelProviderImpl struct{}

func (t subscriptionChannelProviderImpl) Channel(pubsub *redis.PubSub) <-chan *redis.Message {
	return pubsub.Channel()
}

var ldsSubscriptionChannelProvider subscriptionChannelProviderInterface

func init() {
	ldsSubscriptionChannelProvider = subscriptionChannelProviderImpl{}
}

// Get implements the corresponding RPC.
func (srv *GNOILdsServer) LdsCommand(ctx context.Context, req *ocs.LdsCommandRequest) (*ocs.LdsCommandResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Info("gNOI: lds.LdsCommand")

	// Setup Redis client
	cfgDB, err := getRedisDBClient(configDB) // DB number is irrelvant
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer cfgDB.Close()

	// Setup lds command channel notifier
	srv.mu.Lock()
	requestIdx := srv.ldsRequestIndex
	srv.ldsRequestIndex++
	srv.mu.Unlock()
	requestChannel := fmt.Sprintf(ldsCommandReqChFmt, requestIdx)
	respChannel := fmt.Sprintf(ldsCommandRespChFmt, requestIdx)
	np, err := common_utils.NewNotificationProducer(requestChannel)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer np.Close()

	// Subscribe to the response channel.
	sub := cfgDB.Subscribe(context.Background(), respChannel)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	channel := ldsSubscriptionChannelProvider.Channel(sub)

	// Publish to notification channel.
	serializedReq, err := proto.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	encodedReq := base64.StdEncoding.EncodeToString(serializedReq)
	if err := np.SendRaw(encodedReq); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	tc := time.After(ldsCommandTimeout)
	for {
		select {
		case msg := <-channel:
			log.V(lvl.DEBUG).Infof("Received from channel: %s", msg.Payload)

			decodedBytes, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid base64 encoded prototext received from LdsCommandResponse: %s", msg.Payload)
			}

			var decodedMessage ocs.LdsCommandResponse
			err = proto.Unmarshal(decodedBytes, &decodedMessage)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Unable to unmarshal proto binary to LdsCommandResponse. Base64 payload was %s, Error %e", msg.Payload, err)
			}
			return &decodedMessage, nil

		case <-tc:
			return nil, status.Errorf(codes.Internal, "LdsCommand operation failed due to no response after timeout of %v.", ldsCommandTimeout)
		}
	}
}
