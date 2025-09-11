package gnmi

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pjson "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	"github.com/sonic-net/sonic-gnmi/proto/gnoi/ocs"

	db "github.com/Azure/sonic-mgmt-common/translib/db"
)

type gnoiOcsOp string

const (
	setConnectionsOp gnoiOcsOp = "SetConnections"
	getConnectionsOp gnoiOcsOp = "GetConnections"
	ocsReqCh         string    = "OCS_Request_Channel"
	ocsRespCh        string    = "OCS_Response_Channel"

	ocsTimeout         time.Duration = 30 * time.Second
	logTruncationLimit int           = 100
)

type GNOIOcsServer struct {
	*Server
	mu sync.Mutex
}

func NewGNOIOcsServer(srv *Server) *GNOIOcsServer {
	return &GNOIOcsServer{
		Server: srv,
	}
}

var ocsSubscriptionChannelProvider subscriptionChannelProviderInterface

func init() {
	ocsSubscriptionChannelProvider = subscriptionChannelProviderImpl{}
}

func logAndReturnErrorf(format string, a ...any) error {
	err_str := fmt.Sprintf(format, a...)
	log.ErrorDepth(1, err_str)
	return status.Errorf(codes.Internal, err_str)
}

type ProtoMessageConstraint[T any] interface {
	proto.Message
	*T
}

func handleRequest[ReqT any, RespT any, Req ProtoMessageConstraint[ReqT], Resp ProtoMessageConstraint[RespT]](srv *GNOIOcsServer, ctx context.Context, ocsRpcOp gnoiOcsOp, req proto.Message) (Resp, error) {
	// Only one set/get connections request can be serviced by the stack at a time.
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	var requestString string
	switch req.ProtoReflect().Descriptor().Name() {
	case "SetConnectionsRequest":
		requestString = req.(*ocs.SetConnectionsRequest).String()
	case "GetConnectionsRequest":
		requestString = req.(*ocs.GetConnectionsRequest).String()
	}

	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, requestString, false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)

	log.V(lvl.INFO).Infof("gNOI: ocs.%s", ocsRpcOp)

	// Setup Redis client
	sc := db.TransactionalRedisClient(db.StateDB)
	defer sc.Close()

	// Setup ocs command channel notifier
	np, err := common_utils.NewNotificationProducer(ocsReqCh)
	if err != nil {
		return nil, logAndReturnErrorf("gNOI: ocs.%s: Error in creating notification producer: %v", ocsRpcOp, err.Error())
	}
	defer np.Close()

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), ocsRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, logAndReturnErrorf("gNOI: ocs.%s: Error in setting up subscription to response channel: %v", ocsRpcOp, err.Error())
	}
	channel := ocsSubscriptionChannelProvider.Channel(sub)

	// Publish to notification channel.
	reqStr, err := pjson.Marshal(req)
	if err != nil {
		return nil, logAndReturnErrorf("gNOI: ocs.%s: Error in marshalling JSON: %v", ocsRpcOp, err.Error())
	}
	if err := np.Send(string(ocsRpcOp), "", map[string]string{dataMsgFld: string(reqStr)}); err != nil {
		return nil, logAndReturnErrorf("gNOI: ocs.%s: Error in sending request to notification channel: %v", ocsRpcOp, err.Error())
	}

	tc := time.After(ocsTimeout)
	for {
		select {
		case msg := <-channel:
			op, data, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				return nil, logAndReturnErrorf("gNOI: ocs.%s: Error while processing message payload [%v]: %v", ocsRpcOp, msg, err.Error())
			}
			log.V(lvl.INFO).Infof("gNOI: ocs.%s: Received on the %s channel: op = [%v], data = [%v], fvs (truncated) = [%.*v]", ocsRpcOp, ocsRespCh, op, data, logTruncationLimit, fvs)

			if op != string(ocsRpcOp) {
				log.V(lvl.WARNING).Infof("gNOI: ocs.%s: Received op [%v] doesn't match", ocsRpcOp, op)
				continue
			}

			// Check for error within RPC response.
			msgDataStr := fvs[dataMsgFld]
			if swssCode := swssToErrorCode(data); swssCode != codes.OK {
				return nil, logAndReturnErrorf("gNOI: ocs.%s: Error while processing SWSS Error code [%v]: %v", ocsRpcOp, data, msgDataStr)
			}

			// Deserialize and return response.
			if msgDataStr == "" {
				return nil, logAndReturnErrorf("gNOI: ocs.%s: Received empty response content", ocsRpcOp)
			}
			var resp Resp = new(RespT)
			if err := pjson.Unmarshal([]byte(msgDataStr), resp); err != nil {
				return nil, logAndReturnErrorf("gNOI: ocs.%s: Error in unmarshalling response protojson (truncated) [%.*v]: [%v]", ocsRpcOp, logTruncationLimit, msgDataStr, err.Error())
			}
			log.V(lvl.INFO).Infof("gNOI: ocs.%s: Returning response (truncated) [%.*v]", ocsRpcOp, logTruncationLimit, resp)
			return resp, nil

		case <-tc:
			// Crossed the ocs response notification timeout.
			return nil, logAndReturnErrorf("gNOI: ocs.%s: No response before timeout (%v)!", ocsRpcOp, ocsTimeout)
		}
	}
}

// SetConnections implements the corresponding RPC.
func (srv *GNOIOcsServer) SetConnections(ctx context.Context, req *ocs.SetConnectionsRequest) (*ocs.SetConnectionsResponse, error) {
	req.String()
	return handleRequest[ocs.SetConnectionsRequest, ocs.SetConnectionsResponse](srv, ctx, setConnectionsOp, req)
}

// GetConnections implements the corresponding RPC.
func (srv *GNOIOcsServer) GetConnections(ctx context.Context, req *ocs.GetConnectionsRequest) (*ocs.GetConnectionsResponse, error) {
	return handleRequest[ocs.GetConnectionsRequest, ocs.GetConnectionsResponse](srv, ctx, getConnectionsOp, req)
}
