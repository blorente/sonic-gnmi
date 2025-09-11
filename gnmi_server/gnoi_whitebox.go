package gnmi

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	wbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/whitebox"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	json "google.golang.org/protobuf/encoding/protojson"
)

const (
	fIKey         = "FailureInject"
	failureReqCh  = "FAILURE_INJECTION_REQUEST_CHANNEL"
	failureRespCh = "FAILURE_INJECTION_RESPONSE_CHANNEL"
	notifTimeout  = 30 * time.Second
)

// SetControllterConnectionState implements the corresponding RPC.
func (srv *Server) SetControllerConnectionState(ctx context.Context, req *wbpb.SetControllerConnectionStateRequest) (*wbpb.SetControllerConnectionStateResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI whitebox SetControllerConnectionState RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI whitebox SetControllerConnectionState RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("Whitebox Test: SetControllerConnectionState")
	// DBUS client client takes a string and packages in an array for the backend.
	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}

	// DBUS client client takes a string and packages in an array for the backend.
	if _, err = sc.WhiteboxSet(string(reqStr)); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &wbpb.SetControllerConnectionStateResponse{}, nil
}

func sendFailureReqOnNotifCh(ctx context.Context, req *wbpb.InjectFailureRequest, sc *redis.Client, failureNotifKey string) (resp proto.Message, err error, msgDataStr string) {
	np, err := common_utils.NewNotificationProducer(failureReqCh)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	defer np.Close()

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), failureRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	defer sub.Close()
	channel := sub.Channel()

	reqStr, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	// Publish to notification channel.
	if err := np.Send(failureNotifKey, "", map[string]string{dataMsgFld: string(reqStr)}); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}

	// Wait for response on FAILURE_INJECTION_RESPONSE_CHANNEL.
	tc := time.After(notifTimeout)
	var tErr error
	log.V(lvl.INFO).Infof("Waiting for response Notification from failure injection daemon")
	for {
		select {
		case msg := <-channel:
			op, data, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				return nil, status.Errorf(codes.Internal, fmt.Sprintf("Error while receiving Response Notification: [%s] for message [%s]", err.Error(), msg)), msgDataStr
			}

			if op != failureNotifKey {
				log.V(lvl.INFO).Infof("Op: %v doesn't match for %v!", op, failureNotifKey)
				tErr = status.Errorf(codes.Internal, fmt.Sprintf("Op: %v doesn't match for %v!", op, failureNotifKey))
				continue
			}
			if fvs != nil {
				if _, ok := fvs[dataMsgFld]; ok {
					msgDataStr = fvs[dataMsgFld]
				}
			}
			if swssCode := swssToErrorCode(data); swssCode != codes.OK {
				errStr := fmt.Sprintf("Failure Injection Notification returned SWSS Error code: %v, error = %v", swssCode, msgDataStr)
				log.V(lvl.INFO).Infof(errStr)
				return nil, status.Errorf(swssCode, errStr), msgDataStr
			}
			return &wbpb.InjectFailureResponse{}, nil, msgDataStr

		case <-tc:
			// Crossed the failure injection response notification timeout.
			log.V(lvl.ERROR).Infof("Response Notification timeout from failure injection daemon!")
			if tErr == nil {
				tErr = status.Errorf(codes.Internal, "Response Notification timeout from failure injection daemon!")
			}
			return nil, tErr, msgDataStr
		}
	}
}

// InjectFailure implements the corresponding RPC.
func (srv *Server) InjectFailure(ctx context.Context, req *wbpb.InjectFailureRequest) (*wbpb.InjectFailureResponse, error) {
	log.V(2).Info("Whitebox Test: InjectFailure")
	// Initialize State DB.
	rclient := db.TransactionalRedisClient(db.StateDB)
	defer db.CloseRedisClient(rclient)
	resp, err, _ := sendFailureReqOnNotifCh(ctx, req, rclient, fIKey)
	if err != nil || resp == nil {
		return nil, err
	}
	return resp.(*wbpb.InjectFailureResponse), nil
}
