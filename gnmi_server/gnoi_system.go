package gnmi

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	syspb "github.com/openconfig/gnoi/system"
	"github.com/openconfig/gnoi/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pjson "google.golang.org/protobuf/encoding/protojson"
)

const (
	rebootKey           = "Reboot"
	rebootStatusKey     = "RebootStatus"
	rebootCancelKey     = "CancelReboot"
	rebootReqCh         = "Reboot_Request_Channel"
	rebootRespCh        = "Reboot_Response_Channel"
	dataMsgFld          = "MESSAGE"
	notificationTimeout = 10 * time.Second
)

type sysXfmrInterface interface {
	resetOptics(string) (string, error)
}

type sysXfmrImpl struct{}

func (t sysXfmrImpl) resetOptics(req string) (string, error) {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return "", err
	}
	return sc.SystemOptics(req)
}

var sysXfmr sysXfmrInterface

func init() {
	sysXfmr = sysXfmrImpl{}
}

// Vaild reboot method map.
var validRebootMap = map[syspb.RebootMethod]bool{
	syspb.RebootMethod_COLD:      true,
	syspb.RebootMethod_WARM:      true,
	syspb.RebootMethod_POWERDOWN: true,
	syspb.RebootMethod_NSF:       true,
}

// Validates reboot request.
func validRebootReq(req *syspb.RebootRequest) error {
	if _, ok := validRebootMap[req.GetMethod()]; !ok {
		log.V(lvl.ERROR).Info("Invalid request: reboot method is not supported.")
		return fmt.Errorf("Invalid request: reboot method is not supported.")
	}
	// Back end does not support delayed reboot request.
	if req.GetDelay() > 0 {
		log.V(lvl.ERROR).Info("Invalid request: reboot is not immediate.")
		return fmt.Errorf("Invalid request: reboot is not immediate.")
	}
	if req.GetMessage() == "" {
		log.V(lvl.ERROR).Info("Invalid request: message is empty.")
		return fmt.Errorf("Invalid request: message is empty.")
	}

	if len(req.GetSubcomponents()) == 0 {
		return nil
	}
	if err := validateModules(req.GetSubcomponents()); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return err
	}

	return nil
}

// Validates if all subcomponents are valid transceivers.
func validateModules(comps []*types.Path) error {
	for _, comp := range comps {
		if _, err := validateAndGetXcvr(comp); err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return err
		}
	}
	return nil
}

func processModuleReset(comps []*types.Path) error {
	for _, comp := range comps {
		xcvr, err := validateAndGetXcvr(comp)
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return err
		}

		port := strings.TrimPrefix(xcvr, "Ethernet")
		cmd := "reset_modules port=" + port
		log.V(lvl.INFO).Info("Reset command: ", cmd)
		if _, err := sysXfmr.resetOptics(cmd); err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return err
		}
	}
	return nil
}

func sendRebootReqOnNotifCh(ctx context.Context, req proto.Message, sc *redis.Client, rebootNotifKey string) (resp proto.Message, err error, msgDataStr string) {
	np, err := common_utils.NewNotificationProducer(rebootReqCh)
	if err != nil {
		log.V(lvl.INFO).Infof("[Reboot_Log] Error in setting up NewNotificationProducer: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	defer np.Close()

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), rebootRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		log.V(lvl.INFO).Infof("[Reboot_Log] Error in setting up subscription to response channel: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	defer sub.Close()
	channel := sub.Channel()

	switch rebootNotifKey {
	case rebootKey:
		req = req.(*syspb.RebootRequest)
		resp = &syspb.RebootResponse{}
	case rebootStatusKey:
		req = req.(*syspb.RebootStatusRequest)
		resp = &syspb.RebootStatusResponse{}
	case rebootCancelKey:
		req = req.(*syspb.CancelRebootRequest)
		resp = &syspb.CancelRebootResponse{}
	}

	reqStr, err := json.Marshal(req)
	if err != nil {
		log.V(lvl.INFO).Infof("[Reboot_Log] Error in marshalling JSON: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}
	// Publish to notification channel.
	if err := np.Send(rebootNotifKey, "", map[string]string{dataMsgFld: string(reqStr)}); err != nil {
		log.V(lvl.INFO).Infof("[Reboot_Log] Error in publishing to notification channel: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error()), msgDataStr
	}

	// Wait for response on Reboot_Response_Channel.
	tc := time.After(notificationTimeout)
	for {
		select {
		case msg := <-channel:
			op, data, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				log.V(lvl.INFO).Infof("[Reboot_Log] Error while receiving Response Notification = [%v] for message [%v]", err.Error(), msg)
				return nil, status.Errorf(codes.Internal, fmt.Sprintf("Error while receiving Response Notification: [%s] for message [%s]", err.Error(), msg)), msgDataStr
			}
			log.V(lvl.INFO).Infof("[Reboot_Log] Received on the Reboot notification channel: op = [%v], data = [%v], fvs = [%v]", op, data, fvs)

			if op != rebootNotifKey {
				log.V(lvl.INFO).Infof("[Reboot_Log] Op: [%v] doesn't match for `%v`!", op, rebootNotifKey)
				continue
			}
			if fvs != nil {
				if _, ok := fvs[dataMsgFld]; ok {
					msgDataStr = fvs[dataMsgFld]
				}
			}
			if swssCode := swssToErrorCode(data); swssCode != codes.OK {
				log.V(lvl.INFO).Infof("[Reboot_Log] Response Notification returned SWSS Error code: %v, error = %v", swssCode, msgDataStr)
				return nil, status.Errorf(swssCode, "Response Notification returned SWSS Error code: "+msgDataStr), msgDataStr
			}
			return resp, nil, msgDataStr

		case <-tc:
			// Crossed the reboot response notification timeout.
			log.V(lvl.INFO).Infof("[Reboot_Log] Response Notification timeout from NSF Manager!")
			return nil, status.Errorf(codes.Internal, "Response Notification timeout from NSF Manager!"), msgDataStr
		}
	}
}

func precheckNSFReboot(ctx context.Context, srv *Server, req *syspb.RebootRequest, rclient *redis.Client) (*syspb.RebootResponse, error) {
	// Reject NSF if system is in critical state.
	if srv.SsHelper.IsSystemCritical() {
		log.V(lvl.INFO).Info("NSF request rejected since system is in critical state")
		return nil, status.Errorf(codes.FailedPrecondition, "NSF request rejected since system is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}
	// Reject NSF if NSF is already in progress.
	if srv.WarmRestartHelper.IsNSFOngoing() {
		log.V(lvl.INFO).Info("NSF request rejected since NSF is already in progress!")
		return nil, status.Errorf(codes.Unavailable, "NSF request rejected since NSF is already in progress!")
	}
	// Reject NSF if LinkQual is in progress.
	result, err := rclient.HGet(context.Background(), "LINKQUAL_RESULT|LINKQUAL_ACTIVE_SESSIONS", "count").Result()
	// Valid case if table entry is not present, i.e. no LinkQual is ongoing.
	if err != nil {
		resp, err, _ := sendRebootReqOnNotifCh(ctx, req, rclient, rebootKey)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			log.V(lvl.INFO).Info("NSF request received empty response from NSF Manager.")
			return &syspb.RebootResponse{}, nil
		}
		return resp.(*syspb.RebootResponse), err
	}
	linkQualRes, err := strconv.Atoi(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if linkQualRes > 0 {
		return nil, status.Errorf(codes.Unavailable, "NSF request rejected since LinkQual is in progress: LinkQual active session count %v", result)
	}
	resp, err, _ := sendRebootReqOnNotifCh(ctx, req, rclient, rebootKey)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		log.V(lvl.INFO).Info("NSF request received empty response from NSF Manager.")
		return &syspb.RebootResponse{}, nil
	}
	return resp.(*syspb.RebootResponse), nil
}

// Reboot implements the corresponding RPC.
func (srv *Server) Reboot(ctx context.Context, req *syspb.RebootRequest) (*syspb.RebootResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("gNOI: Reboot")
	if err := validRebootReq(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Initialize State DB.
	rclient, err := getRedisDBClient(stateDB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(rclient)

	// NSF WARM BOOT.
	if req.GetMethod() == syspb.RebootMethod_NSF {
		// NSF requires a quiescent state, so disable PortCycler before a WARM
		// reboot.
		if srv.doPortCycleDisable {
			configDbClient, configDbClientErr := common_utils.NewConfigDBClient()
			if configDbClientErr != nil {
				return nil, status.Errorf(codes.Aborted, "Failed to start a new ConfigDB client with error %v. Cannot disable Port-Cycling.", configDbClientErr)
			}
			defer db.CloseRedisClient(configDbClient)

			if err = common_utils.DisablePortCycler(configDbClient); err != nil {
				return nil, status.Errorf(codes.Aborted, "Failed to disable Port-Cycling with error %v.", err)
			}
			srv.doPortCycleDisable = false
		}
		// NSF pre-check validation before sending to NSF Manager.
		return precheckNSFReboot(ctx, srv, req, rclient)
	}

	// Module reset.
	if len(req.GetSubcomponents()) > 0 {
		if err := processModuleReset(req.GetSubcomponents()); err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		return &syspb.RebootResponse{}, nil
	}

	// System reboot (COLD BOOT).
	resp, err, _ := sendRebootReqOnNotifCh(ctx, req, rclient, rebootKey)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		log.V(lvl.INFO).Info("NSF request received empty response from NSF Manager.")
		return &syspb.RebootResponse{}, nil
	}
	return resp.(*syspb.RebootResponse), nil
}

// RebootStatus implements the corresponding RPC.
func (srv *Server) RebootStatus(ctx context.Context, req *syspb.RebootStatusRequest) (*syspb.RebootStatusResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("gNOI: RebootStatus")
	resp := &syspb.RebootStatusResponse{}
	// Initialize State DB.
	rclient, err := getRedisDBClient(stateDB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(rclient)

	respStr, err, msgData := sendRebootReqOnNotifCh(ctx, req, rclient, rebootStatusKey)
	if err != nil {
		log.V(lvl.INFO).Infof("gNOI: Received error for RebootStatusResponse: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if msgData == "" || respStr == nil {
		log.V(lvl.INFO).Info("gNOI: Received empty RebootStatusResponse")
		return nil, status.Errorf(codes.Internal, "Received empty RebootStatusResponse")
	}
	if err := pjson.Unmarshal([]byte(msgData), resp); err != nil {
		log.V(lvl.INFO).Infof("gNOI: Cannot unmarshal the response: [%v]; err: [%v]", msgData, err)
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("Cannot unmarshal the response: [%s]; err: [%s]", msgData, err.Error()))
	}
	log.V(lvl.INFO).Infof("gNOI: Returning RebootStatusResponse: resp = [%v]\n, msgData = [%v]", resp, msgData)
	return resp, nil
}

// CancelReboot RPC implements the corresponding RPC.
func (srv *Server) CancelReboot(ctx context.Context, req *syspb.CancelRebootRequest) (*syspb.CancelRebootResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.DEBUG).Info("gNOI: CancelReboot")
	if req.GetMessage() == "" {
		log.V(lvl.ERROR).Info("Invalid CancelReboot request: message is empty.")
		return nil, status.Errorf(codes.Internal, "Invalid CancelReboot request: message is empty.")
	}
	// Initialize State DB.
	rclient, err := getRedisDBClient(stateDB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(rclient)

	resp, err, _ := sendRebootReqOnNotifCh(ctx, req, rclient, rebootCancelKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if resp == nil {
		return &syspb.CancelRebootResponse{}, nil
	}
	return resp.(*syspb.CancelRebootResponse), err
}

// Ping implements the corresponding RPC.
func (srv *Server) Ping(req *syspb.PingRequest, stream syspb.System_PingServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI system Ping RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI system Ping RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("gNOI: Ping")
	return status.Errorf(codes.Unimplemented, "Method system.Ping is unimplemented.")
}

// Traceroute implements the corresponding RPC.
func (srv *Server) Traceroute(req *syspb.TracerouteRequest, stream syspb.System_TracerouteServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI system Traceroute RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI system Traceroute RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("gNOI: Traceroute")
	return status.Errorf(codes.Unimplemented, "Method system.Traceroute is unimplemented.")
}

// SetPackage implements the corresponding RPC.
func (srv *Server) SetPackage(stream syspb.System_SetPackageServer) error {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI system SetPackage RPC disabled since NSF is ongoing!")
		return status.Errorf(codes.Unavailable, "gNOI system SetPackage RPC disabled since NSF is ongoing!")
	}
	ctx := stream.Context()
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("gNOI: SetPackage")
	return status.Errorf(codes.Unimplemented, "Method system.SetPackage is unimplemented.")
}

// SwitchControlProcessor implements the corresponding RPC.
func (srv *Server) SwitchControlProcessor(ctx context.Context, req *syspb.SwitchControlProcessorRequest) (*syspb.SwitchControlProcessorResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI system SwitchControlProcessor RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI system SwitchControlProcessor RPC disabled since NSF is ongoing!")
	}
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
	log.V(lvl.DEBUG).Info("gNOI: SwitchControlProcessor")
	return &syspb.SwitchControlProcessorResponse{}, nil
}

// Time implements the corresponding RPC.
func (srv *Server) Time(ctx context.Context, req *syspb.TimeRequest) (*syspb.TimeResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI system Time RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI system Time RPC disabled since NSF is ongoing!")
	}
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
	log.V(lvl.DEBUG).Info("gNOI: Time")
	var tm syspb.TimeResponse
	tm.Time = uint64(time.Now().UnixNano())
	return &tm, nil
}

// TODO(b/330075525) Fix support for MS's use of system reboot
// func RebootSystem(fileName string) error {
// 	log.V(2).Infof("Rebooting with %s...", fileName)
// 	sc, err := ssc.NewDbusClient(dbusCaller)
// 	if err != nil {
// 		return err
// 	}
// 	err = sc.ConfigReload(fileName)
// 	return err
// }

// func (srv *Server) Reboot(ctx context.Context, req *syspb.RebootRequest) (*syspb.RebootResponse, error) {
// 	fileName := common_utils.GNMI_WORK_PATH + "/config_db.json.tmp"

// 	_, err := authenticate(srv.config, ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	log.V(1).Info("gNOI: Reboot")
// 	log.V(1).Info("Request:", req)
// 	log.V(1).Info("Reboot system now, delay is ignored...")
// 	// TODO: Support GNOI reboot delay
// 	// Delay in nanoseconds before issuing reboot.
// 	// https://github.com/openconfig/gnoi/blob/master/system/system.proto#L102-L115
// 	config_db_json, err := io.ReadFile(fileName)
// 	if errors.Is(err, os.ErrNotExist) {
// 		fileName = ""
// 	}
// 	err = RebootSystem(string(config_db_json))
// 	if err != nil {
// 		return nil, err
// 	}
// 	var resp syspb.RebootResponse
// 	return &resp, nil
// }
