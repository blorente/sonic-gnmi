package gnmi

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	qual "github.com/sonic-net/sonic-gnmi/proto/gnoi/qualification"
	apistatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	getPktRes    string = "GET_PACKET"
	getPktID     string = "GET_PACKET_ID"
	startPktQual string = "START_PACKET"
	stopPktQual  string = "STOP_PACKET"
)

// populateStatus populates status  with error code and message.
func populateStatus(status *apistatus.Status, code codes.Code, msg string) {
	status.Code = int32(code)
	status.Message = msg
}

// writeStartPktQualReqToCh writes the request to channel.
func writeStartPktQualReqToCh(req *qual.StartPacketQualificationRequest) error {
	if err := writeBertReq(lqReqCh, startPktQual, "dummy", proto.MarshalTextString(req)); err != nil {
		return err
	}

	// Write ID.
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(sc)

	for _, r := range req.GetConfigs() {
		dbWriteMutex.Lock()
		err := sc.HSet(context.Background(), getKey([]string{lqResult, getPktID}), r.GetId(), time.Now().UnixNano()).Err()
		dbWriteMutex.Unlock()
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return err
		}
	}
	return nil
}

// prepareStartPktQualResp populates response, and error messages.
func prepareStartPktQualResp(req *qual.StartPacketQualificationRequest) (*qual.StartPacketQualificationResponse, error) {
	// Get IDs from the DB.
	ids, err := retrieveIDs(packetQual)
	if err != nil {
		return nil, err
	}
	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(cc)

	valid := false
	var errs []string
	newReq := &qual.StartPacketQualificationRequest{}
	resp := &qual.StartPacketQualificationResponse{}
	idm := map[string]bool{}
	for _, preq := range req.GetConfigs() {
		r := &qual.StartPacketQualificationResponse_StartResponse{
			Id: preq.GetId(),
			Status: &apistatus.Status{
				Code: int32(codes.OK),
			},
		}
		if preq.GetId() == "" {
			err := fmt.Sprintf("Config ID is empty.")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		if preq.GetMinimumWaitBeforePreparationSeconds() > int32(maxTestDurationSecs) ||
			preq.GetMinimumWaitBeforePreparationSeconds() < int32(minTestDurationSecs) {
			err := fmt.Sprintf("Minimum wait time %d is out-of-range.", preq.GetMinimumWaitBeforePreparationSeconds())
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.OutOfRange, err)
		}
		if preq.GetPreparationTimeoutSeconds() > int32(maxTestDurationSecs) ||
			preq.GetPreparationTimeoutSeconds() < int32(minTestDurationSecs) {
			err := fmt.Sprintf("Preparation timeout %d is out-of-range.", preq.GetPreparationTimeoutSeconds())
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.OutOfRange, err)
		}
		if preq.GetQualificationDurationSeconds() > int32(maxTestDurationSecs) ||
			preq.GetQualificationDurationSeconds() < int32(minTestDurationSecs) {
			err := fmt.Sprintf("Qualification duration %d is out-of-range.", preq.GetQualificationDurationSeconds())
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.OutOfRange, err)
		}
		if preq.GetQualificationEnd() == qual.StartPacketQualificationRequest_END_UNSPECIFIED {
			err := fmt.Sprintf("Qualification end is not set.")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		if preq.GetQualificationEnd() == qual.StartPacketQualificationRequest_NEAR_END &&
			preq.GetNumPreparationPackets() <= 0 {
			err := fmt.Sprintf("Number of preparation packets %d should be positive.", preq.GetNumPreparationPackets())
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		if _, er := validateIntf(preq.GetInterface(), cc); er != nil {
			err := fmt.Sprintf("Failed to validate interface: %s, err: %s.", preq.GetInterface().String(), er)
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.NotFound, err)
		}
		// ID is in DB; already used.
		if err := isIDInUse(preq.GetId(), ids); err != nil {
			err := fmt.Sprintf("Config ID exists!")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		// ID is repeated.
		if _, ok := idm[preq.GetId()]; ok {
			err := fmt.Sprintf("Config ID exists!")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		if r.GetStatus().GetCode() == int32(codes.OK) {
			valid = true
			idm[preq.GetId()] = true
			newReq.Configs = append(newReq.Configs, preq)
		}
		resp.Responses = append(resp.Responses, r)
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Error: %v", strings.Join(errs, ", "))
	}
	if !valid {
		return nil, fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}
	// Write to channel.
	if err := writeStartPktQualReqToCh(newReq); err != nil {
		return nil, err
	}
	return resp, nil
}

// StartPacketQualification implements the corresponding RPC.
func (srv *Server) StartPacketQualification(ctx context.Context, req *qual.StartPacketQualificationRequest) (*qual.StartPacketQualificationResponse, error) {
	// Reject if the platform does not support packet based link qualification.
	if !srv.lqHelper.SupportsPktLq() {
		log.V(lvl.ERROR).Info("gNOI qual StartPacketQualification RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI qual StartPacketQualification RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI Qual StartPacketQualification RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI Qual StartPacketQualification RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}
	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)
	log.V(lvl.INFO).Info("gNOI: StartPacketQualification")
	resp, err := prepareStartPktQualResp(req)
	// RPC fails when qualification operation is not supported on any of the ports specified by the request.
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}

// prepareStopPktQualResp populates response, and error messages.
func prepareStopPktQualResp(req *qual.StopPacketQualificationRequest) (*qual.StopPacketQualificationResponse, error) {
	// Get IDs from the DB.
	ids, err := retrieveIDs(packetQual)
	if err != nil {
		return nil, err
	}

	valid := false
	var errs []string
	newReq := &qual.StopPacketQualificationRequest{}
	resp := &qual.StopPacketQualificationResponse{}
	for _, id := range req.GetIds() {
		r := &qual.StopPacketQualificationResponse_StopResponse{
			Id: id,
			Status: &apistatus.Status{
				Code: int32(codes.OK),
			},
		}
		if id == "" {
			err := fmt.Sprintf("Config ID is empty.")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		// ID doesn't exist.
		if err := isIDInUse(id, ids); err == nil {
			err := fmt.Sprintf("Config ID doesn't exist!")
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.InvalidArgument, err)
		}
		if r.GetStatus().GetCode() == int32(codes.OK) {
			valid = true
			newReq.Ids = append(newReq.Ids, id)
		}
		resp.Responses = append(resp.Responses, r)
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Error: %v", strings.Join(errs, ", "))
	}
	if !valid {
		return resp, fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}
	// Write to channel.
	if err := writeBertReq(lqReqCh, stopPktQual, "dummy", proto.MarshalTextString(newReq)); err != nil {
		return nil, err
	}
	return resp, nil
}

// StopPacketQualification implements the corresponding RPC.
func (srv *Server) StopPacketQualification(ctx context.Context, req *qual.StopPacketQualificationRequest) (*qual.StopPacketQualificationResponse, error) {
	// Reject if the platform does not support packet based link qualification.
	if !srv.lqHelper.SupportsPktLq() {
		log.V(lvl.ERROR).Info("gNOI qual StopPacketQualification RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI qual StopPacketQualification RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI Qual StopPacketQualification RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI Qual StopPacketQualification RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}
	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)
	log.V(lvl.INFO).Info("gNOI: StopPacketQualification")
	resp, err := prepareStopPktQualResp(req)
	// RPC fails when qualification operation is not supported on any of the ports specified by the request.
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}

// getPktResult gets results using ID.
func getPktResult(req *qual.GetPacketQualificationResultRequest, rclient *redis.Client) (*qual.GetPacketQualificationResultResponse, error) {
	resp := &qual.GetPacketQualificationResultResponse{}
	valid := false
	var errs []string
	for _, id := range req.GetIds() {
		r := &qual.GetPacketQualificationResultResponse_GetResponse{
			Id: id,
			Status: &apistatus.Status{
				Code: int32(codes.OK),
			},
		}
		key := getKey([]string{lqResult, getPktRes, id})
		log.V(lvl.DEBUG).Infof("Hash key: %s\n", key)
		results, err := rclient.HGetAll(context.Background(), key).Result()
		if err != nil {
			log.V(lvl.ERROR).Infof("Cannot get results for key: %s.\n", key)
			return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot get results for key: %s.", key).Error())
		}
		rep, ok := results[msgFld]
		if !ok {
			err := fmt.Sprintf("Result is not found for ID: %s", id)
			errs = append(errs, err)
			populateStatus(r.GetStatus(), codes.NotFound, err)
		} else {
			res := &qual.QualificationResults{}
			if err := proto.UnmarshalText(rep, res); err != nil {
				log.V(lvl.ERROR).Infof("Cannot unmarshal result for ID: %s.\n", id)
				return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot unmarshal result for ID: %s.", id).Error())
			}
			r.Results = res
		}
		if r.GetStatus().GetCode() == int32(codes.OK) {
			valid = true
		}
		resp.Responses = append(resp.Responses, r)
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Error: %v", strings.Join(errs, ", "))
	}
	if !valid {
		return resp, fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}
	return resp, nil
}

// GetPacketQualificationResult implements the corresponding RPC.
func (srv *Server) GetPacketQualificationResult(ctx context.Context, req *qual.GetPacketQualificationResultRequest) (*qual.GetPacketQualificationResultResponse, error) {
	// Reject if the platform does not support packet based link qualification.
	if !srv.lqHelper.SupportsPktLq() {
		log.V(lvl.ERROR).Info("gNOI qual GetPacketQualificationResult RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI qual GetPacketQualificationResult RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI Qual GetPacketQualificationResult RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI Qual GetPacketQualificationResult RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	if srv.SsHelper.IsSystemCritical() {
		return nil, status.Errorf(codes.Internal, "System is in critical state: %s", srv.SsHelper.GetSystemCriticalReason())
	}
	pr, _ := peer.FromContext(ctx)
	connectionKey, valid := srv.ConnectionManager.Add(pr.Addr, req.String(), false)
	if !valid {
		return nil, status.Error(codes.Unavailable, "Server connections are at capacity.")
	}
	defer srv.ConnectionManager.Remove(connectionKey)
	log.V(lvl.INFO).Info("gNOI: GetPacketQualificationResult")

	rclient, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Unavailable, err.Error())
	}
	defer db.CloseRedisClient(rclient)

	resp, err := getPktResult(req, rclient)
	// RPC fails when qualification operation is not supported on any of the ports specified by the request.
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}
