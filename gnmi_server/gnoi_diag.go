package gnmi

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	dpb "github.com/openconfig/gnoi/diag"
	"github.com/openconfig/gnoi/types"
	"github.com/redis/go-redis/v9"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type OpType int

const (
	prbsQual OpType = iota
	packetQual
)

const (
	maxTestDurationSecs uint32 = 86400 // 24h
	minTestDurationSecs uint32 = 1
	lqReqCh             string = "LINKQUAL_OPERATIONS_REQUEST_CHANNEL"
	lqResult            string = "LINKQUAL_RESULT"
	startBERT           string = "START_BERT"
	stopBERT            string = "STOP_BERT"
	getBERTRes          string = "GET_BERT"
	getBERTID           string = "GET_BERT_ID"
	purgeThreshHrs      int64  = 25 // 25h
	intfKey             string = "name"
	portKeyPrefix       string = "PORT"
	msgFld              string = "message"
)

var (
	// Mutex for DB writes
	dbWriteMutex sync.Mutex
)

// validateIntf validates if the interface exists
// It validates both the OpenConfig and the SONiC formats
// It checks that the PORT table in the Config DB knows about this interface
func validateIntf(intf *types.Path, cc *redis.Client) (string, error) {
	if intf == nil {
		return "", fmt.Errorf("Interface is nil.")
	}
	if cc == nil {
		return "", fmt.Errorf("REDIS client is nil.")
	}
	// Check both OpenConfig and SONiC formats.
	if intf.GetOrigin() != "openconfig" && intf.GetOrigin() != "openconfig-interfaces" {
		return "", fmt.Errorf("Interface is malformed.")
	}
	elems := intf.GetElem()
	if len(elems) != 2 {
		return "", fmt.Errorf("Interface is malformed.")
	}
	if elems[0].GetName() != "interfaces" || len(elems[0].GetKey()) > 0 {
		return "", fmt.Errorf("Interface is malformed.")
	}
	if elems[1].GetName() != "interface" || len(elems[1].GetKey()) != 1 {
		return "", fmt.Errorf("Interface is malformed.")
	}
	name, ok := elems[1].GetKey()[intfKey]
	if !ok {
		return "", fmt.Errorf("Interface is malformed.")
	}
	flen, err := cc.HLen(context.Background(), getKey([]string{portKeyPrefix, name})).Result()
	if err != nil || flen == 0 {
		return "", fmt.Errorf("Interface is invalid.")
	}
	return name, nil
}

// cleanupIDs purges IDs that are too old.
func cleanupIDs(idKey string, ids map[string]string, sc *redis.Client) (map[string]string, error) {
	m := make(map[string]string)
	for id, ts := range ids {
		// Get timestamp.
		tns, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			log.V(lvl.ERROR).Infof("Cannot convert timestamp for ID: %s.\n", id)
			return nil, fmt.Errorf("Cannot convert timestamp for ID: %s.", id)
		}
		t := time.Unix(0, tns)
		elapsedHrs := int64(time.Since(t).Hours())
		// ID is too old. Purge now.
		if elapsedHrs > purgeThreshHrs {
			log.V(lvl.DEBUG).Infof("Purging entry: %s, as it has been in the DB for %v hour(s).\n", id, elapsedHrs)
			if err := sc.HDel(context.Background(), idKey, id).Err(); err != nil {
				log.V(lvl.ERROR).Infof("Cannot delete entry for key: %s, id: %s.\n", idKey, id)
				return nil, fmt.Errorf("Cannot delete entry for key: %s, id: %s.", idKey, id)
			}
		} else {
			// Do not purge. Populate in the new map.
			m[id] = ts
		}
	}
	return m, nil
}

// retrieveIDs retrieves operation IDs. It purges old IDs in the process.
func retrieveIDs(op OpType) (map[string]string, error) {
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(sc)

	var idKey string
	switch op {
	case prbsQual:
		idKey = getKey([]string{lqResult, getBERTID})
	case packetQual:
		idKey = getKey([]string{lqResult, getPktID})
	}

	ids, err := sc.HGetAll(context.Background(), idKey).Result()
	if err != nil {
		log.V(lvl.ERROR).Infof("Cannot get IDs for key: %s.\n", idKey)
		return nil, err
	}
	// Cleanup old IDs.
	ids, err = cleanupIDs(idKey, ids, sc)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// isIDInUse checks if the ID is already in use.
func isIDInUse(id string, ids map[string]string) error {
	if _, ok := ids[id]; ok {
		log.V(lvl.ERROR).Infof("ID: %s exists.\n", id)
		return fmt.Errorf("ID: %s exists.", id)
	}
	return nil
}

func writeBertReq(ch, op, data, reqStr string) error {
	np, err := common_utils.NewNotificationProducer(ch)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return fmt.Errorf(err.Error())
	}
	defer np.Close()

	// Write request to channel.
	if err := np.Send(op, data, map[string]string{msgFld: reqStr}); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return err
	}

	return nil
}

// writeStartBERTReqToCh writes the request to channel.
func writeStartBERTReqToCh(req *dpb.StartBERTRequest) error {
	if err := writeBertReq(lqReqCh, startBERT, "dummy", proto.MarshalTextString(req)); err != nil {
		return err
	}

	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return fmt.Errorf(err.Error())
	}
	defer db.CloseRedisClient(sc)

	// Write ID.
	dbWriteMutex.Lock()
	err = sc.HSet(context.Background(), getKey([]string{lqResult, getBERTID}), req.GetBertOperationId(), time.Now().UnixNano()).Err()
	dbWriteMutex.Unlock()
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return err
	}
	return nil
}

// prepareStartResp populates response, and error messages.
func prepareStartResp(req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error) {
	if req.GetBertOperationId() == "" {
		log.V(lvl.ERROR).Info("Invalid request: BERT ID is empty.")
		return nil, fmt.Errorf("Invalid request: BERT ID is empty.")
	}
	// Get IDs from the DB and check if ID exists.
	ids, err := retrieveIDs(prbsQual)
	if err != nil {
		return nil, err
	}
	if err := isIDInUse(req.GetBertOperationId(), ids); err != nil {
		log.V(lvl.ERROR).Infof("Invalid request: %s\n", err.Error())
		return nil, fmt.Errorf("Invalid request: %s", err.Error())
	}

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(cc)

	resp := &dpb.StartBERTResponse{}
	resp.BertOperationId = req.GetBertOperationId()

	valid := false
	var errs []string
	newReq := &dpb.StartBERTRequest{BertOperationId: req.GetBertOperationId()}
	for _, preq := range req.GetPerPortRequests() {
		r := &dpb.StartBERTResponse_PerPortResponse{
			Interface: preq.GetInterface(),
			Status:    dpb.BertStatus_BERT_STATUS_OK,
		}

		if preq.GetTestDurationInSecs() > maxTestDurationSecs {
			errs = append(errs, fmt.Sprintf("Duration %d is too long.", preq.GetTestDurationInSecs()))
			r.Status = dpb.BertStatus_BERT_STATUS_TEST_DURATION_TOO_LONG
		} else if preq.GetTestDurationInSecs() < minTestDurationSecs {
			errs = append(errs, fmt.Sprintf("Duration %d is too short.", preq.GetTestDurationInSecs()))
			r.Status = dpb.BertStatus_BERT_STATUS_TEST_DURATION_TOO_SHORT
		}
		if preq.GetPrbsPolynomial() == dpb.PrbsPolynomial_PRBS_POLYNOMIAL_UNKNOWN {
			errs = append(errs, fmt.Sprintf("PRBS polynomial is not set."))
			r.Status = dpb.BertStatus_BERT_STATUS_UNSUPPORTED_PRBS_POLYNOMIAL
		}
		if _, err := validateIntf(preq.GetInterface(), cc); err != nil {
			errs = append(errs, fmt.Sprintf("Failed to validate interface: %s, err: %s.", preq.GetInterface().String(), err))
			r.Status = dpb.BertStatus_BERT_STATUS_NON_EXISTENT_PORT
		}
		if r.Status == dpb.BertStatus_BERT_STATUS_OK {
			valid = true
			newReq.PerPortRequests = append(newReq.PerPortRequests, preq)
		}
		resp.PerPortResponses = append(resp.PerPortResponses, r)
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Error: %v", strings.Join(errs, ", "))
	}
	if !valid {
		return resp, fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}
	// Write to channel.
	if err := writeStartBERTReqToCh(newReq); err != nil {
		return nil, err
	}
	return resp, nil
}

// StartBERT implements corresponding gnoi.diag.StartBERT RPC.
func (srv *Server) StartBERT(ctx context.Context, req *dpb.StartBERTRequest) (*dpb.StartBERTResponse, error) {
	// Reject if the platform does not support PRBS link qualification.
	if !srv.lqHelper.SupportsBert() {
		log.V(lvl.ERROR).Info("gNOI diag StartBERT RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI diag StartBERT RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI diag StartBERT RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI diag StartBERT RPC disabled since NSF is ongoing!")
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
	log.V(lvl.INFO).Info("gNOI: StartBERT")
	resp, err := prepareStartResp(req)
	// RPC fails when BERT operation is not supported on any of the ports specified by the request.
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}

// prepareStopResp populates response, and error messages.
func prepareStopResp(req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error) {
	if req.GetBertOperationId() == "" {
		log.V(lvl.ERROR).Info("Invalid request: BERT ID is empty.")
		return nil, fmt.Errorf("Invalid request: BERT ID is empty.")
	}

	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(sc)

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(cc)

	resp := &dpb.StopBERTResponse{}
	resp.BertOperationId = req.GetBertOperationId()

	valid := false
	key := getKey([]string{lqResult, getBERTRes, req.GetBertOperationId()})
	var errs []string
	newReq := &dpb.StopBERTRequest{BertOperationId: req.GetBertOperationId()}
	for _, preq := range req.GetPerPortRequests() {
		r := &dpb.StopBERTResponse_PerPortResponse{
			Interface: preq.GetInterface(),
			Status:    dpb.BertStatus_BERT_STATUS_OK,
		}

		intf, err := validateIntf(preq.GetInterface(), cc)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Failed to validate interface: %s, err: %s.", preq.GetInterface().String(), err))
			r.Status = dpb.BertStatus_BERT_STATUS_NON_EXISTENT_PORT
		}
		if !sc.HExists(context.Background(), key, intf).Val() {
			errs = append(errs, fmt.Sprintf("Operation ID is not found on interface: %s.", preq.GetInterface().String()))
			r.Status = dpb.BertStatus_BERT_STATUS_OPERATION_ID_NOT_FOUND
		}
		if r.Status == dpb.BertStatus_BERT_STATUS_OK {
			valid = true
			newReq.PerPortRequests = append(newReq.PerPortRequests, preq)
		}
		resp.PerPortResponses = append(resp.PerPortResponses, r)
	}
	if len(errs) > 0 {
		log.V(lvl.ERROR).Infof("Error: %v", strings.Join(errs, ", "))
	}
	if !valid {
		return resp, fmt.Errorf("Invalid request: %v", strings.Join(errs, ", "))
	}
	// Write to channel.
	if err := writeBertReq(lqReqCh, stopBERT, "dummy", proto.MarshalTextString(newReq)); err != nil {
		return nil, err
	}
	return resp, nil
}

// StopBERT implements corresponding gnoi.diag.StopBERT RPC.
func (srv *Server) StopBERT(ctx context.Context, req *dpb.StopBERTRequest) (*dpb.StopBERTResponse, error) {
	// Reject if the platform does not support PRBS link qualification.
	if !srv.lqHelper.SupportsBert() {
		log.V(lvl.ERROR).Info("gNOI diag StopBERT RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI diag StopBERT RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI diag StopBERT RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI diag StopBERT RPC disabled since NSF is ongoing!")
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
	log.V(lvl.INFO).Info("gNOI: StopBERT")
	resp, err := prepareStopResp(req)
	// RPC fails when BERT operation is not supported on any of the ports specified by the request.
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}

// getKey generates the hash key from the supplied string array.
func getKey(k []string) string {
	return strings.Join(k, "|")
}

// getResultForAllIds gets results from all operation IDs.
func getResultForAllIds(rclient *redis.Client) (*dpb.GetBERTResultResponse, error) {
	// Retrieve IDs.
	ids, err := retrieveIDs(prbsQual)
	if err != nil {
		return nil, err
	}
	resp := &dpb.GetBERTResultResponse{}
	// Get results for IDs.
	for id, _ := range ids {
		rKey := getKey([]string{lqResult, getBERTRes, id})
		log.V(lvl.DEBUG).Infof("Get results for key: %s\n", rKey)
		results, err := rclient.HGetAll(context.Background(), rKey).Result()
		if err != nil {
			log.V(lvl.ERROR).Infof("Cannot get results for key: %s.\n", rKey)
			return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot get results for key: %s.", rKey).Error())
		}
		for intf, res := range results {
			r := &dpb.GetBERTResultResponse_PerPortResponse{}
			if err := proto.UnmarshalText(res, r); err != nil {
				log.V(lvl.ERROR).Infof("Cannot unmarshal result for intf: %s.\n", intf)
				return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot unmarshal result for intf: %s.", intf).Error())
			}
			resp.PerPortResponses = append(resp.PerPortResponses, r)
		}
	}
	return resp, nil
}

// getResultForOpAndIntfIds gets results using both operation ID and interface ID.
func getResultForOpAndIntfIds(req *dpb.GetBERTResultRequest, rclient *redis.Client) (*dpb.GetBERTResultResponse, error) {
	if req.GetBertOperationId() == "" {
		log.V(lvl.ERROR).Info("Invalid request: BERT ID is empty.")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request: BERT ID is empty.")
	}

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Unavailable, err.Error())
	}
	defer db.CloseRedisClient(cc)

	key := getKey([]string{lqResult, getBERTRes, req.GetBertOperationId()})
	for _, preq := range req.GetPerPortRequests() {
		intf, err := validateIntf(preq.GetInterface(), cc)
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return nil, status.Errorf(codes.InvalidArgument, fmt.Errorf("Interface [%s] is not valid.", preq.GetInterface()).Error())
		}
		if !rclient.HExists(context.Background(), key, intf).Val() {
			log.V(lvl.ERROR).Infof("Result is not found for intf: %s.\n", intf)
			return nil, status.Errorf(codes.InvalidArgument, fmt.Errorf("Result is not found for intf: %s.", intf).Error())
		}
	}

	// Valid request.
	results, err := rclient.HGetAll(context.Background(), key).Result()
	if err != nil {
		log.V(lvl.ERROR).Infof("Cannot get results for key: %s.\n", key)
		return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot get results for key: %s.", key).Error())
	}
	resp := &dpb.GetBERTResultResponse{}
	for _, preq := range req.GetPerPortRequests() {
		intf, err := validateIntf(preq.GetInterface(), cc)
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return nil, status.Errorf(codes.InvalidArgument, fmt.Errorf("Interface [%s] is not valid.", preq.GetInterface()).Error())
		}
		r := &dpb.GetBERTResultResponse_PerPortResponse{}
		if err := proto.UnmarshalText(results[intf], r); err != nil {
			log.V(lvl.ERROR).Infof("Cannot unmarshal result for intf: %s.\n", intf)
			return nil, status.Errorf(codes.Unavailable, fmt.Errorf("Cannot unmarshal result for intf: %s.", intf).Error())
		}
		resp.PerPortResponses = append(resp.PerPortResponses, r)
	}
	return resp, nil
}

// getResult gets results from DB.
func getResult(req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error) {
	rclient, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Unavailable, err.Error())
	}
	defer db.CloseRedisClient(rclient)

	if req.GetResultFromAllPorts() {
		return getResultForAllIds(rclient)
	}
	return getResultForOpAndIntfIds(req, rclient)
}

// GetBERTResult implements corresponding gnoi.diag.GetBERTResult RPC.
func (srv *Server) GetBERTResult(ctx context.Context, req *dpb.GetBERTResultRequest) (*dpb.GetBERTResultResponse, error) {
	// Reject if the platform does not support PRBS link qualification.
	if !srv.lqHelper.SupportsBert() {
		log.V(lvl.ERROR).Info("gNOI diag GetBERTResult RPC is not supported!")
		return nil, status.Errorf(codes.Unimplemented, "gNOI diag GetBERTResult RPC is not supported!")
	}
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI diag GetBERTResult RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI diag GetBERTResult RPC disabled since NSF is ongoing!")
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
	log.V(lvl.INFO).Info("gNOI: GetBERTResult")
	return getResult(req)
}
