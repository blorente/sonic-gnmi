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
	bbpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/blackbox"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"github.com/openconfig/gnoi/types"
	apistatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	appStateDB                  = "APPL_STATE_DB"
	appPortTbl                  = "PORT_TABLE"
	compStTbl                   = "COMPONENT_STATE_TABLE"
	stateVerReqChan             = "VERIFY_STATE_REQ_CHANNEL"
	stateVerRespTbl             = "VERIFY_STATE_RESP_TABLE"
	setHwLinkReqCh              = "SET_HARDWARE_LINK_STATE_REQ_CHANNEL"
	setHwLinkRespCh             = "SET_HARDWARE_LINK_STATE_RESP_CHANNEL"
	setHwLinkOp                 = "set_hardware_link_state"
	notificationResponseTimeout = 5 * time.Second
)

var (
	verifyStateTimeout = 80 * time.Second
)

type BBTransformerInterface interface {
	setXcvrState(string) (string, error)
}

type bbTransformerImpl struct{}

func (t bbTransformerImpl) setXcvrState(req string) (string, error) {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return "", err
	}
	return sc.DebugTunnel(req)
}

var bbXfmr BBTransformerInterface

func init() {
	bbXfmr = bbTransformerImpl{}
}

type xcvrInfo struct {
	xcvr      *types.Path
	processed bool
}

func setXcvrResp(xcvr *types.Path, code codes.Code, msg string) *bbpb.SetTransceiverStateResponse_TransceiverStateResponse {
	return &bbpb.SetTransceiverStateResponse_TransceiverStateResponse{
		Transceiver: xcvr,
		Status: &apistatus.Status{
			Code:    int32(code),
			Message: msg,
		},
	}
}

func getIntfState() (map[string]map[string]string, error) {
	asd, err := getRedisDBClient(appStateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer db.CloseRedisClient(asd)

	pattern := strings.Join([]string{appPortTbl, "Ethernet*"}, ":")
	keys, err := asd.Keys(context.Background(), pattern).Result()
	if err != nil {
		log.V(lvl.ERROR).Info("Cannot get keys with pattern: ", pattern)
		return nil, err
	}

	hash := map[string]map[string]string{}
	for _, key := range keys {
		// Get interface name from key. Key is of the format: PORT_TABLE:EthernetX
		splittedKeys := strings.SplitN(key, ":", 2)
		if len(splittedKeys) != 2 {
			log.V(lvl.ERROR).Info("Malformed key: ", key)
			continue
		}

		m, err := asd.HGetAll(context.Background(), key).Result()
		if err != nil {
			log.V(lvl.ERROR).Info("Cannot get data for key: ", key)
			continue
		}

		hash[splittedKeys[1]] = m
	}

	if len(hash) == 0 {
		return nil, fmt.Errorf("Cannot get any interface data from APPL_STATE_DB:PORT_TABLE.")
	}

	return hash, nil
}

func getIntfsFromHwPort(port string, hash map[string]map[string]string) ([]string, error) {
	if hash == nil {
		return nil, fmt.Errorf("Interface data must not be nil!")
	}

	var intfs []string
	for intf, data := range hash {
		if p, ok := data["index"]; ok {
			if strings.Compare(strings.TrimSpace(p), strings.TrimSpace(port)) == 0 {
				intfs = append(intfs, intf)
			}
		}
	}

	if len(intfs) == 0 {
		return nil, fmt.Errorf("No interfaces are associated with port [%s].", port)
	}

	return intfs, nil
}

func validateAndGetXcvr(xcvr *types.Path) (string, error) {
	if xcvr == nil {
		return "", fmt.Errorf("Xcvr is nil.")
	}
	// Check the OpenConfig format.
	if xcvr.GetOrigin() != "openconfig" {
		return "", fmt.Errorf("Xcvr is malformed: origin != openconfig!")
	}
	elems := xcvr.GetElem()
	if len(elems) != 2 {
		return "", fmt.Errorf("Xcvr is malformed: size(elements) != 2!")
	}
	if elems[0].GetName() != "components" || len(elems[0].GetKey()) > 0 {
		return "", fmt.Errorf("Xcvr is malformed: %v!", elems[0])
	}
	if elems[1].GetName() != "component" || len(elems[1].GetKey()) != 1 {
		return "", fmt.Errorf("Xcvr is malformed: %v!", elems[1])
	}
	name, ok := elems[1].GetKey()[intfKey]
	if !ok {
		return "", fmt.Errorf("Xcvr is malformed. Unknown key: %v!", intfKey)
	}
	return name, nil
}

// validateXcvrReq validates if the request is correctly formed.
func validateXcvrReq(req *bbpb.SetTransceiverStateRequest_TransceiverStateRequest) (string, error) {
	if req.GetState() == bbpb.SetTransceiverStateRequest_TransceiverStateRequest_STATE_UNSPECIFIED {
		return "", fmt.Errorf("Operation unspecified.")
	}

	return validateAndGetXcvr(req.GetTransceiver())
}

func processXcvrReq(req *bbpb.SetTransceiverStateRequest, hash map[string]map[string]string) (map[string]xcvrInfo, map[string]string, map[string]string, map[string]map[string]bool, *bbpb.SetTransceiverStateResponse, error) {
	xcvrs := map[string]xcvrInfo{}
	newReq := map[string]string{}
	intfToXcvr := map[string]string{}
	xcvrToIntfs := map[string]map[string]bool{}
	resp := &bbpb.SetTransceiverStateResponse{}

	for _, r := range req.GetTransceiverRequests() {
		xcvr, err := validateXcvrReq(r)
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			resp.TransceiverResponses = append(resp.TransceiverResponses, setXcvrResp(r.GetTransceiver(), codes.InvalidArgument, err.Error()))
			continue
		}

		// Perform DBUS operation.
		port := strings.TrimPrefix(xcvr, "Ethernet")
		cmd := "insert_modules port=" + port
		if r.GetState() == bbpb.SetTransceiverStateRequest_TransceiverStateRequest_REMOVE {
			cmd = "remove_modules port=" + port
		}
		if _, err := bbXfmr.setXcvrState(cmd); err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			resp.TransceiverResponses = append(resp.TransceiverResponses, setXcvrResp(r.GetTransceiver(), codes.InvalidArgument, err.Error()))
			continue
		}

		// Get interfaces from the hardware port.
		intfs, err := getIntfsFromHwPort(port, hash)
		if err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			resp.TransceiverResponses = append(resp.TransceiverResponses, setXcvrResp(r.GetTransceiver(), codes.InvalidArgument, err.Error()))
			continue
		}

		xcvrs[xcvr] = xcvrInfo{xcvr: r.GetTransceiver(), processed: false}
		hash := map[string]bool{}
		for _, intf := range intfs {
			newReq[intf] = "up"
			intfToXcvr[intf] = xcvr
			hash[intf] = true
		}
		xcvrToIntfs[xcvr] = hash
	}
	if len(newReq) == 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("Invalid arguments!")
	}

	return xcvrs, newReq, intfToXcvr, xcvrToIntfs, resp, nil
}

// SetTransceiverState implements the corresponding RPC.
func (srv *Server) SetTransceiverState(ctx context.Context, req *bbpb.SetTransceiverStateRequest) (*bbpb.SetTransceiverStateResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox SetTransceiverState RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox SetTransceiverState RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("Blackbox Test: SetTransceiverState")

	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(sc)

	np, err := common_utils.NewNotificationProducer(setHwLinkReqCh)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer np.Close()

	hash, err := getIntfState()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	xcvrs, newReq, intfToXcvr, xcvrToIntfs, resp, err := processXcvrReq(req, hash)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), setHwLinkRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	channel := sub.Channel()

	// Publish to notification channel.
	if err := np.Send(setHwLinkOp, "", newReq); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	tc := time.After(notificationResponseTimeout)
	for {
		select {
		case msg := <-channel:
			_, _, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				log.V(lvl.ERROR).Info(err.Error())
			}

			for f, v := range fvs {
				xcvr, ok := intfToXcvr[f]
				delete(intfToXcvr, f)
				if !ok {
					continue
				}
				data, dataOk := xcvrs[xcvr]
				if !dataOk {
					log.V(lvl.ERROR).Info("Cannot retrieve xcvr data for ", xcvr)
					continue
				}

				if data.processed {
					continue
				}

				sCh := strings.SplitN(v, ":", 2)
				if len(sCh) < 2 {
					log.V(lvl.ERROR).Info("Malformed response ", v, " in notification channel ", msg.Channel)
					continue
				}

				if swssToErrorCode(sCh[0]) != codes.OK {
					data.processed = true
					xcvrs[xcvr] = data
					resp.TransceiverResponses = append(resp.TransceiverResponses, setXcvrResp(data.xcvr, swssToErrorCode(sCh[0]), sCh[1]))
					continue
				}

				_, intfsOk := xcvrToIntfs[xcvr]
				if !intfsOk {
					log.V(lvl.ERROR).Info("Cannot retrieve xcvr to intfs map for ", xcvr)
					continue
				}

				delete(xcvrToIntfs[xcvr], f)
				if len(xcvrToIntfs[xcvr]) == 0 {
					resp.TransceiverResponses = append(resp.TransceiverResponses, setXcvrResp(data.xcvr, swssToErrorCode(sCh[0]), sCh[1]))
				}

			}
			if len(intfToXcvr) == 0 {
				return resp, nil
			}

		case <-tc:
			// Timeout
			return nil, status.Errorf(codes.Internal, "Timeout!")
		}
	}
	return resp, nil
}

func setHwLinkStateStatus(intf *types.Path, code codes.Code, msg string) *bbpb.SetHardwareLinkStateResponse_SetHardwareLinkStateStatus {
	return &bbpb.SetHardwareLinkStateResponse_SetHardwareLinkStateStatus{
		Interface: intf,
		Status: &apistatus.Status{
			Code:    int32(code),
			Message: msg,
		},
	}
}

type linkInfo struct {
	intf  *types.Path
	state string
}

// Processes message payload as op, data, field-value pairs.
func processMsgPayload(pload string) (string, string, map[string]string, error) {
	var payload []string
	if err := json.Unmarshal([]byte(pload), &payload); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return "", "", nil, err
	}

	if len(payload) < 2 || len(payload)%2 != 0 {
		return "", "", nil, fmt.Errorf("Payload is malformed: %v\n", strings.Join(payload, ","))
	}

	op := payload[0]
	data := payload[1]
	fvs := map[string]string{}
	for i := 2; i < len(payload); i += 2 {
		fvs[payload[i]] = payload[i+1]
	}
	return op, data, fvs, nil
}

// Converts a SWSS error code string into a Google RPC code.
func swssToErrorCode(statusStr string) codes.Code {
	switch statusStr {
	case "SWSS_RC_SUCCESS":
		return codes.OK
	case "SWSS_RC_UNKNOWN":
		return codes.Unknown
	case "SWSS_RC_IN_USE", "SWSS_RC_INVALID_PARAM":
		return codes.InvalidArgument
	case "SWSS_RC_DEADLINE_EXCEEDED":
		return codes.DeadlineExceeded
	case "SWSS_RC_NOT_FOUND":
		return codes.NotFound
	case "SWSS_RC_EXISTS":
		return codes.AlreadyExists
	case "SWSS_RC_PERMISSION_DENIED":
		return codes.PermissionDenied
	case "SWSS_RC_FULL", "SWSS_RC_NO_MEMORY":
		return codes.ResourceExhausted
	case "SWSS_RC_UNIMPLEMENTED":
		return codes.Unimplemented
	case "SWSS_RC_INTERNAL":
		return codes.Internal
	case "SWSS_RC_NOT_EXECUTED", "SWSS_RC_FAILED_PRECONDITION":
		return codes.FailedPrecondition
	case "SWSS_RC_UNAVAIL":
		return codes.Unavailable
	}
	return codes.Internal
}

// SetHardwareLinkState implements the corresponding RPC.
func (srv *Server) SetHardwareLinkState(ctx context.Context, req *bbpb.SetHardwareLinkStateRequest) (*bbpb.SetHardwareLinkStateResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox SetHardwareLinkState RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox SetHardwareLinkState RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.INFO).Info("Blackbox Test: SetHardwareLinkState")

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(cc)

	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(sc)

	np, err := common_utils.NewNotificationProducer(setHwLinkReqCh)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer np.Close()

	resp := &bbpb.SetHardwareLinkStateResponse{}
	intfs := map[string]linkInfo{}
	newReq := map[string]string{}
	for _, link := range req.GetLinkRequests() {
		intf, err := validateIntf(link.GetInterface(), cc)
		if err == nil {
			info := linkInfo{intf: link.GetInterface(), state: "down"}
			if link.GetEnabled() {
				info.state = "up"
			}
			intfs[intf] = info
			newReq[intf] = intfs[intf].state
			continue
		}
		resp.LinkResponses = append(resp.LinkResponses, setHwLinkStateStatus(link.GetInterface(), codes.InvalidArgument, err.Error()))
	}
	if len(newReq) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid arguments!")
	}

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), setHwLinkRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	channel := sub.Channel()

	// Publish to notification channel.
	if err := np.Send(setHwLinkOp, "", newReq); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	tc := time.After(notificationResponseTimeout)
	for {
		select {
		case msg := <-channel:
			_, _, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				log.V(lvl.ERROR).Info(err.Error())
			}

			for f, v := range fvs {
				info, ok := intfs[f]
				if ok {
					sCh := strings.SplitN(v, ":", 2)
					if len(sCh) < 2 {
						log.V(lvl.ERROR).Infof("Malformed response [%v] in notification channel %s.", v, msg.Channel)
						continue
					}
					resp.LinkResponses = append(resp.LinkResponses, setHwLinkStateStatus(info.intf, swssToErrorCode(sCh[0]), sCh[1]))
					delete(intfs, f)
				}
				if len(intfs) == 0 {
					return resp, nil
				}
			}
		case <-tc:
			// Timeout.
			return nil, status.Errorf(codes.DeadlineExceeded, "Response timeout!")
		}
	}
	return resp, nil
}

// validComp maps valid components for easy lookup.
func validComp() map[string]bool {
	ret := map[string]bool{}
	for k, _ := range common_utils.AllComponents {
		ret[k.String()] = true
	}
	return ret
}

func writeToCompStateTbl(req *bbpb.SetAlarmRequest) error {
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return fmt.Errorf("REDIS is not available.")
	}
	defer db.CloseRedisClient(sc)

	// Populate entry.
	// COMPONENT_STATE_TABLE has the following fields: `state`, `reason`, `timestamp`, `timestamp-seconds`, and `timestamp-nanoseconds`.
	hash := make(map[string]interface{})
	des := req.GetDescription()
	if strings.HasPrefix(des, "MINOR:") {
		hash["state"] = "MINOR"
		hash["reason"] = strings.TrimSpace(strings.TrimPrefix(des, "MINOR:"))
	} else if strings.HasPrefix(des, "ERROR:") {
		hash["state"] = "ERROR"
		hash["reason"] = strings.TrimSpace(strings.TrimPrefix(des, "ERROR:"))
	} else if strings.HasPrefix(des, "INACTIVE:") {
		hash["state"] = "INACTIVE"
		hash["reason"] = strings.TrimSpace(strings.TrimPrefix(des, "INACTIVE:"))
	} else {
		hash["state"] = "ERROR"
		hash["reason"] = des
	}
	if req.Severity == bbpb.OpenconfigAlarmTypesOPENCONFIGALARMSEVERITY_OPENCONFIGALARMTYPESOPENCONFIGALARMSEVERITY_MINOR {
		hash["state"] = "MINOR"
	}
	ts := time.Now()
	hash["timestamp"] = ts.String()
	hash["timestamp-seconds"] = strconv.FormatInt(ts.Unix(), 10)
	hash["timestamp-nanoseconds"] = strconv.Itoa(ts.Nanosecond())
	if req.Type == bbpb.OpenconfigAlarmTypesOPENCONFIGALARMTYPEID_OPENCONFIGALARMTYPESOPENCONFIGALARMTYPEID_EQPT {
		hash["hw-err"] = "true"
	} else {
		hash["hw-err"] = "false"
	}
	if req.Severity == bbpb.OpenconfigAlarmTypesOPENCONFIGALARMSEVERITY_OPENCONFIGALARMTYPESOPENCONFIGALARMSEVERITY_CRITICAL {
		hash["essential"] = "true"
	} else {
		hash["essential"] = "false"
	}

	if err := sc.HMSet(context.Background(), getKey([]string{compStTbl, req.GetResource()}), hash).Err(); err != nil {
		log.V(lvl.ERROR).Infof("Cannot write to the COMPONENT_STATE_TABLE in the DB.")
		return err
	}
	return nil
}

// SetAlarm implements the corresponding RPC.
func (srv *Server) SetAlarm(ctx context.Context, req *bbpb.SetAlarmRequest) (*bbpb.SetAlarmResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox SetAlarm RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox SetAlarm RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.DEBUG).Info("Blackbox Test: SetAlarm")

	// Check if request is valid. Check component.
	m := validComp()
	if _, ok := m[req.GetResource()]; !ok {
		log.V(lvl.ERROR).Info("Invalid component for SetAlarm!")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid component for SetAlarm!")
	}
	// Write to DB.
	if err := writeToCompStateTbl(req); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &bbpb.SetAlarmResponse{}, nil
}

func verifyStateResult(component string, code codes.Code, msg string) *bbpb.VerifyStateResponse_VerifyStateResult {
	return &bbpb.VerifyStateResponse_VerifyStateResult{
		Component: component,
		Status: &apistatus.Status{
			Code:    int32(code),
			Message: msg,
		},
	}
}

// VerifyState implements the corresponding RPC.
func (srv *Server) VerifyState(ctx context.Context, req *bbpb.VerifyStateRequest) (*bbpb.VerifyStateResponse, error) {
	// Reject while NSF Freeze is ongoing
	if srv.WarmRestartHelper.FetchFreezeStatus() {
		log.V(lvl.ERROR).Info("gNOI blackbox VerifyState RPC disabled since NSF is ongoing!")
		return nil, status.Errorf(codes.Unavailable, "gNOI blackbox VerifyState RPC disabled since NSF is ongoing!")
	}
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}
	log.V(lvl.DEBUG).Info("Blackbox Test: VerifyState")

	np, err := common_utils.NewNotificationProducer(stateVerReqChan)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer np.Close()

	sdb, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(sdb)

	// Subscribe to State DB for responses.
	// Redis namespace notification channel has format of "__keyspace@<namespace id>__:<key>".
	dbID, _ := sdcfg.GetDbId(stateDB, "")
	dbSeparator, _ := sdcfg.GetDbSeparator(stateDB, "")
	channelStr := "__keyspace@" + strconv.Itoa(dbID) + "__:" + stateVerRespTbl + dbSeparator + "*"
	sub := sdb.PSubscribe(context.Background(), channelStr)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	channel := sub.Channel()

	resp := &bbpb.VerifyStateResponse{Success: true}
	t := time.Now().Format("2006-01-02 15:04:05")
	wcs := map[string]bool{}

	// If the request does not come with any component, perform state verification to all supported components.
	cs := req.GetComponents()
	if len(cs) == 0 {
		cs = []string{"all"}
	}
	for _, c := range cs {
		// Write to notification channel.
		if err := np.Send(c, t, map[string]string{}); err != nil {
			log.V(lvl.ERROR).Info(err.Error())
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		wcs[c] = true
	}

	// Wait for responses.
	if len(wcs) == 0 {
		return resp, nil
	}
	tc := time.After(verifyStateTimeout)
	for {
		select {
		case msg := <-channel:
			splitedKeys := strings.SplitN(msg.Channel, ":", 2)
			if len(splitedKeys) < 2 {
				log.V(lvl.ERROR).Infof("Missing key in Redis namespace notification channel %s.", msg.Channel)
				continue
			}
			dbSeparator, _ := sdcfg.GetDbSeparator(stateDB, "")
			splitedTableKeys := strings.SplitN(splitedKeys[1], dbSeparator, 2)
			if len(splitedTableKeys) < 2 {
				log.V(lvl.ERROR).Infof("Missing table key in %s.", splitedKeys[1])
				continue
			}
			c := splitedTableKeys[1]
			if _, ok := wcs[c]; !ok {
				continue
			}
			r, err := sdb.HGetAll(context.Background(), splitedKeys[1]).Result()
			if err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				continue
			}
			if r["timestamp"] != t {
				continue
			}
			if r["status"] == "pass" {
				resp.Results = append(resp.Results, verifyStateResult(c, codes.OK, ""))
			} else {
				resp.Results = append(resp.Results, verifyStateResult(c, codes.Internal, r["err_str"]))
				resp.Success = false
			}
			delete(wcs, c)
			if len(wcs) == 0 {
				return resp, nil
			}
		case <-tc:
			// Timeout.
			for c, _ := range wcs {
				resp.Results = append(resp.Results, verifyStateResult(c, codes.DeadlineExceeded, "Component response timeout."))
				resp.Success = false
			}
			return resp, nil
		}
	}

	return resp, nil
}
