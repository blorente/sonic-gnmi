package gnmi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/sonic-net/sonic-gnmi/common_utils"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	debugpb "github.com/sonic-net/sonic-gnmi/proto/gnoi/debug"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
	ssc "github.com/sonic-net/sonic-gnmi/sonic_service_client"

	log "github.com/golang/glog"
	"github.com/openconfig/gnoi/healthz"
	types "github.com/openconfig/gnoi/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	compKey                  string        = "name"
	syncdKey                 string        = "syncd"
	aclRecarvingKey          string        = "acl_recarving"
	xcvrHlthReqCh            string        = "XCVRD_REQUEST_CHANNEL"
	xcvrHlthRespCh           string        = "XCVRD_RESPONSE_CHANNEL"
	getXcvrDbgDataOp         string        = "get_xcvr_debug_data"
	xcvrDbgDataFld           string        = "xcvr_debug_data"
	intfHlthReqCh            string        = "INTERFACE_DEBUG_DATA_REQUEST_CHANNEL"
	intfHlthRespCh           string        = "INTERFACE_DEBUG_DATA_RESPONSE_CHANNEL"
	getIntfDbgDataOp         string        = "get_interface_debug_data"
	intfDbgDataFld           string        = "interface_debug_data"
	ddComponentKey           string        = "component"
	ddComponentAll           string        = "all"
	ddLogLvlKey              string        = "level"
	ddLogLvlAlert            string        = "alert"
	ddLogLvlCritical         string        = "critical"
	ddLogLvlAll              string        = "all"
	ddLogLvlSuf              string        = "-info"
	ddFileSegSize            int           = 4096
	artifactSleepTime        time.Duration = 5 * time.Second
	NSF_REGISTRATION_TABLE   string        = "WARM_RESTART_REGISTRATION_TABLE"
	NSF_RESTART_TABLE        string        = "WARM_RESTART_TABLE"
	NSF_PERFORMANCE_TABLE    string        = "WARM_RESTART_PERFORMANCE_TABLE"
	NSF_PERF_HISTORY_TABLE   string        = "WARM_RESTART_PERFORMANCE_HISTORY"
	NSF_RECONCILIATION_TABLE string        = "WARM_RESTART_RECONCILIATION_TABLE"
	NSF_P4RT_TELEMETRY_TABLE string        = "P4RT_TELEMETRY"
)

var (
	artifactColTimeout time.Duration = 7 * time.Minute
)

type GNOIHealthzServer struct {
	*Server
}

func NewGNOIHealthzServer(srv *Server) *GNOIHealthzServer {
	return &GNOIHealthzServer{
		Server: srv,
	}
}

func isLogicalPort(intf *types.Path) bool {
	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return false
	}
	defer db.CloseRedisClient(cc)

	if _, err := validateIntf(intf, cc); err != nil {
		log.V(lvl.DEBUG).Info("Cannot validate interface: ", intf.String(), ", Error: ", err.Error())
		return false
	}
	return true
}

func getXcvrPath(xcvr string) *types.Path {
	return &types.Path{
		Origin: "openconfig",
		Elem: []*types.PathElem{
			{
				Name: "components",
			},
			{
				Name: "component",
				Key: map[string]string{
					"name": xcvr,
				},
			},
		},
	}
}

func getXcvrFromIntf(path *types.Path) (string, error) {
	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return "", status.Errorf(codes.Internal, err.Error())
	}
	defer db.CloseRedisClient(cc)

	intf, err := validateIntf(path, cc)
	if err != nil {
		log.V(lvl.ERROR).Info("Cannot validate interface: ", path.String(), ", Error: ", err.Error())
		return "", err
	}

	hash, err := getIntfState()
	if err != nil {
		return "", err
	}

	for itf, data := range hash {
		if itf != intf {
			continue
		}
		port, ok := data["index"]
		if !ok {
			log.V(lvl.ERROR).Info("Index is not populated for interface ", intf)
			return "", fmt.Errorf("Index is not populated for interface %v.", intf)
		}
		return "Ethernet" + port, nil
	}
	return "", fmt.Errorf("Cannot find xcvr info for interface %v.", intf)
}

func getLogicalPortData(req *healthz.GetRequest) (*healthz.GetResponse, error) {
	// Get interface debug data.
	debugResp, err := processLogicalPort(req.GetPath())
	if err != nil {
		return nil, err
	}

	// Get xcvr debug data.
	xcvr, err := getXcvrFromIntf(req.GetPath())
	if err != nil {
		return nil, err
	}
	xcvrResp, err := processXcvr(getXcvrPath(xcvr))
	if err != nil {
		return nil, err
	}
	for _, page := range xcvrResp.GetTransceiverEepromPages() {
		debugResp.TransceiverEepromPages = append(debugResp.TransceiverEepromPages, page)
	}

	resp := &healthz.GetResponse{}
	any := &anypb.Any{}
	if err := any.MarshalFrom(debugResp); err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot marshal the response!")
	}
	resp.Component = &healthz.ComponentStatus{
		Path:    req.GetPath(),
		Healthz: any,
	}
	return resp, nil
}

func processLogicalPort(path *types.Path) (*debugpb.PortDebugData, error) {
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer db.CloseRedisClient(sc)

	np, err := common_utils.NewNotificationProducer(intfHlthReqCh)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer np.Close()

	cc, err := getRedisDBClient(configDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer db.CloseRedisClient(cc)
	intf, err := validateIntf(path, cc)
	if err != nil {
		log.V(lvl.ERROR).Info("Cannot validate interface: ", path.String(), ", Error: ", err.Error())
		return nil, err
	}

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), intfHlthRespCh)
	if _, err := sub.Receive(context.Background()); err != nil {
		return nil, err
	}

	channel := sub.Channel()

	// Publish to notification channel.
	if err := np.Send(getIntfDbgDataOp, intf, map[string]string{}); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}

	// Wait for responses. Use the same timeout as VerifyState.
	tc := time.After(verifyStateTimeout)
	for {
		select {
		case msg := <-channel:
			op, data, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				continue
			}

			if op != getIntfDbgDataOp || data != intf {
				log.V(lvl.ERROR).Info("Op: [", op, "], or data: [", data, "] don't match!")
				continue
			}
			intfData, ok := fvs[intfDbgDataFld]
			if !ok {
				continue
			}
			debugResp := &debugpb.PortDebugData{
				PhyData: intfData,
			}

			return debugResp, nil
		case <-tc:
			// Timeout.
			return nil, status.Errorf(codes.DeadlineExceeded, "Response timeout!")
		}
	}
}

func isXcvr(xcvr *types.Path) bool {
	if _, err := validateAndGetXcvr(xcvr); err != nil {
		log.V(lvl.INFO).Info("Cannot validate xcvr: ", xcvr.String(), ", Error: ", err.Error())
		return false
	}
	return true
}

func populatePortDebugData(m map[string][]string) (*debugpb.PortDebugData, error) {
	debugResp := &debugpb.PortDebugData{}
	for k, v := range m {
		num, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("Cannot parse eeprom page number: [%v]!", k)
		}

		buf := &bytes.Buffer{}
		value := new(big.Int)
		for _, hexStr := range v {
			value.SetString(hexStr, 16)
			buf.WriteByte(byte(value.Int64()))
		}
		page := &debugpb.PortDebugData_TransceiverEepromPage{
			PageNum:       int32(num),
			EepromContent: buf.Bytes(),
		}
		debugResp.TransceiverEepromPages = append(debugResp.TransceiverEepromPages, page)
	}
	return debugResp, nil
}

func getXcvrData(req *healthz.GetRequest) (*healthz.GetResponse, error) {
	debugResp, err := processXcvr(req.GetPath())
	if err != nil {
		return nil, err
	}
	resp := &healthz.GetResponse{}
	any := &anypb.Any{}
	if err := any.MarshalFrom(debugResp); err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot marshal the response!")
	}
	resp.Component = &healthz.ComponentStatus{
		Path:    req.GetPath(),
		Healthz: any,
	}
	return resp, nil
}

func processXcvr(path *types.Path) (*debugpb.PortDebugData, error) {
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer db.CloseRedisClient(sc)

	np, err := common_utils.NewNotificationProducer(xcvrHlthReqCh)
	if err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}
	defer np.Close()

	xcvr, err := validateAndGetXcvr(path)
	if err != nil {
		return nil, err
	}

	// Subscribe to the response channel.
	sub := sc.Subscribe(context.Background(), xcvrHlthRespCh)
	if _, err = sub.Receive(context.Background()); err != nil {
		return nil, err
	}
	channel := sub.Channel()

	// Publish to notification channel.
	if err := np.Send(getXcvrDbgDataOp, xcvr, map[string]string{}); err != nil {
		log.V(lvl.ERROR).Info(err.Error())
		return nil, err
	}

	// Wait for responses. Use the same timeout as VerifyState.
	tc := time.After(verifyStateTimeout)
	for {
		select {
		case msg := <-channel:
			op, data, fvs, err := processMsgPayload(msg.Payload)
			if err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				continue
			}

			if op != getXcvrDbgDataOp || data != xcvr {
				log.V(lvl.ERROR).Info("Op: [", op, "], or data: [", data, "] don't match!")
				continue
			}
			eeprm, ok := fvs[xcvrDbgDataFld]
			if !ok {
				continue
			}
			m := map[string][]string{}
			if err := json.Unmarshal([]byte(eeprm), &m); err != nil {
				log.V(lvl.ERROR).Info(err.Error())
				return nil, status.Errorf(codes.Internal, "Cannot unmarshal eeprom data: [%v]!", eeprm)
			}

			debugResp, err := populatePortDebugData(m)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "Error: [%v].", err.Error())
			}

			return debugResp, nil
		case <-tc:
			// Timeout.
			return nil, status.Errorf(codes.DeadlineExceeded, "Response timeout!")
		}
	}
}

func isDebugData(p *types.Path) bool {
	elems := p.GetElem()
	if len(elems) != 4 {
		return false
	}
	if elems[0].GetName() != "components" || len(elems[0].GetKey()) > 0 {
		return false
	}
	if elems[1].GetName() != "component" || len(elems[1].GetKey()) != 1 {
		return false
	}
	if _, ok := elems[1].GetKey()["name"]; !ok {
		return false
	}
	if elems[2].GetName() != "healthz" || len(elems[2].GetKey()) > 0 {
		return false
	}
	if (elems[3].GetName() != ddLogLvlAlert+ddLogLvlSuf && elems[3].GetName() != ddLogLvlCritical+ddLogLvlSuf && elems[3].GetName() != ddLogLvlAll+ddLogLvlSuf) || len(elems[3].GetKey()) > 0 {
		return false
	}
	return true
}

func waitForArtifact(file string) error {
	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return err
	}
	for start := time.Now(); time.Since(start) < artifactColTimeout; {
		if _, err := sc.HealthzCheck(file); err == nil {
			return nil
		}
		time.Sleep(artifactSleepTime)
	}
	return fmt.Errorf("Artifact collection timeout on file %v", file)
}

func getDebugData(p *types.Path) (*healthz.GetResponse, error) {
	c := ddComponentAll
	ll := ddLogLvlAlert
	elems := p.GetElem()
	if len(elems) == 4 {
		c, _ = elems[1].GetKey()["name"]
		ll = strings.TrimSuffix(elems[3].GetName(), ddLogLvlSuf)
	}
	req := map[string]string{
		ddComponentKey: c,
		ddLogLvlKey:    ll,
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error: [%w]", err)
	}

	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	s, err := sc.HealthzCollect(string(b))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Host service error: %w", err)
	}

	// Wait for artifact file to be ready.
	if err := waitForArtifact(s); err != nil {
		return nil, status.Errorf(codes.Internal, "Error: [%w]", err)
	}

	fi, err := os.ReadFile(s)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error: [%w]", err)
	}
	h := sha256.Sum256(fi)

	resp := &healthz.GetResponse{}
	resp.Component = &healthz.ComponentStatus{
		Path: p,
		Id:   s,
		Artifacts: []*healthz.ArtifactHeader{
			&healthz.ArtifactHeader{
				Id: s,
				ArtifactType: &healthz.ArtifactHeader_File{
					File: &healthz.FileArtifactType{
						Name: s,
						Size: int64(len(fi)),
						Hash: &types.HashType{
							Method: types.HashType_SHA256,
							Hash:   h[:],
						},
					},
				},
			},
		},
	}
	return resp, nil
}

var nsfTableList = []string{
	NSF_REGISTRATION_TABLE,
	NSF_RESTART_TABLE,
	NSF_PERFORMANCE_TABLE,
	NSF_PERF_HISTORY_TABLE,
	NSF_RECONCILIATION_TABLE,
	NSF_P4RT_TELEMETRY_TABLE,
}

func fetchStatusTimestamp(rc *redis.Client, key string) *debugpb.NSFDebugData_StatusTimestamp {
	statusTime := &debugpb.NSFDebugData_StatusTimestamp{}
	entry, err := rc.HGetAll(context.Background(), key).Result()
	if err != nil {
		log.V(lvl.DEBUG).Info("Cannot get key: ", key, "; Error: ", err)
		return statusTime
	}
	statusTime.Status = entry["status"]
	startt, _ := time.Parse("2006-01-02.15:04:05.999999", entry["start-timestamp"])
	statusTime.StartTimestamp = timestamppb.New(startt)
	stopt, _ := time.Parse("2006-01-02.15:04:05.999999", entry["finish-timestamp"])
	statusTime.StopTimestamp = timestamppb.New(stopt)
	return statusTime
}

func fetchPerformanceInfo(rc *redis.Client, keyCursor int, keys []string) (*debugpb.NSFDebugData_WarmbootPerformanceInfo, string) {
	info := &debugpb.NSFDebugData_WarmbootPerformanceInfo{}
	status := ""
	ns, _ := sdcfg.GetDbDefaultNamespace()
	sep, _ := sdcfg.GetDbSeparator(stateDB, ns)
	for _, key := range keys {
		splitTableKeys := strings.SplitN(key, sep, 4)
		if len(splitTableKeys) < keyCursor+1 {
			continue
		}
		stage := splitTableKeys[keyCursor]
		if stage != "system" && stage != common_utils.RegistrationFreezeKey && stage != common_utils.RegistrationUnfreezeKey && stage != common_utils.RegistrationCheckpointKey && stage != common_utils.RegistrationReconciliationKey {
			continue
		}
		statusTime := fetchStatusTimestamp(rc, key)
		switch {
		case len(splitTableKeys) == keyCursor+1:
			perfInfo := &debugpb.NSFDebugData_WarmbootPerformanceOverall{}
			perfInfo.Key = stage
			perfInfo.StatusTimestamp = statusTime
			if stage == "system" {
				info.SystemPerformance = perfInfo
				status = perfInfo.StatusTimestamp.Status
				break
			}
			info.PerStagePerformance = append(info.PerStagePerformance, perfInfo)
		case len(splitTableKeys) == keyCursor+2:
			perfInfo := &debugpb.NSFDebugData_WarmbootPerformancePerApplication{}
			perfInfo.WarmbootStage = stage
			perfInfo.Application = splitTableKeys[keyCursor+1]
			perfInfo.StatusTimestamp = statusTime
			info.PerAppPerformance = append(info.PerAppPerformance, perfInfo)
		}
	}
	return info, status
}

// This is under development based on Syncd counters update.
func fetchSyncdInfo(rc *redis.Client, tblName string) *debugpb.NSFDebugData_SyncdWarmbootStatistics {
	info := &debugpb.NSFDebugData_SyncdWarmbootStatistics{}

	// WARM_RESTART_RECONCILIATION_TABLE|syncd
	entry, err := rc.HGetAll(context.Background(), tblName+"|"+syncdKey).Result()
	if err != nil {
		log.V(lvl.DEBUG).Info("Cannot get key: ", syncdKey, "; Error: ", err)
		return info
	}
	info.AsicOperations, _ = strconv.ParseUint(entry["asic-operations"], 10, 64)
	for op, opVal := range entry {
		opInfo := &debugpb.NSFDebugData_SyncdObjectOperation{}
		splitFieldKeys := strings.SplitN(op, ":", 2)
		if len(splitFieldKeys) != 2 && !strings.HasPrefix(splitFieldKeys[0], "SAI_OBJECT_TYPE") {
			continue
		}
		opInfo.ObjectType = splitFieldKeys[0]
		opInfo.AsicOperationType = splitFieldKeys[1]
		opInfo.NumberOfAsicOperations, _ = strconv.ParseUint(opVal, 10, 64)
		info.SyncdObjectOperation = append(info.SyncdObjectOperation, opInfo)
	}

	// WARM_RESTART_RECONCILIATION_TABLE|syncd|heuristicObjects
	randomMatchInfo := &debugpb.NSFDebugData_SyncdRandomMatchInfo{}
	entry, err = rc.HGetAll(context.Background(), tblName+"|"+syncdKey+"|heuristicObjects").Result()
	if err != nil {
		log.V(lvl.DEBUG).Info("Cannot get key: ", syncdKey+"|heuristicObjects", "; Error: ", err)
		return info
	}
	for k, v := range entry {
		if k == "random-operations" {
			randomMatches, _ := strconv.ParseUint(v, 10, 64)
			randomMatchInfo.RandomMatches = randomMatches
		} else {
			object := &debugpb.NSFDebugData_SyncdRandomMatchInfo_SyncdRandomMatchObjectInfo{}
			object.ObjectType = k
			object.Oid = strings.Split(v, ",")
			randomMatchInfo.RandomMatchObjects = append(randomMatchInfo.RandomMatchObjects, object)
		}
	}
	info.RandomMatchInfo = randomMatchInfo

	log.V(lvl.DEBUG).Infof("NSF Syncd Healthz info: %v", info)
	return info
}

// fetchACLRecarvingInfo fetches ACL recarving from redis dB and
// populate data in the form of debugpb.
func fetchACLRecarvingInfo(rc *redis.Client, tblName string) *debugpb.NSFDebugData_P4RTAclRecarvingTelemetry {
	info := &debugpb.NSFDebugData_P4RTAclRecarvingTelemetry{}
	entry, err := rc.HGetAll(context.Background(), tblName+"|"+aclRecarvingKey).Result()
	if err != nil {
		log.V(lvl.ERROR).Info("Cannot get data for key: ", aclRecarvingKey, " in Table:", tblName, "; Error: ", err)
		return info
	}

	info.AddedTables = strings.Split(strings.TrimSpace(entry["added_tables"]), ",")
	info.RemovedTables = strings.Split(strings.TrimSpace(entry["removed_tables"]), ",")
	info.ModifiedEssentialTables = strings.Split(strings.TrimSpace(entry["modified_essential_tables"]), ",")
	info.ModifiedNonEssentialTables = strings.Split(strings.TrimSpace(entry["modified_nonessential_tables"]), ",")
	info.Timestamp = timestamppb.Now()
	log.V(lvl.DEBUG).Infof("NSF ACL Recarving info: %v", info)
	return info
}

func getNSFDebugInfoFromDb(rc *redis.Client, debugResp *debugpb.NSFDebugData, tblName string) error {

	keys, err := rc.Keys(context.Background(), tblName+"|*").Result()
	if err != nil || len(keys) < 1 {
		return err
	}
	ns, _ := sdcfg.GetDbDefaultNamespace()
	sep, _ := sdcfg.GetDbSeparator(stateDB, ns)

	// This can be optimized for different proto type conversions.
	switch tblName {
	case NSF_REGISTRATION_TABLE:
		for _, key := range keys {
			info := &debugpb.NSFDebugData_WarmbootRegistrationInfo{}
			splitTableKeys := strings.SplitN(key, sep, 3)
			if len(splitTableKeys) != 3 {
				continue
			}
			info.Docker = splitTableKeys[1]
			info.Application = splitTableKeys[2]
			entry, err := rc.HGetAll(context.Background(), key).Result()
			if err != nil {
				log.V(lvl.DEBUG).Info("Cannot get key: ", key, "; Error: ", err)
				debugResp.WarmbootRegistrationInfo = append(debugResp.WarmbootRegistrationInfo, info)
				continue
			}
			info.StopOnFreeze, _ = strconv.ParseBool(entry[common_utils.RegistrationStopOnFreezeKey])
			info.Freeze, _ = strconv.ParseBool(entry[common_utils.RegistrationFreezeKey])
			info.Checkpoint, _ = strconv.ParseBool(entry[common_utils.RegistrationCheckpointKey])
			info.Reconciliation, _ = strconv.ParseBool(entry[common_utils.RegistrationReconciliationKey])
			ts, _ := time.Parse("2006-01-02.15:04:05.999999", entry["timestamp"])
			info.Timestamp = timestamppb.New(ts)
			debugResp.WarmbootRegistrationInfo = append(debugResp.WarmbootRegistrationInfo, info)
		}
	case NSF_RESTART_TABLE:
		for _, key := range keys {
			info := &debugpb.NSFDebugData_WarmbootStateInfo{}
			splitTableKeys := strings.SplitN(key, sep, 2)
			if len(splitTableKeys) != 2 {
				continue
			}
			info.Application = splitTableKeys[1]
			entry, err := rc.HGetAll(context.Background(), key).Result()
			if err != nil {
				log.V(lvl.DEBUG).Info("Cannot get key: ", key, "; Error: ", err)
				debugResp.WarmbootStateInfo = append(debugResp.WarmbootStateInfo, info)
				continue
			}
			info.RestoreCount, _ = strconv.ParseUint(entry["restore_count"], 10, 64)
			ts, _ := time.Parse("2006-01-02.15:04:05.999999", entry["timestamp"])
			info.State = entry["state"]
			info.Timestamp = timestamppb.New(ts)
			debugResp.WarmbootStateInfo = append(debugResp.WarmbootStateInfo, info)
		}
	case NSF_PERFORMANCE_TABLE:
		info, nsfStatus := fetchPerformanceInfo(rc, 1, keys)
		debugResp.LastWarmbootPerformanceInfo = info
		debugResp.NsfStatus = nsfStatus
	case NSF_PERF_HISTORY_TABLE:
		bcMap := make(map[string]bool, 0)
		for _, key := range keys {
			info := &debugpb.NSFDebugData_WarmbootHistoricalInfoPerBoot{}
			splitTableKeys := strings.SplitN(key, sep, 4)
			if len(splitTableKeys) < 1 {
				continue
			}
			bcStr := splitTableKeys[1]
			if _, ok := bcMap[bcStr]; ok {
				continue
			}
			bcTable := tblName + sep + bcStr
			entry, err := rc.HGetAll(context.Background(), bcTable).Result()
			if err != nil {
				log.V(lvl.DEBUG).Info("Cannot get key: ", key, "; Error: ", err)
				continue
			}
			bcMap[bcStr] = true
			bc, _ := strconv.ParseInt(bcStr, 10, 64)
			info.BootCount = int32(bc)
			info.FirmwareVersion = entry["firmware-version"]
			startt, _ := time.Parse("2006-01-02.15:04:05.999999", entry["stack-start-timestamp"])
			info.StackStartTimestamp = timestamppb.New(startt)
			stopt, _ := time.Parse("2006-01-02.15:04:05.999999", entry["stack-stop-timestamp"])
			info.StackStopTimestamp = timestamppb.New(stopt)
			info.StackUptimeSeconds, _ = strconv.ParseUint(entry["stack-uptime-seconds"], 10, 64)
			info.KernelUptimeSeconds, _ = strconv.ParseUint(entry["kernel-uptime-seconds"], 10, 64)
			info.SsdRegion = entry["ssd-region"]
			rct, _ := strconv.ParseUint(entry["reset-count"], 10, 64)
			info.ResetCount = uint32(rct)

			bcKeys, err := rc.Keys(context.Background(), bcTable+sep+"*").Result()
			if err != nil || len(bcKeys) < 1 {
				continue
			}
			pInfo, _ := fetchPerformanceInfo(rc, 2, bcKeys)
			info.WarmbootPerformanceInfo = pInfo
			debugResp.WarmbootHistoricalInfoPerBoot = append(debugResp.WarmbootHistoricalInfoPerBoot, info)
		}
	case NSF_P4RT_TELEMETRY_TABLE:
		debugResp.P4RtAclRecarvingTelemetry = fetchACLRecarvingInfo(rc, tblName)
	case NSF_RECONCILIATION_TABLE:
		info := fetchSyncdInfo(rc, tblName)
		debugResp.SyncdWarmbootStats = info
	default:
		log.V(lvl.DEBUG).Infof("Continuing since table name = %v is not supported", tblName)
	}
	return err
}

func populateNSFDebugData(path *types.Path) (*debugpb.NSFDebugData, error) {
	sc, err := getRedisDBClient(stateDB)
	if err != nil {
		log.V(lvl.DEBUG).Info(err.Error())
		return nil, err
	}
	defer db.CloseRedisClient(sc)
	debugResp := &debugpb.NSFDebugData{}
	for _, tblName := range nsfTableList {
		if err := getNSFDebugInfoFromDb(sc, debugResp, tblName); err != nil {
			log.V(lvl.DEBUG).Infof("Error Getting NSF info from State DB for table %v: err = %v", tblName, err.Error())
		}
	}

	return debugResp, nil
}

func fetchNSFDebugData(reqPath *types.Path) (*healthz.GetResponse, error) {
	// Get NSF debug data.
	nsfDebugResp, err := populateNSFDebugData(reqPath)
	if err != nil {
		return nil, err
	}

	resp := &healthz.GetResponse{}
	any := &anypb.Any{}
	if err := any.MarshalFrom(nsfDebugResp); err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot marshal the NSF Debug Response")
	}
	resp.Component = &healthz.ComponentStatus{
		Path:    reqPath,
		Healthz: any,
	}
	return resp, nil
}

func isNSFDebugData(p *types.Path) bool {
	if p == nil {
		return false
	}
	// Check both OpenConfig and SONiC formats.
	if p.GetOrigin() != "openconfig" && p.GetOrigin() != "openconfig-platform" {
		return false
	}
	elems := p.GetElem()
	if len(elems) != 2 {
		return false
	}
	if elems[0].GetName() != "components" || len(elems[0].GetKey()) > 0 {
		return false
	}
	if elems[1].GetName() != "component" || len(elems[1].GetKey()) != 1 {
		return false
	}
	if name, ok := elems[1].GetKey()["name"]; !ok || name != "NSF" {
		return false
	}
	return true
}

// Get implements the corresponding RPC.
func (srv *GNOIHealthzServer) Get(ctx context.Context, req *healthz.GetRequest) (*healthz.GetResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	log.V(lvl.INFO).Info("gNOI: healthz.Get")

	switch {
	case isNSFDebugData(req.GetPath()):
		return fetchNSFDebugData(req.GetPath())
	case isLogicalPort(req.GetPath()):
		return getLogicalPortData(req)
	case isXcvr(req.GetPath()):
		return getXcvrData(req)
	case isDebugData(req.GetPath()):
		return getDebugData(req.GetPath())
	default:
		return nil, status.Errorf(codes.Unimplemented, fmt.Sprintf("Healthz.Get is unimplemented for component: [%s].", req.GetPath()))
	}
}

// Acknowledge implements the corresponding RPC.
func (srv *GNOIHealthzServer) Acknowledge(ctx context.Context, req *healthz.AcknowledgeRequest) (*healthz.AcknowledgeResponse, error) {
	ctx, err := authenticate(srv.config, ctx)
	if err != nil {
		return nil, err
	}

	log.V(lvl.INFO).Info("gNOI: healthz.Acknowledge")

	sc, err := ssc.NewDbusClient(dbusCaller)
	if err != nil {
		return nil, err
	}
	_, err = sc.HealthzAck(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Host service error: %v", err)
	}

	return &healthz.AcknowledgeResponse{}, nil
}

// Artifact implements the corresponding RPC.
func (srv *GNOIHealthzServer) Artifact(req *healthz.ArtifactRequest, stream healthz.Healthz_ArtifactServer) error {
	file := req.GetId()
	fi, err := os.ReadFile(file)
	if err != nil {
		return status.Errorf(codes.NotFound, "File %v not found: [%v].", file, err.Error())
	}
	h := sha256.Sum256(fi)

	header := &healthz.ArtifactResponse{
		Contents: &healthz.ArtifactResponse_Header{
			Header: &healthz.ArtifactHeader{
				Id: file,
				ArtifactType: &healthz.ArtifactHeader_File{
					File: &healthz.FileArtifactType{
						Name: file,
						Size: int64(len(fi)),
						Hash: &types.HashType{
							Method: types.HashType_SHA256,
							Hash:   h[:],
						},
					},
				},
			},
		},
	}
	if err := stream.Send(header); err != nil {
		return err
	}

	for idx := 0; idx < len(fi); idx += ddFileSegSize {
		end := idx + ddFileSegSize
		if end > len(fi) {
			end = len(fi)
		}
		content := &healthz.ArtifactResponse{
			Contents: &healthz.ArtifactResponse_Bytes{
				Bytes: fi[idx:end],
			},
		}
		if err := stream.Send(content); err != nil {
			return err
		}
	}

	trailer := &healthz.ArtifactResponse{
		Contents: &healthz.ArtifactResponse_Trailer{
			Trailer: &healthz.ArtifactTrailer{},
		},
	}
	if err := stream.Send(trailer); err != nil {
		return err
	}

	return nil
}

func (srv *GNOIHealthzServer) List(ctx context.Context, req *healthz.ListRequest) (*healthz.ListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "gNOI Healthz List not implemented")
}

func (srv *GNOIHealthzServer) Check(ctx context.Context, req *healthz.CheckRequest) (*healthz.CheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "gNOI Healthz Check not implemented")
}
