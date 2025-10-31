//////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Dell, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//////////////////////////////////////////////////////////////////////////

package transformer

import (
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

const (
	/* sFlow tables */
	SFLOW_GLOBAL_TBL     = "SFLOW"
	SFLOW_COL_TBL        = "SFLOW_COLLECTOR"
	SFLOW_INTF_TBL       = "SFLOW_SESSION"       /* Session table in ConfigDb */
	SFLOW_STATE_INTF_TBL = "SFLOW_SESSION_TABLE" /* Session table in ApplStateDb */
	SFLOW_STATE_TBL      = "SFLOW_TABLE"
	COUNTERS             = "COUNTERS"

	/* sFlow keys */
	SFLOW_GLOBAL_KEY             = "global"
	SFLOW_ADMIN_KEY              = "admin_state"
	SFLOW_HEADER_BYTES           = "header_bytes"
	SFLOW_SAMPL_RATE_KEY         = "sample_rate"
	SFLOW_BACKOFF_SAMPL_RATE_KEY = "backoff_ingress_sample_rate"
	SFLOW_AGENT_KEY              = "agent_ip"
	SFLOW_POLLING_INTERVAL       = "polling_interval"
	SFLOW_INTF_NAME_KEY          = "name"
	SFLOW_COL_IP_KEY             = "collector_ip"
	SFLOW_COL_PORT_KEY           = "collector_port"
	SFLOW_KEY                    = "sflow"
	FEATURE_STATE                = "state"
	SFLOW_PKG_SENT               = "packets_sent"
	SFLOW_SAMPLE_CNT             = "sample_count"

	/* sFlow default values */
	DEFAULT_POLLING_INT = 20
	DEFAULT_AGENT       = "default"
	DEFAULT_VRF_NAME    = "default"
	DEFAULT_COL_PORT    = "6343"

	/* sFlow URIs */
	SAMPLING                                    = "/openconfig-sampling:sampling"
	SAMPLING_SFLOW                              = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow"
	SAMPLING_SFLOW_CONFIG                       = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/config"
	SAMPLING_SFLOW_CONFIG_ENABLED               = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/config/enabled"
	SAMPLING_SFLOW_CONFIG_SAMPLE_SIZE           = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/config/sample-size"
	SAMPLING_SFLOW_STATE                        = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/state"
	SAMPLING_SFLOW_STATE_ENABLED                = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/state/enabled"
	SAMPLING_SFLOW_STATE_SAMPLE_SIZE            = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/state/SAMPLE_SIZE"
	SAMPLING_SFLOW_COLS                         = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/collectors"
	SAMPLING_SFLOW_COLS_COL                     = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/collectors/collector"
	SAMPLING_SFLOW_COLS_COL_CONFIG              = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/collectors/collector/config"
	SAMPLING_SFLOW_COLS_COL_STATE               = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/collectors/collector/state"
	SAMPLING_SFLOW_INTFS                        = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces"
	SAMPLING_SFLOW_INTFS_INTF                   = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces/interface"
	SAMPLING_SFLOW_INTFS_INTF_CONFIG            = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces/interface/config"
	SAMPLING_SFLOW_INTFS_INTF_CONFIG_SAMPL_RATE = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces/interface/config/sampling-rate"
	SAMPLING_SFLOW_INTFS_INTF_CONFIG_ENABLED    = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces/interface/config/enabled"
	SAMPLING_SFLOW_INTFS_INTF_STATE             = "/openconfig-sampling:sampling/openconfig-sampling-sflow:sflow/interfaces/interface/state"

	/* IPv4/v6 localhost address */
	IPV4_LOCALHOST = "127.0.0.1"
	IPV6_LOCALHOST = "::1"
)

type Sflow struct {
	Enabled         string
	SampleSize      string
	AgentIp         string
	PollingInterval string
}

type SflowCol struct {
	Ip          string
	Port        string
	StateIp     string
	StatePort   string
	PacketsSent uint64
}

type SflowIntf struct {
	Enabled               string
	Sampling_Rate         string
	Packets_Sampled       uint64
	Backoff_Sampling_Rate string
}

func init() {
	XlateFuncBind("DbToYang_sflow_xfmr", DbToYang_sflow_xfmr)
	XlateFuncBind("YangToDb_sflow_xfmr", YangToDb_sflow_xfmr)
	XlateFuncBind("DbToYang_sflow_collector_xfmr", DbToYang_sflow_collector_xfmr)
	XlateFuncBind("YangToDb_sflow_collector_xfmr", YangToDb_sflow_collector_xfmr)
	XlateFuncBind("Subscribe_sflow_collector_xfmr", Subscribe_sflow_collector_xfmr)
	XlateFuncBind("DbToYang_sflow_interface_xfmr", DbToYang_sflow_interface_xfmr)
	XlateFuncBind("YangToDb_sflow_interface_xfmr", YangToDb_sflow_interface_xfmr)
	XlateFuncBind("Subscribe_sflow_interface_xfmr", Subscribe_sflow_interface_xfmr)
}

func getSflowRootObject(s *ygot.GoStruct) *ocbinds.OpenconfigSampling_Sampling {
	deviceObj := (*s).(*ocbinds.Device)
	return deviceObj.Sampling
}

var DbToYang_sflow_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	log.V(lvl.DEBUG).Infof("Received GET for sFlow Template: %s ,path: %s, vars: %v",
		pathInfo.Template, pathInfo.Path, pathInfo.Vars)

	log.V(lvl.DEBUG).Info("inParams.Uri:", inParams.requestUri)
	targetUriPath, _ := getYangPathFromUri(pathInfo.Path)
	return getSflow(getSflowRootObject(inParams.ygRoot), targetUriPath, inParams.uri, inParams.dbs[:])
}

var YangToDb_sflow_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	var err error
	res_map := make(map[string]map[string]db.Value)

	log.V(lvl.DEBUG).Info("sFlow SubTreeXfmr: ", inParams.uri)

	sflowObj := getSflowRootObject(inParams.ygRoot)
	global_map := make(map[string]db.Value)
	global_map[SFLOW_GLOBAL_KEY] = db.Value{Field: make(map[string]string)}

	if inParams.oper == DELETE {
		return res_map, errors.New("DELETE not supported")
	}

	if sflowObj.Sflow.Config.Enabled != nil {
		if *(sflowObj.Sflow.Config.Enabled) {
			global_map[SFLOW_GLOBAL_KEY].Field[SFLOW_ADMIN_KEY] = "up"
		} else {
			global_map[SFLOW_GLOBAL_KEY].Field[SFLOW_ADMIN_KEY] = "down"
		}
	}

	if sflowObj.Sflow.Config.SampleSize != nil {
		global_map[SFLOW_GLOBAL_KEY].Field[SFLOW_HEADER_BYTES] = strconv.FormatUint(uint64(*(sflowObj.Sflow.Config.SampleSize)), 10)
	}

	if sflowObj.Sflow.Config.AgentIdIpv6 != nil {
		global_map[SFLOW_GLOBAL_KEY].Field[SFLOW_AGENT_KEY] = *(sflowObj.Sflow.Config.AgentIdIpv6)
	}

	if sflowObj.Sflow.Config.PollingInterval != nil {
		global_map[SFLOW_GLOBAL_KEY].Field[SFLOW_POLLING_INTERVAL] = strconv.FormatUint(uint64(*(sflowObj.Sflow.Config.PollingInterval)), 10)
	}

	res_map[SFLOW_GLOBAL_TBL] = global_map
	return res_map, err
}

var DbToYang_sflow_collector_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	log.V(lvl.DEBUG).Infof("Received GET for sFlow Collector Template: %s ,path: %s, vars: %v",
		pathInfo.Template, pathInfo.Path, pathInfo.Vars)
	log.V(lvl.DEBUG).Info("inParams.Uri:", inParams.requestUri)
	targetUriPath, _ := getYangPathFromUri(pathInfo.Path)
	return getSflowCol(getSflowRootObject(inParams.ygRoot), targetUriPath, inParams.uri, inParams.dbs[:])
}

func makeColKey(uri string) string {
	ip := NewPathInfo(uri).Var("address")
	port := NewPathInfo(uri).Var("port")
	name := ""
	if ip != "" {
		name = ip + "_" + port
	}
	return name
}

func validColIP(ip string) bool {
	/* Allow localhost address for debugging purposes */
	return ip == IPV4_LOCALHOST || ip == IPV6_LOCALHOST || validIPv4(ip) || validIPv6(ip)
}

var YangToDb_sflow_collector_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	var err error
	res_map := make(map[string]map[string]db.Value)

	log.V(lvl.DEBUG).Info("sFlow Collector YangToDBSubTreeXfmr: ", inParams.uri)
	col_map := make(map[string]db.Value)
	sflowObj := getSflowRootObject(inParams.ygRoot)

	key := makeColKey(inParams.uri)
	if inParams.oper == DELETE {
		if key != "" {
			col_map[key] = db.Value{Field: make(map[string]string)}
		}
		res_map[SFLOW_COL_TBL] = col_map
		return res_map, err
	}

	if inParams.oper == REPLACE {
		// 1. get the existing collectors from Config DB.
		keys, err := inParams.d.GetKeys(&db.TableSpec{Name: SFLOW_COL_TBL})
		if err != nil {
			log.V(tlerr.ErrorSeverity(err)).Infof("Failed to get SFLOW_COLLECTOR keys : %v", err)
			return res_map, err
		}
		// 2. Get the data from Request. Compare with the keys from Config DB.
		checkCfg := make(map[string]bool)
		for col := range sflowObj.Sflow.Collectors.Collector {
			if !validColIP(col.Address) {
				return res_map, tlerr.InvalidArgs("Invalid collector IP")
			}
			port := strconv.FormatUint(uint64(col.Port), 10)
			keyInReq := col.Address + "_" + port
			checkCfg[keyInReq] = true
		}

		collectorsToDelete := map[string]db.Value{}
		for _, key := range keys {
			if _, ok := checkCfg[key.Get(0)]; !ok {
				collectorsToDelete[key.Get(0)] = db.Value{}
			}
		}

		// 3. Delete the entries if needed.
		if len(collectorsToDelete) > 0 {
			subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
			subOpMap[db.ConfigDB] = map[string]map[string]db.Value{
				SFLOW_COL_TBL: collectorsToDelete,
			}
			log.V(lvl.DEBUG).Infof("SFLOW_COLLECTOR cleanup, subOpMap[db.ConfigDB]: %v", subOpMap[db.ConfigDB])
			updateSubOpDataMap(subOpMap, DELETE, inParams)
		}
	}

	if key != "" {
		ip := NewPathInfo(inParams.uri).Var("address")
		port := NewPathInfo(inParams.uri).Var("port")
		if !validColIP(ip) {
			return res_map, tlerr.InvalidArgs("Invalid collector IP")
		}
		col_map[key] = db.Value{Field: make(map[string]string)}
		col_map[key].Field[SFLOW_COL_IP_KEY] = ip
		col_map[key].Field[SFLOW_COL_PORT_KEY] = port
	} else {
		for col := range sflowObj.Sflow.Collectors.Collector {
			if !validColIP(col.Address) {
				return res_map, tlerr.InvalidArgs("Invalid collector IP")
			}
			port := strconv.FormatUint(uint64(col.Port), 10)
			key = col.Address + "_" + port
			col_map[key] = db.Value{Field: make(map[string]string)}
			col_map[key].Field[SFLOW_COL_IP_KEY] = col.Address
			col_map[key].Field[SFLOW_COL_PORT_KEY] = port
		}
	}

	res_map[SFLOW_COL_TBL] = col_map
	return res_map, err
}

var Subscribe_sflow_collector_xfmr SubTreeXfmrSubscribe = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var err error
	var result XfmrSubscOutParams
	result.dbDataMap = make(RedisDbSubscribeMap)
	key := makeColKey(inParams.uri)
	if key == "" {
		key = "*"
	}

	log.V(lvl.DEBUG).Infof("XfmrSubscribe_sflow_collector_xfmr")
	result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {SFLOW_COL_TBL: {key: {}}}}
	log.V(lvl.DEBUG).Infof("Returning XfmrSubscribe_sflow_collector_xfmr")
	return result, err
}

var DbToYang_sflow_interface_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	pathInfo := NewPathInfo(inParams.uri)
	log.V(lvl.DEBUG).Infof("Received GET for sFlow Interface Template: %s ,path: %s, vars: %v",
		pathInfo.Template, pathInfo.Path, pathInfo.Vars)
	log.V(lvl.DEBUG).Info("inParams.Uri:", inParams.requestUri)
	targetUriPath, _ := getYangPathFromUri(pathInfo.Path)
	return getSflowIntf(getSflowRootObject(inParams.ygRoot), targetUriPath, inParams.uri, inParams.dbs[:])
}

var Subscribe_sflow_interface_xfmr SubTreeXfmrSubscribe = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var err error
	var result XfmrSubscOutParams
	key := NewPathInfo(inParams.uri).Var("name")

	if key == "" {
		key = "*"
	}

	log.V(lvl.DEBUG).Infof("XfmrSubscribe_sflow_interface_xfmr")
	result.dbDataMap = make(RedisDbSubscribeMap)
	result.dbDataMap = RedisDbSubscribeMap{db.ConfigDB: {SFLOW_INTF_TBL: {key: {}}}}

	log.V(lvl.DEBUG).Infof("Returning XfmrSubscribe_sflow_interface_xfmr")
	return result, err
}

var YangToDb_sflow_interface_xfmr SubTreeXfmrYangToDb = func(inParams XfmrParams) (map[string]map[string]db.Value, error) {
	var err error
	res_map := make(map[string]map[string]db.Value)

	log.V(lvl.DEBUG).Info("sFlow Interface YangToDBSubTreeXfmr: ", inParams.uri)
	intf_map := make(map[string]db.Value)
	sflowObj := getSflowRootObject(inParams.ygRoot)
	pathInfo := NewPathInfo(inParams.uri)
	targetUriPath, _ := getYangPathFromUri(pathInfo.Path)

	if inParams.oper == DELETE {
		if !strings.Contains(targetUriPath, SAMPLING_SFLOW_INTFS_INTF) {
			return res_map, tlerr.NotSupportedError{Format: "DELETE not supported", Path: targetUriPath}
		}

		name := NewPathInfo(inParams.uri).Var("name")
		if name == "" {
			return res_map, tlerr.InvalidArgs("Missing interface name")
		}
		intf_map[name] = db.Value{Field: make(map[string]string)}
		switch targetUriPath {
		case SAMPLING_SFLOW_INTFS_INTF_CONFIG_SAMPL_RATE:
			intf_map[name].Field[SFLOW_SAMPL_RATE_KEY] = ""
		case SAMPLING_SFLOW_INTFS_INTF_CONFIG_ENABLED:
			intf_map[name].Field[SFLOW_ADMIN_KEY] = ""
		case SAMPLING_SFLOW_INTFS_INTF:
			/* Delete all interface configurations */
		default:
			return res_map, errors.New("DELETE not supported on attribute or container")
		}
	} else {
		for _, intf := range sflowObj.Sflow.Interfaces.Interface {

			if intf.Name == nil {
				return res_map, errors.New("sFlow Interface: No interface name")
			}

			if intf.Config == nil {
				log.V(lvl.DEBUG).Infof("sFlow Inteface: No configuration")
				continue
			}

			name := *(intf.Name)
			intf_map[name] = db.Value{Field: make(map[string]string)}

			if intf.Config.Enabled != nil {
				if *(intf.Config.Enabled) {
					intf_map[name].Field[SFLOW_ADMIN_KEY] = "up"
				} else {
					intf_map[name].Field[SFLOW_ADMIN_KEY] = "down"
				}
			}

			if intf.Config.IngressSamplingRate != nil {
				intf_map[name].Field[SFLOW_SAMPL_RATE_KEY] =
					strconv.FormatUint(uint64(*(intf.Config.IngressSamplingRate)), 10)
			}
		}
	}

	res_map[SFLOW_INTF_TBL] = intf_map
	return res_map, err
}

func getSflowInfoFromDb(d *db.DB, tableName string) (Sflow, error) {
	var sfInfo Sflow
	var err error

	sflowEntry, err := d.GetEntry(&db.TableSpec{Name: tableName}, db.Key{Comp: []string{SFLOW_GLOBAL_KEY}})
	if err != nil {
		return sfInfo, err
	}

	sfInfo.Enabled = sflowEntry.Get(SFLOW_ADMIN_KEY)
	sfInfo.SampleSize = sflowEntry.Get(SFLOW_HEADER_BYTES)
	sfInfo.AgentIp = sflowEntry.Get(SFLOW_AGENT_KEY)
	sfInfo.PollingInterval = sflowEntry.Get(SFLOW_POLLING_INTERVAL)

	return sfInfo, err
}

func isV4Address(str string) bool {
	ip := net.ParseIP(str)
	return ip != nil && (ip.To4() != nil)
}

func fillSflowInfo(sflow *ocbinds.OpenconfigSampling_Sampling_Sflow,
	targetUriPath string, d []*db.DB) error {
	var err error

	// No need to nil check sfConfigInfo and sfStateInfo, getSflowInfoFromDb returns an object
	sfConfigInfo, err := getSflowInfoFromDb(d[db.ConfigDB], SFLOW_GLOBAL_TBL)
	if err != nil {
		if !strings.Contains(err.Error(), "Entry does not exist") {
			log.V(lvl.DEBUG).Info("Cant get entry: ", SFLOW_GLOBAL_TBL)
			return err
		}
		err = nil
		log.V(lvl.DEBUG).Info("sFlow not enabled")
	}
	sfStateInfo, err := getSflowInfoFromDb(d[db.ApplStateDB], SFLOW_STATE_TBL)
	if err != nil {
		if !strings.Contains(err.Error(), "Entry does not exist") {
			log.V(lvl.DEBUG).Info("Cant get entry: ", SFLOW_STATE_TBL)
			return nil
		}
		return err
	}

	config := sflow.Config
	state := sflow.State

	configEnabled := false
	config.Enabled = &configEnabled
	if sfConfigInfo.Enabled != "" {
		configEnabled = sfConfigInfo.Enabled == "up"
	}
	stateEnabled := false
	state.Enabled = &stateEnabled
	if sfStateInfo.Enabled != "" {
		stateEnabled = sfStateInfo.Enabled == "up"
	}

	if sfConfigInfo.SampleSize != "" {
		tmp, err := strconv.ParseUint(sfConfigInfo.SampleSize, 10, 16)
		if err != nil {
			log.V(lvl.DEBUG).Info("Failure to convert sfInfo.SampleSize to uint: ", sfConfigInfo.SampleSize)
			return err
		}
		sampleSize := uint16(tmp)
		config.SampleSize = &sampleSize
	}
	if sfStateInfo.SampleSize != "" {
		tmp, err := strconv.ParseUint(sfStateInfo.SampleSize, 10, 16)
		if err != nil {
			log.V(lvl.DEBUG).Info("Failure to convert sfStateInfo.SampleSize to uint: ", sfStateInfo.SampleSize)
			return err
		}
		sampleSize := uint16(tmp)
		state.SampleSize = &sampleSize
	}

	if sfConfigInfo.AgentIp != "" && !isV4Address(sfConfigInfo.AgentIp) {
		config.AgentIdIpv6 = &sfConfigInfo.AgentIp
	}
	if sfStateInfo.AgentIp != "" && !isV4Address(sfStateInfo.AgentIp) {
		state.AgentIdIpv6 = &sfStateInfo.AgentIp
	}

	if sfConfigInfo.PollingInterval != "" {
		tmp, err := strconv.ParseUint(sfConfigInfo.PollingInterval, 10, 16)
		if err != nil {
			log.V(lvl.DEBUG).Info("Failure to convert sfInfo.PollingInterval to uint: ", sfConfigInfo.PollingInterval)
			return err
		}
		pollingInterval := uint16(tmp)
		config.PollingInterval = &pollingInterval
	}
	if sfStateInfo.PollingInterval != "" {
		tmp, err := strconv.ParseUint(sfStateInfo.PollingInterval, 10, 16)
		if err != nil {
			log.V(lvl.DEBUG).Info("Failure to convert sfInfo.PollingInterval to uint: ", sfStateInfo.PollingInterval)
			return err
		}
		pollingInterval := uint16(tmp)
		state.PollingInterval = &pollingInterval
	}

	return err
}

func getSflow(sflow_tr *ocbinds.OpenconfigSampling_Sampling, targetUriPath string,
	uri string, d []*db.DB) error {
	log.V(lvl.DEBUG).Infof("Getting sFlow information")
	var err error

	ygot.BuildEmptyTree(sflow_tr)
	ygot.BuildEmptyTree(sflow_tr.Sflow)
	ygot.BuildEmptyTree(sflow_tr.Sflow.Config)
	ygot.BuildEmptyTree(sflow_tr.Sflow.State)

	switch targetUriPath {
	case SAMPLING_SFLOW:
		ygot.BuildEmptyTree(sflow_tr.Sflow.Collectors)
		ygot.BuildEmptyTree(sflow_tr.Sflow.Interfaces)
		err = fillSflowInfo(sflow_tr.Sflow, targetUriPath, d)
		if err != nil {
			return err
		}
		err = fillSflowCollectorInfo(sflow_tr.Sflow.Collectors, "", targetUriPath, d)
		if err != nil {
			return err
		}
		err = fillSflowInterfaceInfo(sflow_tr.Sflow.Interfaces, "", targetUriPath, d)
		if err != nil {
			return err
		}
	default:
		err = fillSflowInfo(sflow_tr.Sflow, targetUriPath, d)
	}
	return err
}

func getSflowColInfoFromDb(d []*db.DB) (map[string]SflowCol, error) {
	var sfInfo map[string]SflowCol
	var col SflowCol
	var err error

	configDB := d[db.ConfigDB]
	stateDB := d[db.ApplStateDB]
	countersDB := d[db.CountersDB]
	if configDB == nil || stateDB == nil || countersDB == nil {
		log.V(lvl.DEBUG).Info("getSflowColInfoFromDb: configDB or stateDB is nil")
		return sfInfo, tlerr.InvalidArgsError{Format: "uninitialized DB pointer"}
	}

	ckeys, err := configDB.GetKeys(&db.TableSpec{Name: SFLOW_COL_TBL})
	if err != nil {
		return sfInfo, err
	}
	sfInfo = make(map[string]SflowCol)
	for _, key := range ckeys {
		if key.Len() < 1 {
			continue
		}
		name := key.Get(0)
		ip_port := strings.Split(name, "_")
		if len(ip_port) != 2 {
			return sfInfo, tlerr.InternalError{Format: "ConfigDb: Unexpected sflow collector key format(<ip>_<port>): " + name}
		}
		col.Ip = ip_port[0]
		col.Port = ip_port[1]
		sfInfo[name] = col
	}

	// IPv6 keys contain the db.Key separator, so specify the number of expected keys, 1.
	skeys, err := stateDB.GetKeys(&db.TableSpec{Name: SFLOW_COL_TBL, CompCt: 1})
	if err != nil {
		return sfInfo, err
	}
	for _, key := range skeys {
		if key.Len() < 1 {
			continue
		}

		name := key.Get(0)
		if col, ok := sfInfo[name]; ok {
			col.StateIp = col.Ip
			col.StatePort = col.Port
			sfInfo[name] = col
		} else {
			log.V(lvl.DEBUG).Infof("State collector key (%s) found that isn't in config", name)
		}
	}

	cntKeys, err := countersDB.GetKeysByPattern(&db.TableSpec{Name: COUNTERS}, SFLOW_COL_TBL+"*")
	if err != nil {
		return sfInfo, err
	}
	for _, key := range cntKeys {
		if key.Len() < 1 || key.Get(0) != SFLOW_COL_TBL {
			log.V(lvl.DEBUG).Info("No sflow collectors table found")
			continue
		}
		keyLen := key.Len()
		cntName := key.Get(keyLen - 1)
		cntIpPort := strings.Split(cntName, "/")
		if len(cntIpPort) != 2 {
			log.V(lvl.DEBUG).Info("Invalid address/port format")
			continue
		}

		var address string
		if keyLen == 2 {
			// Table with format: SFLOW_COLLECTOR:XXX/<port>
			address = cntIpPort[0]
		} else {
			// Table with format: SFLOW_COLLECTOR:XXX:YYY:ZZZ/<port>
			address = strings.Join(key.Comp[1:keyLen-1], ":") + ":" + cntIpPort[0]
		}
		// Ipv4 example: SFLOW_COLLECTOR:0.0.0.1/6343
		// Ipv6 example: SFLOW_COLLECTOR:2001:0db8:0000:ff00:0042:7879::1/6343
		if validIP := net.ParseIP(address); validIP == nil {
			log.V(lvl.DEBUG).Info("Invalid IP address")
			continue
		}

		name := address + "_" + cntIpPort[1]
		cntEntry, err := countersDB.GetEntry(&db.TableSpec{Name: COUNTERS}, key)
		if err != nil {
			log.V(tlerr.ErrorSeverity(err)).Infof("No counters table found for %v. Err: %v", name, err)
			continue
		}
		if col, ok := sfInfo[name]; ok {
			col.PacketsSent, err = strconv.ParseUint(cntEntry.Get(SFLOW_PKG_SENT), 10, 64)
			if err != nil {
				log.V(lvl.DEBUG).Infof("No Packets-Sent found for %v. Err: %v", name, err)
			}
			sfInfo[name] = col
		} else {
			log.V(lvl.DEBUG).Infof("Counter collector key (%s) found that isn't in appl_state_db", name)
		}
	}
	return sfInfo, err
}

func appendColToYang(sflowCols *ocbinds.OpenconfigSampling_Sampling_Sflow_Collectors,
	ip string, port uint16, sip string, sport uint16, packetsSent uint64) error {
	var err error
	colKey := ocbinds.OpenconfigSampling_Sampling_Sflow_Collectors_Collector_Key{ip, port}

	sfc, found := sflowCols.Collector[colKey]
	if !found {
		sfc, err = sflowCols.NewCollector(ip, port)
		if err != nil {
			log.V(lvl.DEBUG).Infof("Error creating Collector component")
			return err
		}
	}

	sfc.Config = &ocbinds.OpenconfigSampling_Sampling_Sflow_Collectors_Collector_Config{
		Address: &ip,
		Port:    &port,
	}
	sfc.State = &ocbinds.OpenconfigSampling_Sampling_Sflow_Collectors_Collector_State{
		Address:     &sip,
		Port:        &sport,
		PacketsSent: &packetsSent,
	}

	return err
}

func fillSflowCollectorInfo(sflowCols *ocbinds.OpenconfigSampling_Sampling_Sflow_Collectors,
	name string, targetUriPath string, d []*db.DB) error {
	sfInfo, err := getSflowColInfoFromDb(d)
	if err != nil {
		return err
	}

	if name == "" {
		for _, v := range sfInfo {
			if v.Ip == "" {
				log.V(lvl.ERROR).Infof("No collector IP")
				break
			}
			if v.Port == "" {
				v.Port = DEFAULT_COL_PORT
			}
			tmp, err := strconv.ParseUint(v.Port, 10, 16)
			if err != nil {
				return err
			}
			port := uint16(tmp)
			if v.StatePort == "" {
				v.StatePort = DEFAULT_COL_PORT
			}
			stmp, err := strconv.ParseUint(v.StatePort, 10, 16)
			if err != nil {
				return err
			}
			sport := uint16(stmp)
			err = appendColToYang(sflowCols, v.Ip, port, v.StateIp, sport, v.PacketsSent)
			if err != nil {
				return err
			}
		}
		return err
	}

	if v, ok := sfInfo[name]; ok {
		if v.Ip == "" {
			log.V(lvl.ERROR).Infof("No collector IP")
			return err
		}
		if v.Port == "" {
			v.Port = DEFAULT_COL_PORT
		}
		tmp, err := strconv.ParseUint(v.Port, 10, 16)
		if err != nil {
			return err
		}
		port := uint16(tmp)
		if v.StatePort == "" {
			v.StatePort = DEFAULT_COL_PORT
		}
		stmp, err := strconv.ParseUint(v.StatePort, 10, 16)
		if err != nil {
			return err
		}
		sport := uint16(stmp)
		err = appendColToYang(sflowCols, v.Ip, port, v.StateIp, sport, v.PacketsSent)
		return err
	}

	return errors.New("Collector entry not found")
}

func getSflowCol(sflow_tr *ocbinds.OpenconfigSampling_Sampling, targetUriPath string,
	uri string, d []*db.DB) error {
	log.V(lvl.DEBUG).Infof("Getting sFlow collector information")
	ygot.BuildEmptyTree(sflow_tr.Sflow)
	ygot.BuildEmptyTree(sflow_tr.Sflow.Collectors)
	key := makeColKey(uri)
	return fillSflowCollectorInfo(sflow_tr.Sflow.Collectors, key, targetUriPath, d)
}

func getSflowIntfInfosFromDb(d *db.DB, tableName string) (map[string]SflowIntf, error) {
	var sfInfo map[string]SflowIntf
	var intf SflowIntf
	var err error

	sflowIntfTbl, err := d.GetTable(&db.TableSpec{Name: tableName})

	if err != nil {
		return sfInfo, err
	}

	keys, err := sflowIntfTbl.GetKeys()
	if err != nil {
		log.V(lvl.DEBUG).Info("No interface configured, sFlow not enabled")
		return sfInfo, nil
	}

	sfInfo = make(map[string]SflowIntf)
	var name string
	for _, key := range keys {
		intfEntry, err := sflowIntfTbl.GetEntry(key)
		if err != nil {
			return sfInfo, err
		}

		switch key.Len() {
		case 1:
			name = key.Get(0)
			intf.Enabled = intfEntry.Get(SFLOW_ADMIN_KEY)
			intf.Sampling_Rate = intfEntry.Get(SFLOW_SAMPL_RATE_KEY)
			intf.Backoff_Sampling_Rate = intfEntry.Get(SFLOW_BACKOFF_SAMPL_RATE_KEY)
		case 2:
			name = key.Get(1)
			intf.Packets_Sampled, err = strconv.ParseUint(intfEntry.Get(SFLOW_SAMPLE_CNT), 10, 64)
			if err != nil {
				log.V(lvl.DEBUG).Infof("No Packets-Sampled found for %v, err: %v", name, err)
			}
		default:
			log.V(lvl.DEBUG).Info("Not a valid key for sflow")
		}
		sfInfo[name] = intf
	}

	return sfInfo, err
}

func fillSflowInterfaceInfo(sflowIntfs *ocbinds.OpenconfigSampling_Sampling_Sflow_Interfaces,
	name string, targetUriPath string, d []*db.DB) error {
	configInfos, err := getSflowIntfInfosFromDb(d[db.ConfigDB], SFLOW_INTF_TBL)
	if err != nil {
		return err
	}
	stateInfos, err := getSflowIntfInfosFromDb(d[db.ApplStateDB], SFLOW_STATE_INTF_TBL)
	if err != nil {
		return err
	}

	cntInfos, err := getSflowIntfInfosFromDb(d[db.CountersDB], COUNTERS)
	if err != nil {
		return err
	}

	if name == "" {
		for name, cinfo := range configInfos {
			sinfo, ok := stateInfos[name]
			if !ok {
				return errors.New("fillSflowInterfaceInfo: unable to find state db read for " + name)
			}
			tmp, err := strconv.ParseUint(cinfo.Sampling_Rate, 10, 32)
			if err != nil {
				return errors.New("Unable to parse cinfo.Sampling_Rate for " + name)
			}
			samplingRateConfig := uint32(tmp)
			enabledConfig := cinfo.Enabled == "up"
			tmp, err = strconv.ParseUint(sinfo.Sampling_Rate, 10, 32)
			if err != nil {
				return errors.New("Unable to parse sinfo.Sampling_Rate for " + name)
			}
			samplingRateState := uint32(tmp)
			enabledState := sinfo.Enabled == "up"

			backoffTmp, err := strconv.ParseUint(sinfo.Backoff_Sampling_Rate, 10, 32)
			if err != nil {
				log.V(lvl.DEBUG).Infof("Unable to parse sinfo.Backoff_Sampling_Rate for %v, err: %v", name, err)
			}
			backoff := uint32(backoffTmp)
			actualSamplingRate := backoff
			if backoff < samplingRateState {
				actualSamplingRate = samplingRateState
			}

			packetsSampled := uint64(0)
			if cntInfo, ok := cntInfos[name]; ok {
				packetsSampled = cntInfo.Packets_Sampled
			}

			err = appendIntfToYang(sflowIntfs, name, enabledConfig, samplingRateConfig, enabledState, samplingRateState, packetsSampled, actualSamplingRate)
			if err != nil {
				break
			}
		}
		return err
	}

	if cinfo, ok := configInfos[name]; ok {
		sinfo, ok := stateInfos[name]
		if !ok {
			return errors.New("fillSflowInterfaceInfo: unable to find state db read for " + name)
		}

		tmp, _ := strconv.ParseUint(cinfo.Sampling_Rate, 10, 32)
		samplingRateConfig := uint32(tmp)
		enabledConfig := cinfo.Enabled == "up"
		tmp, _ = strconv.ParseUint(sinfo.Sampling_Rate, 10, 32)
		samplingRateState := uint32(tmp)
		enabledState := sinfo.Enabled == "up"

		backoffTmp, err := strconv.ParseUint(sinfo.Backoff_Sampling_Rate, 10, 32)
		if err != nil {
			log.V(lvl.DEBUG).Infof("Unable to parse sinfo.Backoff_Sampling_Rate for %v, err: %v", name, err)
		}
		backoff := uint32(backoffTmp)
		actualSamplingRate := backoff
		if backoff < samplingRateState {
			actualSamplingRate = samplingRateState
		}

		packetsSampled := uint64(0)
		if cntInfo, ok := cntInfos[name]; ok {
			packetsSampled = cntInfo.Packets_Sampled
		}
		err = appendIntfToYang(sflowIntfs, name, enabledConfig, samplingRateConfig, enabledState, samplingRateState, packetsSampled, actualSamplingRate)
		return err
	}

	return errors.New("sFlow Interface entry not found")
}

func appendIntfToYang(sflowIntf *ocbinds.OpenconfigSampling_Sampling_Sflow_Interfaces,
	name string, enabled bool, samplingRate uint32, enabledState bool, samplingRateState uint32,
	packetsSampled uint64, actualSamplingRate uint32) error {
	var err error
	sfc, found := sflowIntf.Interface[name]
	if !found {
		sfc, err = sflowIntf.NewInterface(name)
		if err != nil {
			log.V(lvl.ERROR).Infof("Error creating sFlow Interface")
			return err
		}
	}

	ygot.BuildEmptyTree(sfc)
	ygot.BuildEmptyTree(sfc.Config)
	ygot.BuildEmptyTree(sfc.State)

	sfc.Config.Enabled = &enabled
	sfc.Config.Name = &name
	sfc.Config.IngressSamplingRate = &samplingRate

	sfc.State.Enabled = &enabledState
	sfc.State.Name = &name
	sfc.State.IngressSamplingRate = &samplingRateState
	sfc.State.ActualIngressSamplingRate = &actualSamplingRate
	sfc.State.PacketsSampled = &packetsSampled

	return err
}

func getSflowIntf(sflow_tr *ocbinds.OpenconfigSampling_Sampling, targetUriPath string,
	uri string, d []*db.DB) error {
	log.V(lvl.DEBUG).Infof("Getting sFlow interface information")

	name := NewPathInfo(uri).Var("name")
	ygot.BuildEmptyTree(sflow_tr.Sflow)
	ygot.BuildEmptyTree(sflow_tr.Sflow.Interfaces)
	err := fillSflowInterfaceInfo(sflow_tr.Sflow.Interfaces, name, targetUriPath, d)
	return err
}
