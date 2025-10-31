//                                                                            //
//  Copyright 2024 Dell, Inc.                                                 //
//                                                                            //
//  Licensed under the Apache License, Version 2.0 (the "License");           //
//  you may not use this file except in compliance with the License.          //
//  You may obtain a copy of the License at                                   //
//                                                                            //
//  http://www.apache.org/licenses/LICENSE-2.0                                //
//                                                                            //
//  Unless required by applicable law or agreed to in writing, software       //
//  distributed under the License is distributed on an "AS IS" BASIS,         //
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  //
//  See the License for the specific language governing permissions and       //
//  limitations under the License.                                            //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

package transformer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"

	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

func init() {
	XlateFuncBind("YangToDb_lag_min_links_xfmr", YangToDb_lag_min_links_xfmr)
	XlateFuncBind("DbToYang_lag_min_links_xfmr", DbToYang_lag_min_links_xfmr)
	XlateFuncBind("DbToYang_intf_lag_state_xfmr", DbToYang_intf_lag_state_xfmr)
	XlateFuncBind("Subscribe_intf_lag_state_xfmr", Subscribe_intf_lag_state_xfmr)
	XlateFuncBind("DbToYangPath_intf_lag_state_path_xfmr", DbToYangPath_intf_lag_state_path_xfmr)
	XlateFuncBind("YangToDb_lag_type_xfmr", YangToDb_lag_type_xfmr)
	XlateFuncBind("DbToYang_lag_type_xfmr", DbToYang_lag_type_xfmr)
}

const (
	PORTCHANNEL_TABLE             = "PORTCHANNEL"
	DEFAULT_PORTCHANNEL_MIN_LINKS = "1"
	DEFAULT_PORTCHANNEL_SPEED     = "0"
	LAG_TYPE                      = "lag-type"
	PORTCHANNEL_STATE_MEMBER_TN   = "LAG_MEMBER_TABLE"
	PORTCHANNEL_STATE_PORT_TN     = "LAG_TABLE"
)

var LAG_TYPE_MAP = map[string]string{
	strconv.FormatInt(int64(ocbinds.OpenconfigIfAggregate_AggregationType_LACP), 10):   "LACP",
	strconv.FormatInt(int64(ocbinds.OpenconfigIfAggregate_AggregationType_STATIC), 10): "STATIC",
}

/* Validate whether LAG exists in DB */
func validatePortChannel(d *db.DB, lagName string) error {

	intfType, _, ierr := getIntfTypeByName(lagName)
	if ierr != nil || intfType != IntfTypePortChannel {
		return tlerr.InvalidArgsError{Format: "Invalid PortChannel: " + lagName}
	}

	err := validateIntfExists(d, PORTCHANNEL_TABLE, lagName)
	if err != nil {
		errStr := "PortChannel: " + lagName + " does not exist"
		return tlerr.InvalidArgsError{Format: errStr}
	}

	return nil
}

func uint16Conv(sval string) (uint16, error) {
	v, err := strconv.ParseUint(sval, 10, 16)
	if err != nil {
		/* Google: removing this code from upstream, ours is better as it preserves the original error
		errStr := "Conversion of string: " + "sval" + " to int failed"
		if log.V(3) {
			log.Error(errStr)
		}
		return 0, errors.New(errStr)
		*/
		return 0, err
	}
	return uint16(v), nil
}

func deleteLagIntfAndMembers(inParams *XfmrParams, lagName *string) error {
	if lagName == nil || inParams == nil {
		log.V(lvl.ERROR).Infof("deleteLagIntfAndMembers: Invalid args, inParams %v lagName %v", inParams, lagName)
		return tlerr.InvalidArgsError{Format: "inParams or lagName nil"}
	}
	log.V(lvl.DEBUG).Info("Inside deleteLagIntfAndMembers")
	var err error

	subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
	resMap := make(map[string]map[string]db.Value)
	lagMap := make(map[string]db.Value)
	lagMemberMap := make(map[string]db.Value)
	lagIntfMap := make(map[string]db.Value)
	lagMap[*lagName] = db.Value{Field: map[string]string{}}
	emptyDbVal := db.Value{Field: map[string]string{}}

	intTbl := IntfTypeTblMap[IntfTypePortChannel]
	subOpMap[db.ConfigDB] = resMap
	inParams.subOpDataMap[DELETE] = &subOpMap
	/* Validate given PortChannel exists */
	intfType, _, ierr := getIntfTypeByName(*lagName)
	if ierr != nil || intfType != IntfTypePortChannel {
		return tlerr.InvalidArgsError{Format: "Invalid PortChannel: " + *lagName}
	}

	entry, err := inParams.d.GetEntry(&db.TableSpec{Name: PORTCHANNEL_TABLE}, db.Key{Comp: []string{*lagName}})
	if err != nil || !entry.IsPopulated() {
		// Not returning error from here since mgmt infra will return "Resource not found" error in case of non existence entries
		return nil
	}

	/* Validate L3 Configuration only operation is not Delete */
	/* Google: removing this code from upstream since it is dead.
	if inParams.oper != DELETE {
		err = validateL3ConfigExists(inParams.d, lagName)
		if err != nil {
			return err
		}
	}
	*/

	/* Handle PORTCHANNEL_MEMBER TABLE */
	var flag bool = false
	ts := db.TableSpec{Name: intTbl.cfgDb.memberTN + inParams.d.Opts.KeySeparator + *lagName}
	lagKeys, err := inParams.d.GetKeys(&ts)
	if err == nil {
		for key := range lagKeys {
			flag = true
			log.V(lvl.DEBUG).Info("Member port", lagKeys[key].Get(1))
			memberKey := *lagName + "|" + lagKeys[key].Get(1)
			lagMemberMap[memberKey] = db.Value{Field: map[string]string{}}
		}
		if flag {
			resMap["PORTCHANNEL_MEMBER"] = lagMemberMap
		}
	}

	/* Handle PORTCHANNEL_INTERFACE TABLE */
	processIntfTableRemoval(inParams.d, *lagName, PORTCHANNEL_INTERFACE_TN, lagIntfMap)
	if len(lagIntfMap) != 0 {
		resMap[PORTCHANNEL_INTERFACE_TN] = lagIntfMap
	}

	// Handle UMF_TRUNK_QUEUE cleanup
	trunkQueueMap := make(map[string]db.Value)
	ts = db.TableSpec{Name: "UMF_TRUNK_QUEUE" + inParams.d.Opts.KeySeparator + *lagName}
	tqKeys, err := inParams.d.GetKeys(&ts)
	if err == nil && len(tqKeys) > 0 {
		for _, key := range tqKeys {
			trunkQueueKey := strings.Join(key.Comp, inParams.d.Opts.KeySeparator)
			trunkQueueMap[trunkQueueKey] = emptyDbVal
		}
		resMap["UMF_TRUNK_QUEUE"] = trunkQueueMap
	}

	/* Handle PORTCHANNEL TABLE */
	resMap["PORTCHANNEL"] = lagMap
	subOpMap[db.ConfigDB] = resMap
	log.V(lvl.DEBUG).Info("subOpMap: ", subOpMap)
	updateSubOpDataMap(subOpMap, DELETE, *inParams)
	return nil
}

func doGetLagType(d *db.DB, lagName *string, mode *string) (bool, error) {
	intTbl := IntfTypeTblMap[IntfTypePortChannel]
	curr, err := d.GetEntry(&db.TableSpec{Name: intTbl.cfgDb.portTN}, db.Key{Comp: []string{*lagName}})
	found := false
	if err != nil {
		return found, errors.New("Failed to Get PortChannel details")
	}
	if val, ok := curr.Field["lag_type"]; ok {
		*mode = val
		found = true
		log.V(lvl.DEBUG).Infof("Mode from DB: %s\n", *mode)
	} else {
		*mode = "LACP"
		log.V(lvl.DEBUG).Infof("Default LACP Mode: %s\n", *mode)
	}
	return found, nil
}

func getLagStateAttr(attr *string, ifName *string, lagInfoMap map[string]db.Value,
	oc_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Aggregation_State) error {
	lagEntries, ok := lagInfoMap[*ifName]
	if !ok {
		errStr := "Cannot find info for Interface: " + *ifName
		return errors.New(errStr)
	}
	switch *attr {
	case "lag-type":
		oc_val.LagType = ocbinds.OpenconfigIfAggregate_AggregationType_LACP
		lag_type, ok := lagEntries.Field["lag-type"]
		if ok {
			if lag_type == "STATIC" {
				oc_val.LagType = ocbinds.OpenconfigIfAggregate_AggregationType_STATIC
			}
		}
	case "min-links":
		links, _ := strconv.Atoi(lagEntries.Field["min-links"])
		minlinks := uint16(links)
		oc_val.MinLinks = &minlinks
	case "member":
		lagMembers := strings.Split(lagEntries.Field["member@"], ",")
		oc_val.Member = lagMembers
	case "lag-speed":
		lagSpeed, err := strconv.Atoi(lagEntries.Field["lag-speed"])
		if err != nil {
			return err
		}
		uLagSpeed := uint32(lagSpeed)
		oc_val.LagSpeed = &uLagSpeed
	}
	return nil
}

func getLagState(ifName *string, lagInfoMap map[string]db.Value,
	oc_val *ocbinds.OpenconfigInterfaces_Interfaces_Interface_Aggregation_State) error {
	log.V(lvl.DEBUG).Info("getLagState() called")
	for _, attr := range []string{"lag-type", "min-links", "member", "lag-speed"} {
		err := getLagStateAttr(&attr, ifName, lagInfoMap, oc_val)
		if err != nil {
			return err
		}
	}
	return nil
}

/* Get PortChannel Info */
func fillLagInfoForIntf(inParams XfmrParams, ifName *string, lagInfoMap map[string]db.Value) error {
	stateDb := inParams.dbs[db.StateDB]
	lagMemberTS := db.TableSpec{Name: LAG_MEMBER_TABLE_TN + stateDb.Opts.KeySeparator + *ifName}
	/* Get members list */
	lagMemKeys, err := stateDb.GetKeys(&lagMemberTS)
	if err != nil {
		return err
	}
	log.V(lvl.DEBUG).Info("lag-member-table keys", lagMemKeys)
	var lagMembersConfig []string
	for i := range lagMemKeys {
		ethName := string(lagMemKeys[i].Get(1))
		lagMembersConfig = append(lagMembersConfig, ethName)
	}

	/* Calculate lag-speed for each active lagMember */
	var lagSpeed int
	var lagMembers []string
	for _, mem := range lagMembersConfig {
		prtInst, err := stateDb.GetEntry(&lagMemberTS, db.Key{Comp: []string{mem}})
		if err != nil {
			log.V(tlerr.ErrorSeverity(err)).Info("Error retrieving " + LAG_MEMBER_TABLE_TN + stateDb.Opts.KeySeparator + *ifName + " entry for " + mem)
			return err
		}
		if prtInst.Field["runner.selected"] == "false" {
			continue
		}
		lagMembers = append(lagMembers, mem)
		memberSpeed, err := strconv.Atoi(prtInst.Field["link.speed"])
		if err != nil {
			return err
		}
		lagSpeed += memberSpeed
	}
	lagInfoMap[*ifName] = db.Value{Field: map[string]string{"member@": strings.Join(lagMembers, ",")}}
	lagInfoMap[*ifName].Field["lag-speed"] = strconv.Itoa(lagSpeed)

	/* Grab LAG_TABLE*/
	curr, err := stateDb.GetEntry(&db.TableSpec{Name: LAG_TABLE_TN}, db.Key{Comp: []string{*ifName}})
	if err != nil {
		return fmt.Errorf("Failed to retrieve " + LAG_TABLE_TN + " details for " + *ifName)
	}

	/* Get MinLinks value from LAG_TABLE*/
	var links int
	if val, ok := curr.Field["runner.min_ports"]; ok {
		if links, err = strconv.Atoi(val); err != nil {
			return fmt.Errorf("Conversion of %s to int failed: %v", val, err)
		}
	} else {
		log.V(lvl.DEBUG).Info("Minlinks set to 0 (default value)")
		links = 0
	}
	lagInfoMap[*ifName].Field["min-links"] = strconv.Itoa(links)

	/* Get fallback value */
	lagTbl := &db.TableSpec{Name: "LAG_TABLE"}
	applStateDb := inParams.dbs[db.ApplStateDB]
	dbEntry, err := applStateDb.GetEntry(lagTbl, db.Key{Comp: []string{*ifName}})
	if err != nil {
		errStr := "Failed to get PortChannel APPL_STATE_DB entry"
		log.V(lvl.DEBUG).Info(errStr)
		return errors.New(errStr)
	}
	var fallbackVal string
	if val, ok := dbEntry.Field["fallback_operational"]; ok {
		fallbackVal = val
	} else {
		log.V(lvl.DEBUG).Info("Fallback set to False, default value")
		fallbackVal = "false"
	}
	lagInfoMap[*ifName].Field["fallback"] = fallbackVal

	/*Get lag-type from LAG_TABLE*/
	if v, ok := curr.Field["setup.runner_name"]; ok {
		lagInfoMap[*ifName].Field["lag-type"] = v
	} else {
		log.V(lvl.DEBUG).Info("Mode set to LACP, default value")
		lagInfoMap[*ifName].Field["lag-type"] = "LACP"
	}

	log.V(lvl.DEBUG).Infof("Updated the lag-info-map for Interface: %s", *ifName)
	return err
}

// YangToDb_lag_min_links_xfmr is a Yang to DB translation overloaded method for handle min-links config
var YangToDb_lag_min_links_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	if log.V(lvl.DEBUG) {
		log.Info("Entering YangToDb_lag_min_links_xfmr")
	}
	res_map := make(map[string]string)
	var err error

	pathInfo := NewPathInfo(inParams.uri)
	ifKey := pathInfo.Var("name")

	log.V(lvl.DEBUG).Infof("Received Min links config for path: %s; template: %s vars: %v ifKey: %s", pathInfo.Path, pathInfo.Template, pathInfo.Vars, ifKey)

	if inParams.param == nil {
		if log.V(lvl.DEBUG) {
			log.Info("YangToDb_lag_min_links_xfmr Error: No Params")
		}
		return res_map, err
	}

	minLinks, _ := inParams.param.(*uint16)

	if int(*minLinks) > 32 || int(*minLinks) < 0 {
		errStr := "Min links value is invalid for the PortChannel: " + ifKey
		log.Info(errStr)
		err = tlerr.InvalidArgsError{Format: errStr}
		return res_map, err
	}

	res_map["min_links"] = strconv.Itoa(int(*minLinks))
	return res_map, nil
}

var DbToYang_lag_min_links_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	if log.V(3) {
		log.Info("Entering DbToYang_lag_min_links_xfmr")
	}
	var err error
	result := make(map[string]interface{})

	err = validatePortChannel(inParams.d, inParams.key)
	if err != nil {
		log.V(lvl.ERROR).Infof("DbToYang_lag_min_links_xfmr Error: %v ", err)
		return result, err
	}
	data := (*inParams.dbDataMap)[inParams.curDb]
	links, ok := data[PORTCHANNEL_TABLE][inParams.key].Field["min_links"]
	if ok {
		linksUint16, err := uint16Conv(links)
		if err != nil {
			return result, err
		}
		result["min-links"] = linksUint16
	} else {
		if log.V(3) {
			log.Info("min-links set to 0 (default value)")
		}
		linksUint16 := 0
		result["min-links"] = linksUint16
	}

	return result, err
}

// DbToYang_intf_lag_state_xfmr is a DB to Yang translation overloaded method for PortChannel GET operation
var DbToYang_intf_lag_state_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	var err error

	intfsObj := getIntfsRoot(inParams.ygRoot)
	if intfsObj == nil || intfsObj.Interface == nil {
		errStr := "Failed to Get root object!"
		log.V(lvl.ERROR).Infof(errStr)
		return errors.New(errStr)
	}
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")
	if _, ok := intfsObj.Interface[ifName]; !ok {
		obj, _ := intfsObj.NewInterface(ifName)
		ygot.BuildEmptyTree(obj)
	}
	intfObj := intfsObj.Interface[ifName]
	if intfObj.Aggregation == nil {
		ygot.BuildEmptyTree(intfObj)
	}
	if intfObj.Aggregation.State == nil {
		ygot.BuildEmptyTree(intfObj.Aggregation)
	}
	intfType, _, err := getIntfTypeByName(ifName)
	if intfType != IntfTypePortChannel || err != nil {
		intfTypeStr := strconv.Itoa(int(intfType))
		errStr := "TableXfmrFunc - Invalid interface type: " + intfTypeStr
		log.V(lvl.ERROR).Info(errStr)
		return errors.New(errStr)
	}
	/*Validate given PortChannel exists */
	err = validatePortChannel(inParams.d, ifName)
	if err != nil {
		return err
	}

	targetUriPath := pathInfo.YangPath
	log.V(lvl.DEBUG).Info("targetUriPath is ", targetUriPath)
	lagInfoMap := make(map[string]db.Value)
	ocAggregationStateVal := intfObj.Aggregation.State
	err = fillLagInfoForIntf(inParams, &ifName, lagInfoMap)
	if err != nil {
		log.V(lvl.ERROR).Infof("Failed to get info: %s failed!", ifName)
		return err
	}
	log.V(lvl.DEBUG).Info("Succesfully completed DB map population!", lagInfoMap)
	switch targetUriPath {
	case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/min-links":
		log.V(lvl.DEBUG).Info("Get is for min-links")
		attr := "min-links"
		err = getLagStateAttr(&attr, &ifName, lagInfoMap, ocAggregationStateVal)
		if err != nil {
			return err
		}
	case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/lag-type":
		log.V(lvl.DEBUG).Info("Get is for lag type")
		attr := "lag-type"
		err = getLagStateAttr(&attr, &ifName, lagInfoMap, ocAggregationStateVal)
		if err != nil {
			return err
		}
	case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/member":
		log.V(lvl.DEBUG).Info("Get is for member")
		attr := "member"
		err = getLagStateAttr(&attr, &ifName, lagInfoMap, ocAggregationStateVal)
		if err != nil {
			return err
		}
	case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/lag-speed":
		log.V(lvl.DEBUG).Info("Get is for lag-speed")
		attr := "lag-speed"
		if err = getLagStateAttr(&attr, &ifName, lagInfoMap, ocAggregationStateVal); err != nil {
			return err
		}
	case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state":
		log.V(lvl.DEBUG).Info("Get is for State Container!")
		err = getLagState(&ifName, lagInfoMap, ocAggregationStateVal)
		if err != nil {
			return err
		}
	default:
		log.V(lvl.ERROR).Infof(targetUriPath + " - Not an supported Get attribute")
	}
	return err
}

func updateMemberPortsMtu(inParams *XfmrParams, lagName *string, mtuValStr *string) error {
	log.V(lvl.DEBUG).Info("Inside updateLagIntfAndMembersMtu")
	var err error
	resMap := make(map[string]string)
	intPortChannelTbl := IntfTypeTblMap[IntfTypePortChannel]

	/* Validate given PortChannel exits */
	err = validatePortChannel(inParams.d, *lagName)
	if err != nil {
		return err
	}
	ts := db.TableSpec{Name: intPortChannelTbl.cfgDb.memberTN + inParams.d.Opts.KeySeparator + *lagName}
	lagKeys, err := inParams.d.GetKeys(&ts)
	if err == nil {
		subOpMap := make(map[db.DBNum]map[string]map[string]db.Value)
		intfMap := make(map[string]map[string]db.Value)
		intTbl := IntfTypeTblMap[IntfTypeEthernet]
		resMap["mtu"] = *mtuValStr
		intfMap[intTbl.cfgDb.portTN] = make(map[string]db.Value)

		for key := range lagKeys {
			portName := lagKeys[key].Get(1)
			intfMap[intTbl.cfgDb.portTN][portName] = db.Value{Field: resMap}
			log.V(lvl.DEBUG).Info("Member port ", portName, " updated with mtu ", *mtuValStr)
		}

		subOpMap[db.ConfigDB] = intfMap
		inParams.subOpDataMap[UPDATE] = &subOpMap
	}
	return err
}

var Subscribe_intf_lag_state_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var err error
	var result XfmrSubscOutParams

	if inParams.subscProc == TRANSLATE_SUBSCRIBE {

		log.V(lvl.DEBUG).Info("Subscribe_intf_lag_state_xfmr: inParams.subscProc: ", inParams.subscProc)

		pathInfo := NewPathInfo(inParams.requestURI)
		targetUriPath := pathInfo.YangPath

		log.V(lvl.DEBUG).Infof("Subscribe_intf_lag_state_xfmr:- URI:%s pathinfo:%s ", inParams.uri, pathInfo.Path)
		log.V(lvl.DEBUG).Infof("Subscribe_intf_lag_state_xfmr:- Target URI path:%s", targetUriPath)
		defer func() { log.V(lvl.DEBUG).Info("Returning Subscribe_intf_lag_state_xfmr, result:", result) }()

		result.nOpts = new(notificationOpts)
		result.nOpts.pType = OnChange
		result.nOpts.mInterval = 1
		result.onChange = OnchangeEnable
		result.isVirtualTbl = false
		result.needCache = true

		ifName := pathInfo.Var("name")
		log.V(lvl.DEBUG).Info("Subscribe_intf_lag_state_xfmr: ifName: ", ifName)

		// for PORTCHANNEL_MEMBER table
		po_mem_key := "*" + "|" + "*"

		if ifName == "" {
			ifName = "*"
		} else if ifName != "*" {
			po_mem_key = ifName + "|" + "*"
		}

		switch targetUriPath {
		case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/member":
			result.secDbDataMap = RedisDbYgNodeMap{db.StateDB: {PORTCHANNEL_STATE_MEMBER_TN: {ifName + "|*": "member"}}}

		case "/openconfig-interfaces:interfaces/interface/openconfig-if-aggregate:aggregation/state/lag-speed":
			result.secDbDataMap = RedisDbYgNodeMap{db.StateDB: {
				PORTCHANNEL_STATE_MEMBER_TN: {ifName + "|*": map[string]string{"link.speed": "lag-speed"}}}}
		default:
			result.onChange = OnchangeDisable
			result.nOpts.pType = Sample
			result.dbDataMap = make(RedisDbSubscribeMap)
			result.secDbDataMap = RedisDbYgNodeMap{db.ConfigDB: {
				"PORTCHANNEL_MEMBER": {po_mem_key: DBKeyYgNodeInfo{}},
				"PORTCHANNEL":        {ifName: map[string]string{"min_links": "min-links"}}}}
		}

		log.V(lvl.DEBUG).Info("Subscribe_intf_lag_state_xfmr: result ", result)
	}

	return result, err
}

var DbToYangPath_intf_lag_state_path_xfmr PathXfmrDbToYangFunc = func(params XfmrDbToYgPathParams) error {
	intfRoot := "/openconfig-interfaces:interfaces/interface"

	log.V(lvl.DEBUG).Info("Path_intf_lag_state_xfmr: params: ", params)
	defer func() {
		log.V(lvl.DEBUG).Info("DbToYangPath_intf_lag_state_path_xfmr:- params.ygPathKeys: ", params.ygPathKeys)
	}()

	if len(params.tblKeyComp) < 1 {
		return fmt.Errorf("Invalid tblKeyCom for lag path xmfr:%v", params.tblKeyComp)
	}

	switch params.tblName {
	case PORTCHANNEL_TABLE:
		fallthrough
	case "PORTCHANNEL_MEMBER":
		fallthrough
	case PORTCHANNEL_STATE_MEMBER_TN: // Port channel membership change
		params.ygPathKeys[intfRoot+"/name"] = params.tblKeyComp[0]
		// TODO - Upstream does not keyGroup, why do we need to?
		if params.keyGroup != nil {
			*params.keyGroup = append(*params.keyGroup, 0)
		} else {
			params.keyGroup = &[]int{0}
		}

	default:
		return fmt.Errorf("Invalid table name: %s", params.tblName)
	}

	return nil
}

var YangToDb_lag_type_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	result := make(map[string]string)
	var err error

	if inParams.param == nil {
		return result, err
	}

	pathInfo := NewPathInfo(inParams.uri)
	ifKey := pathInfo.Var("name")

	log.V(lvl.DEBUG).Infof("Received Mode configuration for path: %s; template: %s vars: %v ifKey: %s", pathInfo.Path, pathInfo.Template, pathInfo.Vars, ifKey)

	var mode string
	found, err := doGetLagType(inParams.d, &ifKey, &mode)

	t, _ := inParams.param.(ocbinds.E_OpenconfigIfAggregate_AggregationType)
	user_mode := findInMap(LAG_TYPE_MAP, strconv.FormatInt(int64(t), 10))

	if err == nil && found && mode != user_mode {
		errStr := "Cannot configure Mode for an existing PortChannel: " + ifKey
		err = tlerr.InvalidArgsError{Format: errStr}
		return result, err
	}

	log.V(lvl.DEBUG).Info("YangToDb_lag_type_xfmr: ", inParams.ygRoot, " Xpath: ", inParams.uri, " type: ", t)
	result["lag_type"] = user_mode
	return result, nil
}

var DbToYang_lag_type_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	var err error
	result := make(map[string]interface{})

	err = validatePortChannel(inParams.d, inParams.key)
	if err != nil {
		log.V(lvl.ERROR).Infof("DbToYang_lag_type_xfmr Error: %v ", err)
		return result, err
	}

	data := (*inParams.dbDataMap)[inParams.curDb]
	var agg_type ocbinds.E_OpenconfigIfAggregate_AggregationType
	agg_type = ocbinds.OpenconfigIfAggregate_AggregationType_LACP

	lag_type, ok := data[PORTCHANNEL_TABLE][inParams.key].Field["lag_type"]
	if ok {
		if lag_type == "STATIC" {
			agg_type = ocbinds.OpenconfigIfAggregate_AggregationType_STATIC
		}
	}
	result[LAG_TYPE] = ocbinds.E_OpenconfigIfAggregate_AggregationType.ΛMap(agg_type)["E_OpenconfigIfAggregate_AggregationType"][int64(agg_type)].Name
	log.V(lvl.DEBUG).Infof("Lag Type returned from Field Xfmr: %v\n", result)
	return result, err
}
