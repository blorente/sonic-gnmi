package custom_validation

import (
	"context"
	"strconv"
	"strings"
	"time"

	util "github.com/Azure/sonic-mgmt-common/cvl/internal/util"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	log "github.com/golang/glog"
	"github.com/redis/go-redis/v9"
)

const (
	pollInterval = 100 * time.Millisecond

	// TODO(427507175): Consider lowering after investigation
	lockPollAttempts = 10 * 15 // Up to 15 sec
	ipPollAttempts   = 50
)

// ValidateDpbConfigs Purpose: Check correct for correct agent_id
// vc : Custom Validation Context
// Returns -  CVL Error object
func (t *CustomValidation) ValidateDpbConfigs(
	vc *CustValidationCtxt) CVLErrorInfo {

	log.V(lvl.DEBUG).Info("DpbValidateInterfaceConfigs operation: ", vc.CurCfg.VOp,
		" Key: ", vc.CurCfg.Key, " Data: ", vc.CurCfg.Data,
		" Req Data: ", vc.ReqData)

	log.V(lvl.DEBUG).Info("DpbValidateInterfaceConfigs YNodeVal: ", vc.YNodeVal)

	/* check if input passed is found in ConfigDB PORT|* */
	tableKeys, err := vc.RClient.Keys("PORT|*").Result()

	if (err != nil) || (vc.SessCache == nil) {
		log.V(lvl.ERROR).Info("DpbValidateInterfaceConfigs PORT is empty or invalid argument")
		errStr := "ConfigDB PORT list is empty"
		return CVLErrorInfo{
			ErrCode:          CVL_SEMANTIC_ERROR,
			TableName:        "PORT",
			CVLErrDetails:    errStr,
			ConstraintErrMsg: errStr,
		}
	}
	found := false
	for _, dbKey := range tableKeys {
		tmp := strings.Replace(dbKey, "PORT|", "", 1)
		if tmp == vc.YNodeVal {
			log.V(lvl.DEBUG).Info("DpbValidateInterfaceConfigs dbKey ", tmp)
			found = true
		}
	}
	if !found {
		errStr := "Interface not found"
		return CVLErrorInfo{
			ErrCode:          CVL_SEMANTIC_ERROR,
			TableName:        "PORT",
			CVLErrDetails:    errStr,
			ConstraintErrMsg: errStr,
		}
	}

	return CVLErrorInfo{ErrCode: CVL_SUCCESS}

}

func isPortLocked(appstDBClient *redis.Client, portStateKey string) bool {
	entry, err1 := appstDBClient.HGetAll(context.Background(), portStateKey).Result()
	log.V(lvl.DEBUG).Info("[DPB-CVL] APPL_STATE_DB DPB key ", portStateKey, " Entry: ", entry, " ", err1)
	return (err1 == nil && len(entry) > 0 && len(entry["phase"]) > 0 && entry["phase"] == "pending_delete")
}

func isPortInProgress(db *redis.Client, key string) bool {
	status, err := db.HGet(context.Background(), key, "status").Result()
	return err == nil && status == "InProgress"
}

// ValidateDpbStatus Purpose: Check if DPB is in progress
// vc : Custom Validation Context
// Returns -  CVL Error object
func (t *CustomValidation) ValidateDpbStatus(
	vc *CustValidationCtxt) CVLErrorInfo {

	/* Check STATE_DB if port state of the port s getting deleted is OK */
	rclient := util.NewDbClient("STATE_DB")
	defer func() {
		if rclient != nil {
			rclient.Close()
		}
	}()

	if rclient == nil {
		return CVLErrorInfo{
			ErrCode:          CVL_SEMANTIC_ERROR,
			TableName:        "BREAKOUT_PORTS",
			Keys:             strings.Split(vc.CurCfg.Key, "|"),
			ConstraintErrMsg: "Failed to connect to STATE_DB",
			CVLErrDetails:    "Config Validation Error",
			ErrAppTag:        "capability-unsupported",
		}
	}

	// For ports undergoing DPB, Verify lock status in OA
	// Perform ref count check before config push
	portKey := strings.Replace(vc.CurCfg.Key, "PORT|", "PORT_STATE|", 1)
	log.V(lvl.DEBUG).Info("ValidateDpbStatus: ", vc.CurCfg.VOp,
		" Key: ", vc.CurCfg.Key, " Data: ", vc.CurCfg.Data,
		" Req Data: ", vc.ReqData)
	entry, err := vc.RClient.HGetAll(portKey).Result()
	if err == nil && len(entry) > 0 && len(entry["phase"]) > 0 && entry["phase"] == "pending_delete" {
		/* Check lock status in APPL_STATE_DB */
		appstDBClient := util.NewDbClient("APPL_STATE_DB")
		defer func() {
			if appstDBClient != nil {
				appstDBClient.Close()
			}
		}()
		if appstDBClient == nil {
			return CVLErrorInfo{
				ErrCode:          CVL_SEMANTIC_ERROR,
				TableName:        "PORT_STATE",
				Keys:             strings.Split(vc.CurCfg.Key, ":"),
				ConstraintErrMsg: "Failed to connect to APPL_STATE_DB during lock status check",
				CVLErrDetails:    "Config Validation Error",
				ErrAppTag:        "DB-connection-failure",
			}
		}
		portStateKey := strings.Replace(vc.CurCfg.Key, "PORT|", "PORT_STATE:", 1)
		// Wait for port to be locked by OA.
		lockStatus := false
		for i := 0; i < lockPollAttempts; i++ {
			if isPortLocked(appstDBClient, portStateKey) {
				lockStatus = true
				break
			}
			time.Sleep(pollInterval)
		}
		if !lockStatus {
			log.V(lvl.ERROR).Info("[DPB-CVL] APPL_STATE_DB DPB lock verification failed after timeout: ", portStateKey)
			util.CVL_LEVEL_LOG(util.TRACE_SEMANTIC, "APPL_STATE_DB DPB lock verification failed.")
			return CVLErrorInfo{
				ErrCode:          CVL_SEMANTIC_ERROR,
				TableName:        "PORT_STATE",
				Keys:             strings.Split(vc.CurCfg.Key, ":"),
				ConstraintErrMsg: "Lock verification failed during DPB.",
				CVLErrDetails:    "Config Validation Error",
				ErrAppTag:        "DPB-lock-verification-failed",
			}
		}
		/* Perform ref count check before config push */
		intfEntryCnt := 0
		if entry, err1 := appstDBClient.HGetAll(context.Background(), strings.Replace(vc.CurCfg.Key, "PORT|", "INTF_TABLE:", 1)).Result(); err1 == nil && len(entry) > 0 {
			intfEntryCnt++
		}
		if lagEntryKeys, err1 := appstDBClient.Keys(context.Background(), strings.Replace(vc.CurCfg.Key, "PORT|", "LAG_MEMBER_TABLE:"+"*"+":", 1)).Result(); err1 == nil && len(lagEntryKeys) > 0 {
			intfEntryCnt++
		}
		if entry, err1 := appstDBClient.HGetAll(context.Background(), strings.Replace(vc.CurCfg.Key, "PORT|", "SFLOW_SESSION_TABLE:", 1)).Result(); err1 == nil && len(entry) > 0 {
			intfEntryCnt++
		}
		if qosEntryKeys, err1 := appstDBClient.Keys(context.Background(), strings.Replace(vc.CurCfg.Key, "PORT|", "BUFFER_QUEUE_TABLE:", 1)+":*").Result(); err1 == nil && len(qosEntryKeys) > 0 {
			intfEntryCnt += len(qosEntryKeys)
		}
		refCount := 0
		if entry, err1 := rclient.HGetAll(context.Background(), strings.Replace(vc.CurCfg.Key, "PORT|", "PORT_TABLE|", 1)).Result(); err1 == nil && len(entry) > 0 && len(entry["ref_count"]) > 0 {
			refCount, _ = strconv.Atoi(entry["ref_count"])
		}
		if refCount != intfEntryCnt {
			SetRefCountCheckStatus(true)
			log.V(lvl.DEBUG).Info("[DPB-CVL] refCount: ", refCount, " intfEntryCnt: ", intfEntryCnt)
			util.CVL_LEVEL_LOG(util.TRACE_SEMANTIC, "CVL ref count check failed.")
			return CVLErrorInfo{
				ErrCode:          CVL_SEMANTIC_ERROR,
				TableName:        "PORT_STATE",
				Keys:             strings.Split(vc.CurCfg.Key, ":"),
				ConstraintErrMsg: "CVL ref count check failed during DPB.",
				CVLErrDetails:    "Config Validation Error",
				ErrAppTag:        "DPB-CVL-ref-count-check-failed",
			}
		}
	}

	key := strings.Replace(vc.CurCfg.Key, "PORT|", "BREAKOUT_PORTS|", 1)
	log.V(lvl.DEBUG).Info("ValidateDpbStatus: ", vc.CurCfg.VOp,
		" Key: ", vc.CurCfg.Key, " Data: ", vc.CurCfg.Data,
		" Req Data: ", vc.ReqData)
	entry, err = vc.RClient.HGetAll(key).Result()
	if err == nil && len(entry) > 0 && len(entry["master"]) > 0 {
		key = "PORT_BREAKOUT|" + entry["master"]
	} else {
		return CVLErrorInfo{ErrCode: CVL_SUCCESS}
	}
	log.V(lvl.DEBUG).Info("Master port for ", key, " is ", entry["master"])
	/*
		TODO(b/204461898): Disabling the lanes check as a short term solution,
		since the lanes are coming from DB not from the pushed from
		config.
		_, ok := vc.CurCfg.Data["lanes"]
		if (vc.CurCfg.VOp == OP_CREATE) && (!ok) {
			return CVLErrorInfo{
				ErrCode:          CVL_SEMANTIC_ERROR,
				TableName:        "PORT",
				Keys:             strings.Split(vc.CurCfg.Key, "|"),
				ConstraintErrMsg: "Port does not exist",
				CVLErrDetails:    "Config Validation Error",
				ErrAppTag:        "invalid-port",
			}
		}*/

	// Wait for any Ongoing DPB to complete.
	var i int
	inProgress := true
	start := time.Now()
	for i = 0; i < ipPollAttempts; i++ {
		if !isPortInProgress(rclient, key) {
			inProgress = false
			break
		}
		time.Sleep(pollInterval)
	}
	if i > 0 {
		log.V(lvl.INFO).Infof("[CVL] Waited %v for ongoing DPB to complete on %v", time.Since(start), key)
	}
	if inProgress {
		util.CVL_LEVEL_LOG(util.TRACE_SEMANTIC, "STATE_DB DPB table has entry. DPB in-progress")
		return CVLErrorInfo{
			ErrCode:          CVL_SEMANTIC_ERROR,
			TableName:        "BREAKOUT_CFG",
			Keys:             strings.Split(vc.CurCfg.Key, ":"),
			ConstraintErrMsg: "Port breakout is in progress. Try later.",
			CVLErrDetails:    "Config Validation Error",
			ErrAppTag:        "breakout-in-progress",
		}
	}
	return CVLErrorInfo{ErrCode: CVL_SUCCESS}
}

func (t *CustomValidation) CheckDpbInProgressForPortConfig(vc *CustValidationCtxt) CVLErrorInfo {
	// Skipping DELETE op as DELETE is already taken care during cascade delete
	if vc.CurCfg.VOp == OP_DELETE {
		return CVLErrorInfo{ErrCode: CVL_SUCCESS}
	}

	// For create and update operations, if node is key leaf or non-key leaf,
	// it's value is populated. For leaf-list value may not be populated.
	yangNodeVal := vc.YNodeVal
	yangNodeName := vc.YNodeName
	redisKey := vc.CurCfg.Key
	node := vc.YCur
	rediskeyList := strings.SplitN(redisKey, "|", 2)
	tableName := rediskeyList[0]
	tableKey := rediskeyList[1]

	// Determine if node is a leaf-list node
	var isNodeLeafList bool
	for nodeLeaf := node.FirstChild; nodeLeaf != nil; nodeLeaf = nodeLeaf.NextSibling {
		if yangNodeName != nodeLeaf.Data {
			continue
		}
		if (len(nodeLeaf.Attr) > 0) && (nodeLeaf.Attr[0].Name.Local == "leaf-list") {
			isNodeLeafList = true
		}
	}
	util.TRACE_LEVEL_LOG(util.TRACE_SEMANTIC, "CheckDpbInProgressForPortConfig: DPB check on table: %v|%v for node: %v[%v], isleaflist:%t\n", tableName, tableKey, yangNodeName, yangNodeVal, isNodeLeafList)

	var intfNameToCheck string
	// Determine the interface name on which operation is happening
	if len(yangNodeVal) > 0 {
		intfNameToCheck = yangNodeVal
	} else {
		// If port name from context(vc) is blank, check if it is leaf-list. So get from curCfg Data
		if isNodeLeafList && len(vc.CurCfg.Data) > 0 {
			correctNodeName := yangNodeName
			fldVal, exists := vc.CurCfg.Data[yangNodeName]
			if !exists {
				fldVal, exists = vc.CurCfg.Data[yangNodeName+"@"]
				if exists {
					correctNodeName = yangNodeName + "@"
				}
			}
			util.TRACE_LEVEL_LOG(util.TRACE_SEMANTIC, "CheckDpbInProgressForPortConfig: leaf-list data from Request: %v", fldVal)
			if exists && (len(fldVal) > 0) {
				// On adding or deleting element to leaf-list, always generates UPDATE request
				// and yangNodeVal may be empty. So to determine the correct interface on which
				// operation is going, we need to query all elements of leaf-list from DB and
				// compare with leaf-list received in CurCfg.Data.
				tblData, _ := vc.RClient.HGetAll(redisKey).Result()
				dbNodeVal := tblData[correctNodeName]
				util.TRACE_LEVEL_LOG(util.TRACE_SEMANTIC, "CheckDpbInProgressForPortConfig: leaf-list data from DB: %v", dbNodeVal)

				// Data in DB is not present, means new element getting added
				if len(dbNodeVal) == 0 {
					intfNameToCheck = fldVal
				} else {
					elemFromDb := strings.Split(dbNodeVal, ",")
					elemfromReq := strings.Split(fldVal, ",")
					// Adding interface to leaf-list have entry in request but not in DB
					// Deleting interface from leaf-list have entry in DB but not in request
					// So their difference will provide the interface under operation
					elems := util.GetDifference(elemFromDb, elemfromReq)
					if len(elems) > 0 {
						// Only 1 interface under operation, so assuming that length is 1
						intfNameToCheck = elems[0]
					}
				}
			}
		}
	}
	util.CVL_LEVEL_LOG(util.INFO, "CheckDpbInProgressForPortConfig: operation in progress for interface: %s", intfNameToCheck)

	// Skipping if interface name could not be determined
	if len(intfNameToCheck) == 0 {
		return CVLErrorInfo{ErrCode: CVL_SUCCESS}
	}

	// Some yang nodes have union type and so value can be PortChannel or Vlan also
	// Check if the interface is from PORT table. Otherwise return success
	portTblKey, _ := vc.RClient.Keys("PORT|" + intfNameToCheck).Result()
	if len(portTblKey) == 0 {
		return CVLErrorInfo{ErrCode: CVL_SUCCESS}
	}

	// Get Master port information from BREAKOUT_PORTS table in config Db.
	mpdata, _ := vc.RClient.HGetAll("BREAKOUT_PORTS|" + intfNameToCheck).Result()
	// If entry does not exists in BREAKOUT_PORTS it means breakout didn't happended
	// so consider intfNameToCheck as master port
	masterPortName, exists := mpdata["master"]
	if !exists {
		masterPortName = intfNameToCheck
	}
	util.CVL_LEVEL_LOG(util.INFO, "CheckDpbInProgressForPortConfig: DPB status check for Master port: %s", masterPortName)

	/* Check STATE_DB if any DPB in progress */
	rclient := util.NewDbClient("STATE_DB")
	defer func() {
		if rclient != nil {
			rclient.Close()
		}
	}()

	statusData, dbErr := rclient.HGetAll(context.Background(), "PORT_BREAKOUT|"+masterPortName).Result()
	if dbErr != nil {
		return CVLErrorInfo{
			ErrCode:          CVL_FAILURE,
			TableName:        "PORT_BREAKOUT",
			ConstraintErrMsg: "Failed to retrieve Port breakout status",
			CVLErrDetails:    "Data retrievel Error",
			ErrAppTag:        "dpb-progress-status",
		}
	}

	dpbStatus, exists := statusData["status"]
	if !exists || len(dpbStatus) == 0 {
		// DPB status is not in STATE_DB. No DPB in process
		return CVLErrorInfo{ErrCode: CVL_SUCCESS}
	}
	// if DPB status is InProgress, return error
	if dpbStatus == "InProgress" {
		util.CVL_LEVEL_LOG(util.WARNING, "[DPB-CVL] Operation failed on: %v|%v for node: %v[%v] as breakout of %s in progress\n", tableName, tableKey, yangNodeName, yangNodeVal, masterPortName)
		return CVLErrorInfo{
			ErrCode:          CVL_SEMANTIC_ERROR,
			TableName:        tableName,
			Keys:             strings.Split(tableKey, "|"),
			ConstraintErrMsg: "Breakout of port in progress",
			CVLErrDetails:    "Config Validation Semantic Error",
			ErrAppTag:        "breakout-in-progress",
		}
	}

	return CVLErrorInfo{ErrCode: CVL_SUCCESS}
}
