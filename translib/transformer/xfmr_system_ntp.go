package transformer

import (
	"bufio"
	"context"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
	"github.com/redis/go-redis/v9"
)

const (
	NTP_SERVER_TABLE_NAME = "NTP_SERVER"
	NTP_SECRET_PASSWORD   = "asdffdsa"
	NTP_KEY_VALUE_STR     = "value"
	NTP_KEY_ENCRYPTED_STR = "encrypted"
	NTP_KEY_TYPE          = "type"
	NTP_MAX_PLAIN_TXT_LEN = 20
	NTP_MAX_PWD_LEN       = 64
	NTP_DEFAULT_MINPOLL   = 6
	NTP_DEFAULT_MAXPOLL   = 10
)

func init() {
	XlateFuncBind("DbToYang_ntp_server_subtree_xfmr", DbToYang_ntp_server_subtree_xfmr)
	XlateFuncBind("Subscribe_ntp_server_subtree_xfmr", Subscribe_ntp_server_subtree_xfmr)

	// TODO(b/385780256): remove this call once platforms populates this table
	callNtpqAndParse()
}

func getSystemRootObject(inParams XfmrParams) *ocbinds.OpenconfigSystem_System {
	deviceObj := (*inParams.ygRoot).(*ocbinds.Device)
	return deviceObj.System
}

// Find is a function to find if a string is in the string slice
func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func normalizeNtpqRemote(a string) string {
	// Strip leading NTP selection character if it exists
	return strings.TrimLeft(a, "*+#-~")
}

// TODO(b/385780256): delete this code once platforms populates NTP_SERVER
func WriteNtpRemoveServer(rc *redis.Client, ntpqList []string) error {
	var err error
	var errStr string

	log.V(lvl.DEBUG).Infof("WriteNtpRemoveServer: ntpqList %v", ntpqList)
	// There are 10 fields in ntpq server association record
	if len(ntpqList) != 10 {
		errStr = "Failed to analyze NTP server association message"
		log.V(lvl.DEBUG).Info("WriteNtpRemoveServer: ", errStr)
		err = tlerr.InvalidArgsError{Format: errStr}
		return err
	}

	tablekey := strings.Join([]string{NTP_SERVER_TABLE_NAME, normalizeNtpqRemote(ntpqList[0])}, "|")
	boop := rc.HSet(context.Background(), tablekey,
		[]string{
			"refId", ntpqList[1], "stratum", ntpqList[2], "peer_type", ntpqList[3],
			"when", ntpqList[4], "poll", ntpqList[5], "reach", ntpqList[6],
			"delay", ntpqList[7], "offset", ntpqList[8], "jitter", ntpqList[9]})
	if err = boop.Err(); err != nil {
		return err
	}

	return nil
}

// TODO(b/385780256): delete this code once platforms populates NTP_SERVER
func callNtpqAndParse() error {
	log.V(lvl.DEBUG).Infof("callNtpqAndParse called")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/ntpq", "-pnw")
	output, err := cmd.StdoutPipe()
	if err != nil {
		log.V(lvl.DEBUG).Info("callNtpqAndParse: error ", err)
		return err
	}
	if err := cmd.Start(); err != nil {
		log.V(lvl.DEBUG).Info("callNtpqAndParse error  ", err)
		return err
	}
	defer cmd.Wait()

	in := bufio.NewScanner(output)
	//   Sample output
	//     *
	//     *                remote           refid      st t when poll reach   delay   offset  jitter
	//     *          ================================================================================
	//     line1      *10.11.0.1       10.11.8.1        4 u  180  256  377    0.442   -27.516   7.380
	//     line2      +10.11.0.2       10.11.8.1        4 u  174  256  377    0.443    22.323   3.238
	//     line3       2405:200:1410:1401::4:db1
	//     line4                       .INIT.          16 u    -   64    0    0.000    0.000   0.000
	//     *
	//     *  There are two cases on NTP server association outputs with "ntpq -pnw":
	//     *    case1: like the above line1 and line2, all server association information
	//     *           is shown in one line.
	//     *    case2: like the above line3 and line4, server association information
	//     *           is shown in more than one lines due to "remote" or "refid" string is
	//     *           too long.

	line_num := 0
	var list0 []string

	rc := db.TransactionalRedisClient(db.StateDB)
	defer db.CloseRedisClient(rc)

	for in.Scan() {
		line := in.Text()
		list := strings.Fields(line)
		log.V(lvl.DEBUG).Infof("callNtpqAndParse: list %v", list)

		// If cmd returns no NTP peer state, return right away
		if line_num == 0 {
			_, found := Find(list, "remote")
			if !found {
				return nil
			}
		}

		// If peer exists, skip the first 2 lines
		if (line_num == 0) || (line_num == 1) {
			line_num++
			continue
		}

		// Check if it is the above case2 line4, if so, concatenate line3 and line4 in list
		// There are 10 fields in ntpq server association record
		if list0 != nil {
			if len(list) < 10 {
				list = append(list0, list...)
				list0 = nil
			}
		}

		// Check if it is the above case2 line3, if so cache it and continue to next line
		if len(list) < 10 {
			list0 = list
			line_num++
			continue
		}

		if list0 != nil {
			log.V(lvl.DEBUG).Infof("callNtpqAndParse: list0 %v, len %v, line no. %v", list0, len(list0), line_num)
			err = WriteNtpRemoveServer(rc, list0)
			if err != nil {
				log.V(lvl.DEBUG).Infof("callNtpqAndParse: WriteNtpRemoveServer returned %v", err)
				return err
			}
			list0 = nil
		}

		log.V(lvl.DEBUG).Infof("callNtpqAndParse: list %v, len %v, line no. %v", list, len(list), line_num)
		err = WriteNtpRemoveServer(rc, list)
		if err != nil {
			log.V(lvl.DEBUG).Infof("callNtpqAndParse: WriteNtpRemoveServer returned %v", err)
			return err
		}

		line_num++
	}
	return nil
}

func ProcessGetNtpServer(inParams XfmrParams) error {
	requestUriPath, _ := getYangPathFromUri(inParams.requestUri)
	keyName := NewPathInfo(inParams.uri).Var("address")

	log.V(lvl.DEBUG).Info("ProcessGetNtpServer: request ", requestUriPath,
		", key: ", keyName)

	if (requestUriPath != "/openconfig-system:system") &&
		(requestUriPath != "/openconfig-system:system/ntp") &&
		(requestUriPath != "/openconfig-system:system/ntp/servers") &&
		(requestUriPath != "/openconfig-system:system/ntp/servers/server") &&
		(requestUriPath != "/openconfig-system:system/ntp/servers/server/config") &&
		(requestUriPath != "/openconfig-system:system/ntp/servers/server/config/address") &&
		(!strings.HasPrefix(requestUriPath, "/openconfig-system:system/ntp/servers/server/state")) {
		log.V(lvl.DEBUG).Info("ProcessGetNtpServer: no return of ntp server state at ", requestUriPath)
		return nil
	}

	stateDb := inParams.dbs[db.StateDB]
	if stateDb == nil {
		return tlerr.InvalidArgsError{Format: "ProcessGetNtpServer stateDb is nil!"}
	}

	tblSpec := db.TableSpec{Name: NTP_SERVER_TABLE_NAME}
	serversObj := getSystemRootObject(inParams).Ntp.Servers
	ntpServer := serversObj.Server
	if keyName != "" {
		serverObj := ntpServer[keyName]
		if serverObj.State == nil {
			ygot.BuildEmptyTree(serverObj)
		}
		serverObj.State.Address = &keyName

		entry, err := stateDb.GetEntry(&tblSpec, db.Key{Comp: []string{keyName}})
		if err != nil {
			return err
		}
		return populateServer(entry, serverObj)
	} else {
		keys, err := stateDb.GetKeys(&tblSpec)
		if err != nil {
			log.V(lvl.DEBUG).Info("ProcessGetNtpServer, unable to get NTP server table keys with err ", err)
			return err
		}

		for _, key := range keys {
			k := key.Comp[0]
			serverObj := ntpServer[k]
			if serverObj == nil {
				serverObj, _ = serversObj.NewServer(k)
				ygot.BuildEmptyTree(serverObj)
			}

			if serverObj.State == nil {
				ygot.BuildEmptyTree(serverObj)
			}
			serverObj.State.Address = &k

			entry, err := stateDb.GetEntry(&tblSpec, db.Key{Comp: []string{k}})
			if err != nil {
				return err
			}
			populateServer(entry, serverObj)
		}
	}

	return nil
}

func populateServer(entry db.Value, serverObj *ocbinds.OpenconfigSystem_System_Ntp_Servers_Server) error {
	refId := entry.Get("refId")
	stratum := entry.Get("stratum")
	poll := entry.Get("poll")
	reach := entry.Get("reach")
	delay := entry.Get("delay")
	offset := entry.Get("offset")
	jitter := entry.Get("jitter")

	offset_sec, err := strconv.ParseFloat(offset, 64)
	if err == nil {
		offsetNanoNum64 := int64(offset_sec * 1000000000)
		serverObj.State.Offset = &offsetNanoNum64
	}

	poll_num, _ := strconv.ParseUint(poll, 10, 32)
	poll_num32 := uint32(poll_num)
	serverObj.State.PollInterval = &poll_num32

	stratum_num, _ := strconv.ParseUint(stratum, 10, 8)
	stratum_num8 := uint8(stratum_num)
	serverObj.State.Stratum = &stratum_num8

	jitter_sec, err := strconv.ParseFloat(jitter, 64)
	if err == nil {
		jitterNanoNum64 := int64(jitter_sec * 1000000000)
		serverObj.State.RootDispersion = &jitterNanoNum64
	}

	serverObj.State.Reach = &reach

	delaySec, err := strconv.ParseFloat(delay, 64)
	if err == nil {
		delayNano := int64(delaySec * 1000000000)
		serverObj.State.RootDelay = &delayNano
	}

	serverObj.State.Refid = &refId
	return nil
}

// DbToYang_ntp_server_subtree_xfmr is a xfmr function for handling GET NTP server config/state
var DbToYang_ntp_server_subtree_xfmr SubTreeXfmrDbToYang = func(inParams XfmrParams) error {
	log.V(lvl.DEBUG).Info("DbToYang_ntp_server_subtree_xfmr: root ", inParams.ygRoot,
		", uri: ", inParams.uri)
	return ProcessGetNtpServer(inParams)
}

var Subscribe_ntp_server_subtree_xfmr = func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error) {
	var result XfmrSubscOutParams
	result.dbDataMap = make(RedisDbSubscribeMap)

	pathInfo := NewPathInfo(inParams.uri)
	targetUriPath, _ := getYangPathFromUri(pathInfo.Path)
	keyName := pathInfo.Var("address")
	if keyName == "" {
		keyName = "*"
	}

	log.V(lvl.DEBUG).Infof("Subscribe_ntp_server_subtree_xfmr: path=%v key=%v ", targetUriPath, keyName)

	result.dbDataMap = RedisDbSubscribeMap{db.StateDB: {NTP_SERVER_TABLE_NAME: {keyName: {}}}}
	return result, nil
}
