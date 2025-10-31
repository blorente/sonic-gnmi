////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2019 Broadcom. The term Broadcom refers to Broadcom Inc. and/or //
//  its subsidiaries.                                                         //
//                                                                            //
//  Licensed under the Apache License, Version 2.0 (the "License");           //
//  you may not use this file except in compliance with the License.          //
//  You may obtain a copy of the License at                                   //
//                                                                            //
//     http://www.apache.org/licenses/LICENSE-2.0                             //
//                                                                            //
//  Unless required by applicable law or agreed to in writing, software       //
//  distributed under the License is distributed on an "AS IS" BASIS,         //
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  //
//  See the License for the specific language governing permissions and       //
//  limitations under the License.                                            //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

/*
Package translib implements APIs like Create, Get, Subscribe etc.

to be consumed by the north bound management server implementations

This package takes care of translating the incoming requests to

Redis ABNF format and persisting them in the Redis DB.

It can also translate the ABNF format to YANG specific JSON IETF format

This package can also talk to non-DB clients.
*/

package translib

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	"github.com/Workiva/go-datastructures/queue"
	log "github.com/golang/glog"
	"github.com/openconfig/ygot/ygot"
)

// Write lock for all write operations to be synchronized
var writeMutex = &sync.Mutex{}

type ErrSource int

const (
	ProtoErr ErrSource = iota
	AppErr
)

type TranslibFmtType int

const (
	TRANSLIB_FMT_IETF_JSON TranslibFmtType = iota
	TRANSLIB_FMT_YGOT
)

type UserRoles struct {
	Name  string
	Roles []string
}

type SetRequest struct {
	Path             string
	Payload          []byte
	User             UserRoles
	AuthEnabled      bool
	ClientVersion    Version
	DeleteEmptyEntry bool
}

type SetResponse struct {
	ErrSrc ErrSource
	Err    error
}

const (
	QueryParametersContentAll         string = "all"
	QueryParametersContentConfig      string = "config"
	QueryParametersContentNonConfig   string = "nonconfig"
	QueryParametersContentOperational string = "operational"
	QueryParametersContentState       string = "state"
)

type QueryParameters struct {
	Depth   uint     // range 1 to 65535, default is <U+0093>0<U+0094> i.e. all
	Content string   // all, config, non-config(REST)/state(GNMI), operational(GNMI only)
	Fields  []string // list of fields from NBI
}

type GetRequest struct {
	Path          string
	FmtType       TranslibFmtType
	User          UserRoles
	AuthEnabled   bool
	ClientVersion Version
	QueryParams   QueryParameters
	Ctxt          context.Context
}

type GetResponse struct {
	Payload   []byte
	ValueTree ygot.ValidatedGoStruct
	YGSMeta   ygot.YGStructMetaMap // YGSMeta is the metadata map for ValueTree
	ErrSrc    ErrSource
}

type ActionRequest struct {
	Path          string
	Payload       []byte
	User          UserRoles
	AuthEnabled   bool
	ClientVersion Version
}

type ActionResponse struct {
	Payload []byte
	ErrSrc  ErrSource
}

type BulkRequest struct {
	DeleteRequest  []SetRequest
	ReplaceRequest []SetRequest
	UpdateRequest  []SetRequest
	CreateRequest  []SetRequest
	User           UserRoles
	AuthEnabled    bool
	ClientVersion  Version
}

type BulkResponse struct {
	DeleteResponse  []SetResponse
	ReplaceResponse []SetResponse
	UpdateResponse  []SetResponse
	CreateResponse  []SetResponse
}

type ModelData struct {
	Name string
	Org  string
	Ver  string
}

// initializes logging and app modules
func init() {
	log.Flush()
}

// Create - Creates entries in the redis DB pertaining to the path and payload
func Create(req SetRequest) (SetResponse, error) {
	var keys []db.WatchKeys
	var resp SetResponse
	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Create Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Create request received with path =", path)
	log.V(lvl.DEBUG).Info("Create request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, CREATE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	d, err := db.NewDB(getDBOptions(db.ConfigDB, withForceNewRedisConnection, withCacheEnabled))

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	defer d.DeleteDB()

	keys, err = (*app).translateCreate(d)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.StartTx(keys, appInfo.tablesToWatch)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	resp, err = (*app).processCreate(d)

	if err != nil {
		d.AbortTx()
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.CommitTx()

	if err != nil {
		resp.ErrSrc = AppErr
	}

	return resp, err
}

// Update - Updates entries in the redis DB pertaining to the path and payload
func Update(req SetRequest) (SetResponse, error) {
	var keys []db.WatchKeys
	var resp SetResponse
	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Update Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Update request received with path =", path)
	log.V(lvl.DEBUG).Info("Update request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, UPDATE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	d, err := db.NewDB(getDBOptions(db.ConfigDB, withForceNewRedisConnection, withCacheEnabled))

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	defer d.DeleteDB()

	keys, err = (*app).translateUpdate(d)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.StartTx(keys, appInfo.tablesToWatch)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	resp, err = (*app).processUpdate(d)

	if err != nil {
		d.AbortTx()
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.CommitTx()

	if err != nil {
		resp.ErrSrc = AppErr
	}

	return resp, err
}

// Replace - Replaces entries in the redis DB pertaining to the path and payload
func Replace(req SetRequest) (SetResponse, error) {
	var err error
	var keys []db.WatchKeys
	var resp SetResponse
	path := req.Path
	payload := req.Payload
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Replace Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Replace request received with path =", path)
	log.V(lvl.DEBUG).Info("Replace request received with payload =", string(payload))

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	err = appInitialize(app, appInfo, path, &payload, nil, REPLACE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	d, err := db.NewDB(getDBOptions(db.ConfigDB, withForceNewRedisConnection, withCacheEnabled))

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	defer d.DeleteDB()

	keys, err = (*app).translateReplace(d)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.StartTx(keys, appInfo.tablesToWatch)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	resp, err = (*app).processReplace(d)

	if err != nil {
		d.AbortTx()
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.CommitTx()

	if err != nil {
		resp.ErrSrc = AppErr
	}

	return resp, err
}

// Delete - Deletes entries in the redis DB pertaining to the path
func Delete(req SetRequest) (SetResponse, error) {
	var err error
	var keys []db.WatchKeys
	var resp SetResponse
	path := req.Path
	if !isAuthorizedForSet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Delete Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Delete request received with path =", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	opts := appOptions{deleteEmptyEntry: req.DeleteEmptyEntry}
	err = appInitialize(app, appInfo, path, nil, &opts, DELETE)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	d, err := db.NewDB(getDBOptions(db.ConfigDB, withForceNewRedisConnection, withCacheEnabled))

	if err != nil {
		resp.ErrSrc = ProtoErr
		return resp, err
	}

	defer d.DeleteDB()

	keys, err = (*app).translateDelete(d)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.StartTx(keys, appInfo.tablesToWatch)

	if err != nil {
		resp.ErrSrc = AppErr
		return resp, err
	}

	resp, err = (*app).processDelete(d)

	if err != nil {
		d.AbortTx()
		resp.ErrSrc = AppErr
		return resp, err
	}

	err = d.CommitTx()

	if err != nil {
		resp.ErrSrc = AppErr
	}

	return resp, err
}

// Get - Gets data from the redis DB and converts it to northbound format
func Get(req GetRequest) (GetResponse, error) {
	var payload []byte
	var resp GetResponse
	path := req.Path
	if !isAuthorizedForGet(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Get Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Received Get request for path = ", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp = GetResponse{Payload: payload, ErrSrc: ProtoErr}
		return resp, err
	}

	opts := appOptions{depth: req.QueryParams.Depth, content: req.QueryParams.Content, fields: req.QueryParams.Fields, ctxt: req.Ctxt}
	err = appInitialize(app, appInfo, path, nil, &opts, GET)

	if err != nil {
		resp = GetResponse{Payload: payload, ErrSrc: AppErr}
		return resp, err
	}

	dbs, err := getAllDbs(withWriteDisable, withCacheEnabled)

	if err != nil {
		resp = GetResponse{Payload: payload, ErrSrc: ProtoErr}
		return resp, err
	}

	defer closeAllDbs(dbs[:])

	err = (*app).translateGet(dbs)

	if err != nil {
		resp = GetResponse{Payload: payload, ErrSrc: AppErr}
		return resp, err
	}

	resp, err = (*app).processGet(dbs, req.FmtType)

	return resp, err
}

func Action(req ActionRequest) (ActionResponse, error) {
	var payload []byte
	var resp ActionResponse
	path := req.Path

	if !isAuthorizedForAction(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Action Operation",
			Path:   path,
		}
	}

	log.V(lvl.DEBUG).Info("Received Action request for path = ", path)

	app, appInfo, err := getAppModule(path, req.ClientVersion)

	if err != nil {
		resp = ActionResponse{Payload: payload, ErrSrc: ProtoErr}
		return resp, err
	}

	aInfo := *appInfo

	aInfo.isNative = true

	err = appInitialize(app, &aInfo, path, &req.Payload, nil, GET)

	if err != nil {
		resp = ActionResponse{Payload: payload, ErrSrc: AppErr}
		return resp, err
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	dbs, err := getAllDbs(withCacheEnabled, withForceNewRedisConnection)

	if err != nil {
		resp = ActionResponse{Payload: payload, ErrSrc: ProtoErr}
		return resp, err
	}

	defer closeAllDbs(dbs[:])

	err = (*app).translateAction(dbs)

	if err != nil {
		resp = ActionResponse{Payload: payload, ErrSrc: AppErr}
		return resp, err
	}

	resp, err = (*app).processAction(dbs)

	return resp, err
}

func Bulk(req BulkRequest) (BulkResponse, error) {
	var err error
	var keys []db.WatchKeys
	var errSrc ErrSource

	delResp := make([]SetResponse, len(req.DeleteRequest))
	replaceResp := make([]SetResponse, len(req.ReplaceRequest))
	updateResp := make([]SetResponse, len(req.UpdateRequest))
	createResp := make([]SetResponse, len(req.CreateRequest))

	resp := BulkResponse{DeleteResponse: delResp,
		ReplaceResponse: replaceResp,
		UpdateResponse:  updateResp,
		CreateResponse:  createResp}

	if !isAuthorizedForBulk(req) {
		return resp, tlerr.AuthorizationError{
			Format: "User is unauthorized for Action Operation",
		}
	}

	writeMutex.Lock()
	defer writeMutex.Unlock()

	d, err := db.NewDB(getDBOptions(db.ConfigDB, withForceNewRedisConnection, withCacheEnabled))

	if err != nil {
		return resp, err
	}

	defer d.DeleteDB()

	//Start the transaction without any keys or tables to watch will be added later using AppendWatchTx
	err = d.StartTx(nil, nil)

	if err != nil {
		return resp, err
	}

	for i := range req.DeleteRequest {
		path := req.DeleteRequest[i].Path
		opts := appOptions{deleteEmptyEntry: req.DeleteRequest[i].DeleteEmptyEntry}

		log.V(lvl.DEBUG).Info("Delete request received with path =", path)

		app, appInfo, err := getAppModule(path, req.DeleteRequest[i].ClientVersion)

		if err != nil {
			errSrc = ProtoErr
			goto BulkDeleteError
		}

		err = appInitialize(app, appInfo, path, nil, &opts, DELETE)

		if err != nil {
			errSrc = AppErr
			goto BulkDeleteError
		}

		keys, err = (*app).translateDelete(d)

		if err != nil {
			errSrc = AppErr
			goto BulkDeleteError
		}

		err = d.AppendWatchTx(keys, appInfo.tablesToWatch)

		if err != nil {
			errSrc = AppErr
			goto BulkDeleteError
		}

		resp.DeleteResponse[i], err = (*app).processDelete(d)

		if err != nil {
			errSrc = AppErr
		}

	BulkDeleteError:

		if err != nil {
			d.AbortTx()
			resp.DeleteResponse[i].ErrSrc = errSrc
			resp.DeleteResponse[i].Err = err
			return resp, err
		}
	}

	if isExclusivelyCommonAppForReplace := isExclusivelyCommonApp(req.ReplaceRequest); isExclusivelyCommonAppForReplace {
		err = appInitializeBulk(req.ReplaceRequest, resp.ReplaceResponse, d, REPLACE)
		if err != nil {
			d.AbortTx()
			return resp, err
		}
	} else {
		err = processIndividualSetRequests(req.ReplaceRequest, resp.ReplaceResponse, d, REPLACE)
		if err != nil {
			d.AbortTx()
			return resp, err
		}
	}

	if isExclusivelyCommonAppForUpdate := isExclusivelyCommonApp(req.UpdateRequest); isExclusivelyCommonAppForUpdate {
		err = appInitializeBulk(req.UpdateRequest, resp.UpdateResponse, d, UPDATE)
		if err != nil {
			d.AbortTx()
			return resp, err
		}
	} else {
		err = processIndividualSetRequests(req.UpdateRequest, resp.UpdateResponse, d, UPDATE)
		if err != nil {
			d.AbortTx()
			return resp, err
		}
	}

	for i := range req.CreateRequest {
		path := req.CreateRequest[i].Path
		payload := req.CreateRequest[i].Payload

		log.V(lvl.DEBUG).Info("Create request received with path =", path)

		app, appInfo, err := getAppModule(path, req.CreateRequest[i].ClientVersion)

		if err != nil {
			errSrc = ProtoErr
			goto BulkCreateError
		}

		err = appInitialize(app, appInfo, path, &payload, nil, CREATE)

		if err != nil {
			errSrc = AppErr
			goto BulkCreateError
		}

		keys, err = (*app).translateCreate(d)

		if err != nil {
			errSrc = AppErr
			goto BulkCreateError
		}

		err = d.AppendWatchTx(keys, appInfo.tablesToWatch)

		if err != nil {
			errSrc = AppErr
			goto BulkCreateError
		}

		resp.CreateResponse[i], err = (*app).processCreate(d)

		if err != nil {
			errSrc = AppErr
		}

	BulkCreateError:

		if err != nil {
			d.AbortTx()
			resp.CreateResponse[i].ErrSrc = errSrc
			resp.CreateResponse[i].Err = err
			return resp, err
		}
	}

	err = d.CommitTx()

	return resp, err
}

// GetModels - Gets all the models supported by Translib
func GetModels() ([]ModelData, error) {
	var err error

	return getModels(), err
}

// Creates connection will all the redis DBs. To be used for get request
func getAllDbs(opts ...func(*db.Options)) ([db.MaxDB]*db.DB, error) {
	var dbs [db.MaxDB]*db.DB
	var err error
	for dbNum := db.DBNum(0); dbNum < db.MaxDB; dbNum++ {
		if len(dbNum.Name()) == 0 {
			continue
		}
		dbs[dbNum], err = db.NewDB(getDBOptions(dbNum, opts...))
		if err != nil {
			closeAllDbs(dbs[:])
			break
		}
	}

	return dbs, err
}

// Closes the dbs, and nils out the arr.
func closeAllDbs(dbs []*db.DB) {
	for dbsi, d := range dbs {
		if d != nil {
			d.DeleteDB()
			dbs[dbsi] = nil
		}
	}
}

// Compare - Implement Compare method for priority queue for SubscribeResponse struct
func (val SubscribeResponse) Compare(other queue.Item) int {
	o := other.(*SubscribeResponse)
	if val.Timestamp > o.Timestamp {
		return 1
	} else if val.Timestamp == o.Timestamp {
		return 0
	}
	return -1
}

func getDBOptions(dbNo db.DBNum, opts ...func(*db.Options)) db.Options {
	o := db.Options{DBNo: dbNo}
	for _, setopt := range opts {
		setopt(&o)
	}
	switch dbNo {
	case db.ApplDB, db.ApplStateDB, db.CountersDB, db.AsicDB, db.FlexCounterDB, db.LogLevelDB, db.ErrorDB:
		o.TableNameSeparator = ":"
		o.KeySeparator = ":"
	case db.ConfigDB, db.StateDB, db.SnmpDB:
		o.TableNameSeparator = "|"
		o.KeySeparator = "|"
	}
	return o
}

func withWriteDisable(o *db.Options) {
	o.IsWriteDisabled = true
}

func withOnChange(o *db.Options) {
	o.IsOnChangeEnabled = true
}

func withForceNewRedisConnection(o *db.Options) {
	o.ForceNewRedisConnection = true
}

func withCacheEnabled(o *db.Options) {
	o.IsCacheEnabled = true
}

func getAppModule(path string, clientVer Version) (*appInterface, *appInfo, error) {
	var app appInterface

	aInfo, err := getAppModuleInfo(path)

	if err != nil {
		return nil, aInfo, err
	}

	if err := validateClientVersion(clientVer, path, aInfo); err != nil {
		return nil, aInfo, err
	}

	app, err = getAppInterface(aInfo.appType)

	if err != nil {
		return nil, aInfo, err
	}

	return &app, aInfo, err
}

func appInitializeBulk(reqList []SetRequest, respList []SetResponse, d *db.DB, opCode int) error {
	var data appData

	deviceRoot, pathToData, err := buildDeviceRoot(reqList, opCode)
	if err != nil {
		return err
	}

	if deviceRoot == nil || len(pathToData) == 0 {
		return fmt.Errorf("Bulk Set Root Access params are not initialized")
	}

	// appInitializeBulk is only for CommonApp. CommonApp AppInfo is constant
	appInfo := getCommonAppInfo()
	// Fixed input, no err is returned
	app, _ := getAppInterface(appInfo.appType)

	for i := range reqList {
		path := reqList[i].Path
		payload := reqList[i].Payload

		if err := validateClientVersion(reqList[i].ClientVersion, path, appInfo); err != nil {
			respList[i].ErrSrc = ProtoErr
			respList[i].Err = err
			return err
		}

		data = appData{path: path, payload: payload, ygotRoot: deviceRoot, ygotTarget: pathToData[path]}
		(app).initialize(data)

		err = translateAndProcess(&app, appInfo, respList[i], d, opCode)
		if err != nil {
			respList[i].ErrSrc = AppErr
			respList[i].Err = err
			return err
		}
	}

	return nil
}

func buildDeviceRoot(reqList []SetRequest, opCode int) (*ygot.GoStruct, map[string]*interface{}, error) {
	var deviceRoot *ygot.GoStruct
	var pathToData = make(map[string]*interface{})

	// buildDeviceRoot is only for CommonApp. CommonApp AppInfo is constant
	appInfo := getCommonAppInfo()

	for i := range reqList {
		path := reqList[i].Path
		payload := reqList[i].Payload

		log.V(lvl.DEBUG).Info("Bulk request received with path =", path)
		log.V(lvl.DEBUG).Info("Bulk request received with payload =", string(payload))

		// Merge the modules under root
		ygotStruct, ygotTarget, err := getRequestBinder(&path, &payload, opCode, &(appInfo.ygotRootType)).unMarshall()
		if err != nil {
			log.V(lvl.DEBUG).Info("Error in request binding while merging root: ", err)
			return nil, nil, err
		}
		pathToData[path] = ygotTarget

		if moduleNode, ok := (*ygotStruct).(ygot.ValidatedGoStruct); ok {
			if deviceRoot != nil {
				if err := ygot.MergeStructInto((*deviceRoot).(*ocbinds.Device), moduleNode); err != nil {
					log.V(lvl.DEBUG).Info("deviceRoot Merge Error ", err)
					return nil, nil, err
				}
			} else {
				deviceRoot = ygotStruct
			}
		} else {
			err = fmt.Errorf("GetOrCreateNode returned %T; is not a ValidatedGoStruct", *ygotStruct)
			return nil, nil, err
		}
	}

	return deviceRoot, pathToData, nil
}

func translateAndProcess(app *appInterface, appInfo *appInfo, resp SetResponse, d *db.DB, opCode int) error {
	switch opCode {
	case REPLACE:
		keys, err := (*app).translateReplace(d)
		if err != nil {
			return err
		}

		err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
		if err != nil {
			return err
		}

		resp, err = (*app).processReplace(d)
		if err != nil {
			return err
		}
	case UPDATE:
		keys, err := (*app).translateUpdate(d)
		if err != nil {
			return err
		}

		err = d.AppendWatchTx(keys, appInfo.tablesToWatch)
		if err != nil {
			return err
		}

		resp, err = (*app).processUpdate(d)
		if err != nil {
			return err
		}
	default:
		err := fmt.Errorf("Unexpected opCode %d", opCode)
		return err
	}
	return nil
}

func processIndividualSetRequests(reqList []SetRequest, respList []SetResponse, d *db.DB, opCode int) error {
	var errSrc ErrSource
	for i := range reqList {
		path := reqList[i].Path
		payload := reqList[i].Payload

		log.V(lvl.DEBUG).Info("Set request received with path =", path)

		app, appInfo, err := getAppModule(path, reqList[i].ClientVersion)

		if err != nil {
			errSrc = ProtoErr
			goto BulkSetError
		}

		log.V(lvl.DEBUG).Info("Bulk Set request received with path =", path)
		log.V(lvl.DEBUG).Info("Bulk Set request received with payload =", string(payload))

		err = appInitialize(app, appInfo, path, &payload, nil, opCode)
		if err != nil {
			errSrc = AppErr
			goto BulkSetError
		}

		err = translateAndProcess(app, appInfo, respList[i], d, opCode)
		if err != nil {
			errSrc = AppErr
		}

	BulkSetError:

		if err != nil {
			d.AbortTx()
			respList[i].ErrSrc = errSrc
			respList[i].Err = err
			return err
		}
	}
	return nil
}

func isExclusivelyCommonApp(reqList []SetRequest) bool {
	if len(reqList) == 0 {
		return false
	}

	for i := range reqList {

		_, appInfo, err := getAppModule(reqList[i].Path, reqList[i].ClientVersion)
		if err != nil {
			log.V(lvl.DEBUG).Infof("Yang API client is not compatible with this server. Client Version: %v", reqList[i].ClientVersion)
			return false
		}

		if appInfo.isNative || appInfo.appType != reflect.TypeOf(CommonApp{}) {
			return false
		}
	}
	return true
}

func appInitialize(app *appInterface, appInfo *appInfo, path string, payload *[]byte, opts *appOptions, opCode int) error {
	var err error
	var input []byte

	if payload != nil {
		input = *payload
	}

	if appInfo.isNative {
		data := appData{path: path, payload: input}
		data.setOptions(opts)
		(*app).initialize(data)
	} else {
		reqBinder := getRequestBinder(&path, payload, opCode, &(appInfo.ygotRootType))
		ygotStruct, ygotTarget, err := reqBinder.unMarshall()
		if err != nil {
			log.V(lvl.INFO).Info("Error in request binding: ", err)
			return err
		}
		data := appData{path: path, payload: input, ygotRoot: ygotStruct, ygotTarget: ygotTarget, ygSchema: reqBinder.targetNodeSchema}
		data.setOptions(opts)
		(*app).initialize(data)
	}

	return err
}

func (data *appData) setOptions(opts *appOptions) {
	if opts != nil {
		data.appOptions = *opts
	}
}
