////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2019 Dell, Inc.                                                 //
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
	"context"
	"sync"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/internal/apis"
	"github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/ygot/ygot"
)

type RedisDbMap = map[db.DBNum]map[string]map[string]db.Value
type RedisDbSubscribeMap = map[db.DBNum]map[string]map[string]map[string]string
type RedisDbYgNodeMap = map[db.DBNum]map[string]map[string]interface{}

// XfmrParams represents input parameters for table-transformer, key-transformer, field-transformer & subtree-transformer
type XfmrParams struct {
	d                    *db.DB
	dbs                  [db.MaxDB]*db.DB
	curDb                db.DBNum
	ygRoot               *ygot.GoStruct
	xpath                string // flattened yang xpath of uri with uri predicates stripped off
	uri                  string
	requestUri           string //original uri using which a curl/NBI request is made
	oper                 Operation
	table                string
	key                  string
	dbDataMap            *map[db.DBNum]map[string]map[string]db.Value
	subOpDataMap         map[Operation]*RedisDbMap // used to add an in-flight data with a sub-op
	param                interface{}
	txCache              *sync.Map
	skipOrdTblChk        *bool
	isVirtualTbl         *bool
	pCascadeDelTbl       *[]string //used to populate list of tables needed cascade delete by subtree overloaded methods
	yangDefValMap        map[string]map[string]db.Value
	queryParams          QueryParams
	pruneDone            *bool
	invokeCRUSubtreeOnce *bool
	ctxt                 context.Context
}

// PublicXfmrParams is structure that is used to initialize fields of XfmrParams outside of the transformer package.
// It is used to write unit tests of transformer functions and has to be in sync with XfmrParams.
type PublicXfmrParams struct {
	D              *db.DB
	Dbs            [db.MaxDB]*db.DB
	CurDb          db.DBNum
	YgRoot         *ygot.GoStruct
	Uri            string
	RequestUri     string //original uri using which a curl/NBI request is made
	Oper           Operation
	Table          string
	Key            string
	DbDataMap      *RedisDbMap
	SubOpDataMap   map[Operation]*RedisDbMap // used to add an in-flight data with a sub-op
	Param          interface{}
	TxCache        *sync.Map
	SkipOrdTblChk  *bool
	IsVirtualTbl   *bool
	PCascadeDelTbl *[]string //used to populate list of tables needed cascade delete by subtree overloaded methods
	YangDefValMap  map[string]map[string]db.Value
}

// D returns a pointer to the `d` field of the XfmrParams structure.
func (p *XfmrParams) D() **db.DB {
	return &p.d
}

// Dbs returns a pointer to the `dbs` field of the XfmrParams structure.
func (p *XfmrParams) Dbs() *[db.MaxDB]*db.DB {
	return &p.dbs
}

// CurDb returns a pointer to the `curDb` field of the XfmrParams structure.
func (p *XfmrParams) CurDb() *db.DBNum {
	return &p.curDb
}

// YgRoot returns a pointer to the `ygRoot` field of the XfmrParams structure.
func (p *XfmrParams) YgRoot() **ygot.GoStruct {
	return &p.ygRoot
}

// Uri returns a pointer to the `uri` field of the XfmrParams structure.
func (p *XfmrParams) Uri() *string {
	return &p.uri
}

// RequestUri returns a pointer to the `requestUri` field of the XfmrParams structure.
func (p *XfmrParams) RequestUri() *string {
	return &p.requestUri
}

// Oper returns a pointer to the `oper` field of the XfmrParams structure.
func (p *XfmrParams) Oper() *Operation {
	return &p.oper
}

// Table returns a pointer to the `table` field of the XfmrParams structure.
func (p *XfmrParams) Table() *string {
	return &p.table
}

// Key returns a pointer to the `key` field of the XfmrParams structure.
func (p *XfmrParams) Key() *string {
	return &p.key
}

// DbDataMap returns a pointer to the `dbDataMap` field of the XfmrParams structure.
func (p *XfmrParams) DbDataMap() **RedisDbMap {
	return &p.dbDataMap
}

// SubOpDataMap returns a pointer to the `subOpDataMap` field of the XfmrParams structure.
func (p *XfmrParams) SubOpDataMap() *map[Operation]*RedisDbMap {
	return &p.subOpDataMap
}

// Param returns a pointer to the `param` field of the XfmrParams structure.
func (p *XfmrParams) Param() *interface{} {
	return &p.param
}

// TxCache returns a pointer to the `txCache` field of the XfmrParams structure.
func (p *XfmrParams) TxCache() **sync.Map {
	return &p.txCache
}

// SkipOrdTblChk returns a pointer to the `skipOrdTblChk` field of the XfmrParams structure.
func (p *XfmrParams) SkipOrdTblChk() **bool {
	return &p.skipOrdTblChk
}

// IsVirtualTbl returns a pointer to the `isVirtualTbl` field of the XfmrParams structure.
func (p *XfmrParams) IsVirtualTbl() **bool {
	return &p.isVirtualTbl
}

// PCascadeDelTbl returns a pointer to the `pCascadeDelTbl` field of the XfmrParams structure.
func (p *XfmrParams) PCascadeDelTbl() **[]string {
	return &p.pCascadeDelTbl
}

// YangDefValMap returns a pointer to the `yangDefValMap` field of the XfmrParams structure.
func (p *XfmrParams) YangDefValMap() *map[string]map[string]db.Value {
	return &p.yangDefValMap
}

// NewXfmrParams returns instance of XfmrParams initialized with values copied from its input parameter.
func NewXfmrParams(params PublicXfmrParams) *XfmrParams {
	return &XfmrParams{
		d:              params.D,
		dbs:            params.Dbs,
		curDb:          params.CurDb,
		ygRoot:         params.YgRoot,
		uri:            params.Uri,
		requestUri:     params.RequestUri,
		oper:           params.Oper,
		table:          params.Table,
		key:            params.Key,
		dbDataMap:      params.DbDataMap,
		subOpDataMap:   params.SubOpDataMap,
		param:          params.Param,
		txCache:        params.TxCache,
		skipOrdTblChk:  params.SkipOrdTblChk,
		isVirtualTbl:   params.IsVirtualTbl,
		pCascadeDelTbl: params.PCascadeDelTbl,
		yangDefValMap:  params.YangDefValMap,
	}
}

// SubscProcType represents subcription process type identifying the type of subscription request made from translib.
type SubscProcType int

const (
	TRANSLATE_EXISTS SubscProcType = iota
	TRANSLATE_SUBSCRIBE
	PROCESS_SUBSCRIBE
)

/*susbcription sampling interval and subscription preference type*/
type notificationOpts struct {
	mInterval int
	pType     NotificationType
}

// XfmrSubscInParams represents input to subscribe subtree callbacks - request uri, DBs info access-pointers, DB info for request uri and subscription process type from translib.
type XfmrSubscInParams struct {
	uri        string
	requestURI string
	dbs        [db.MaxDB]*db.DB
	dbDataMap  RedisDbMap
	subscProc  SubscProcType
}

// XfmrSubscOutParams represents output from subscribe subtree callback - DB data for request uri, Need cache, OnChange, subscription preference and interval.
type XfmrSubscOutParams struct {
	dbDataMap    RedisDbSubscribeMap
	secDbDataMap RedisDbYgNodeMap // for the leaf/leaf-list node if it maps to different table from its parent
	needCache    bool
	onChange     OnchangeMode
	nOpts        *notificationOpts //these can be set regardless of error
	isVirtualTbl bool              //used for RFC parent table check, set to true when no Redis Mapping
}

// DBKeyYgNodeInfo holds yang node info for a db key. Can be used as value in RedisDbYgNodeMap.
type DBKeyYgNodeInfo struct {
	nodeName     string // leaf or leaf-list name
	keyCompCt    int
	keyGroup     []int // db key component indices that make up the "key group" (for leaf-list)
	onChangeFunc apis.ProcessOnChange
}

type OnchangeMode int

const (
	OnchangeDefault OnchangeMode = iota
	OnchangeEnable
	OnchangeDisable
)

// XfmrDbParams represents input paraDeters for value-transformer
type XfmrDbParams struct {
	oper      Operation
	dbNum     db.DBNum
	tableName string
	key       string
	fieldName string
	value     string
}

// SonicXfmrParams represents input parameters for key-transformer on sonic-yang
type SonicXfmrParams struct {
	dbNum     db.DBNum
	tableName string
	key       string
	xpath     string
}

// XfmrDbToYgPathParams represents input parameters for path-transformer
// Fields in the new structs are getting flagged as unused.
//
//lint:file-ignore U1000 temporarily ignore all "unused var" errors.
type XfmrDbToYgPathParams struct {
	yangPath      *gnmi.Path //current path to be be resolved
	subscribePath *gnmi.Path //user input subscribe path
	ygSchemaPath  string     //current yg schema path
	tblName       string     //table name
	tblKeyComp    []string   //table key comp
	tblEntry      *db.Value  // updated or deleted db entry value. DO NOT MODIFY
	dbNum         db.DBNum
	dbs           [db.MaxDB]*db.DB
	db            *db.DB
	ygPathKeys    map[string]string //to keep translated yang keys as values for the each yang key leaf node
	keyGroup      *[]int            // key group comps -- required when db entry is a leaf-list instance
}

// KeyXfmrYangToDb type is defined to use for conversion of Yang key to DB Key,
// Transformer function definition.
// Param: XfmrParams structure having Database info, YgotRoot, operation, Xpath
// Return: Database keys to access db entry, error
type KeyXfmrYangToDb func(inParams XfmrParams) (string, error)

// KeyXfmrDbToYang type is defined to use for conversion of DB key to Yang key,
// Transformer function definition.
// Param: XfmrParams structure having Database info, operation, Database keys to access db entry
// Return: multi dimensional map to hold the yang key attributes of complete xpath, error */
type KeyXfmrDbToYang func(inParams XfmrParams) (map[string]interface{}, error)

// FieldXfmrYangToDb type is defined to use for conversion of yang Field to DB field
// Transformer function definition.
// Param: Database info, YgotRoot, operation, Xpath
// Return: multi dimensional map to hold the DB data, error
type FieldXfmrYangToDb func(inParams XfmrParams) (map[string]string, error)

// FieldXfmrDbtoYang type is defined to use for conversion of DB field to Yang field
// Transformer function definition.
// Param: XfmrParams structure having Database info, operation, DB data in multidimensional map, output param YgotRoot
// Return: error
type FieldXfmrDbtoYang func(inParams XfmrParams) (map[string]interface{}, error)

// SubTreeXfmrYangToDb type is defined to use for handling the yang subtree to DB
// Transformer function definition.
// Param: XfmrParams structure having Database info, YgotRoot, operation, Xpath
// Return: multi dimensional map to hold the DB data, error
type SubTreeXfmrYangToDb func(inParams XfmrParams) (map[string]map[string]db.Value, error)

// SubTreeXfmrDbToYang type is defined to use for handling the DB to Yang subtree
// Transformer function definition.
// Param : XfmrParams structure having Database pointers, current db, operation, DB data in multidimensional map, output param YgotRoot, uri
// Return :  error
type SubTreeXfmrDbToYang func(inParams XfmrParams) error

// SubTreeXfmrSubscribe type is defined to use for handling subscribe(translateSubscribe & processSubscribe) subtree
// Transformer function definition.
// Param : XfmrSubscInParams structure having uri, database pointers,  subcribe process(translate/processSusbscribe), DB data in multidimensional map
// Return :  XfmrSubscOutParams structure (db data in multiD map, needCache, pType, onChange, minInterval), error
type SubTreeXfmrSubscribe func(inParams XfmrSubscInParams) (XfmrSubscOutParams, error)

// ValidateCallpoint is used to validate a YANG node during data translation back to YANG as a response to GET
// Param : XfmrParams structure having Database pointers, current db, operation, DB data in multidimensional map, output param YgotRoot, uri
// Return :  bool
type ValidateCallpoint func(inParams XfmrParams) bool

// RpcCallpoint is used to invoke a callback for action
// Param : []byte input payload, dbi indices
// Return :  []byte output payload, error
type RpcCallpoint func(body []byte, dbs [db.MaxDB]*db.DB) ([]byte, error)

// PostXfmrFunc type is defined to use for handling any default handling operations required as part of the CREATE
// Transformer function definition.
// Param: XfmrParams structure having database pointers, current db, operation, DB data in multidimensional map, YgotRoot, uri
// Return: Multi dimensional map to hold the DB data Map (tblName, key and Fields), error
type PostXfmrFunc func(inParams XfmrParams) (map[string]map[string]db.Value, error)

// TableXfmrFunc type is defined to use for table transformer function for dynamic derviation of redis table.
// Param: XfmrParams structure having database pointers, current db, operation, DB data in multidimensional map, YgotRoot, uri
// Return: List of table names, error
type TableXfmrFunc func(inParams XfmrParams) ([]string, error)

// ValueXfmrFunc type is defined to use for conversion of DB field value from one forma to another
// Transformer function definition.
// Param: XfmrDbParams structure having Database info, operation, db-number, table, key, field, value
// Return: value string, error
type ValueXfmrFunc func(inParams XfmrDbParams) (string, error)

// PreXfmrFunc type is defined to use for handling any default handling operations required as part of the CREATE, UPDATE, REPLACE, DELETE & GET
// Transformer function definition.
// Param: XfmrParams structure having database pointers, current db, operation, DB data in multidimensional map, YgotRoot, uri
// Return: error
type PreXfmrFunc func(inParams XfmrParams) error

// PathXfmrDbToYangFunc type is defined to convert the given db table key into the yang key for all the list node in the given yang URI path.
// ygPathKeys map will be used to store the yang key as value in the map for each yang key leaf node path of the given yang URI.
// Param : XfmrDbToYgPathParams structure has current yang uri path, subscribe path, table name, table key, db pointer slice, current db pointer, db number, map to hold path and yang keys
// Return: error
type PathXfmrDbToYangFunc func(params XfmrDbToYgPathParams) error

// XfmrInterface is a validation interface for validating the callback registration of app modules
// transformer methods.
type XfmrInterface interface {
	xfmrInterfaceValiidate()
}

// SonicKeyXfmrDbToYang type is defined to use for conversion of DB key to Yang key,
// Transformer function definition.
// Param: SonicXfmrParams structure having DB number, table, key and xpath
// Return: multi dimensional map to hold the yang key attributes, error */
type SonicKeyXfmrDbToYang func(inParams SonicXfmrParams) (map[string]interface{}, error)

func (KeyXfmrYangToDb) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for KeyXfmrYangToDb")
}
func (KeyXfmrDbToYang) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for KeyXfmrDbToYang")
}
func (FieldXfmrYangToDb) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for FieldXfmrYangToDb")
}
func (FieldXfmrDbtoYang) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for FieldXfmrDbtoYang")
}
func (SubTreeXfmrYangToDb) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for SubTreeXfmrYangToDb")
}
func (SubTreeXfmrDbToYang) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for SubTreeXfmrDbToYang")
}
func (SubTreeXfmrSubscribe) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for SubTreeXfmrSubscribe")
}
func (TableXfmrFunc) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for TableXfmrFunc")
}
func (SonicKeyXfmrDbToYang) xfmrInterfaceValiidate() {
	xfmrLogInfo("xfmrInterfaceValiidate for SonicKeyXfmrDbToYang")
}
