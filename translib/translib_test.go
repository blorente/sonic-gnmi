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

package translib

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func Test_Create(t *testing.T) {

}

func Test_Bulk_Set_Invalid_Version(t *testing.T) {
	rc := db.RedisClient(db.ConfigDB)
	rc.HSet(context.Background(), "PORT|Ethernet1/1/1", "NULL", "NULL")
	setReqs := []SetRequest{
		SetRequest{
			Path:          "/openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu",
			Payload:       []byte("{\"mtu\": 9105}"),
			ClientVersion: Version{Major: 10, Minor: 10, Patch: 10},
		},
	}

	_, err := Bulk(BulkRequest{ReplaceRequest: setReqs})
	if _, ok := err.(tlerr.TranslibUnsupportedClientVersion); !ok {
		t.Fatalf("Unexpected TranslibUnsupportedClientVersion in Replace")
	}

	_, err = Bulk(BulkRequest{UpdateRequest: setReqs})
	if _, ok := err.(tlerr.TranslibUnsupportedClientVersion); !ok {
		t.Fatalf("Unexpected TranslibUnsupportedClientVersion in Update")
	}
}

func Test_Bulk_Individual_Set(t *testing.T) {
	rc := db.RedisClient(db.ConfigDB)
	rc.HSet(context.Background(), "PORT|Ethernet1/1/1", "NULL", "NULL")
	setReq1 := SetRequest{
		Path:    "/openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/description",
		Payload: []byte("{\"description\": \"This is a description\"}"),
	}
	setReq2 := SetRequest{
		Path:    "/openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu",
		Payload: []byte("{\"mtu\": 9100}"),
	}
	setResp := SetResponse{}

	configDb := getCfgDb()
	defer configDb.DeleteDB()

	if err := configDb.StartTx(nil, nil); err != nil {
		t.Fatalf("Unexpected Config DB Key tx init error")
	}

	setReqs := []SetRequest{setReq1, setReq2}
	setResps := []SetResponse{setResp, setResp}
	if err := processIndividualSetRequests(setReqs, setResps, configDb, REPLACE); err != nil {
		t.Fatalf("Unexpected error in Bulk REPLACE without root access: %v", err)
	}

	if err := processIndividualSetRequests(setReqs, setResps, configDb, UPDATE); err != nil {
		t.Fatalf("Unexpected error in Bulk UPDATE without root access")
	}
}

func Test_AppInitializeBulk_With_Empty_Req_List(t *testing.T) {
	configDb := getCfgDb()
	defer configDb.DeleteDB()
	if err := configDb.StartTx(nil, nil); err != nil {
		t.Fatalf("Unexpected Config DB Key tx init error")
	}

	if err := appInitializeBulk([]SetRequest{}, []SetResponse{}, configDb, REPLACE); err == nil {
		t.Fatalf("Expected error. device root should be nil")
	}
}

func Test_AppInitializeBulk_With_Invalid_Version(t *testing.T) {
	setReq := SetRequest{
		Path:          "/openconfig-interfaces:interfaces/interface[name=Ethernet1/1/1]/config/mtu",
		Payload:       []byte("{\"mtu\": 9105}"),
		ClientVersion: Version{Major: 10, Minor: 10, Patch: 10},
	}

	configDb := getCfgDb()
	defer configDb.DeleteDB()

	if err := configDb.StartTx(nil, nil); err != nil {
		t.Fatalf("Unexpected Config DB Key tx init error")
	}

	if err := appInitializeBulk([]SetRequest{setReq}, []SetResponse{SetResponse{}}, configDb, REPLACE); err == nil {
		t.Fatalf("Expected error")
	}
}

func Test_translateAndProcess_With_Invalid_Oper(t *testing.T) {
	appInfo := getCommonAppInfo()
	app, _ := getAppInterface(appInfo.appType)
	configDb := getCfgDb()
	defer configDb.DeleteDB()
	if err := translateAndProcess(&app, appInfo, SetResponse{}, configDb, 10); err == nil {
		t.Fatalf("Expected error")
	}
}

func getCfgDb() *db.DB {
	configDb, _ := db.NewDB(db.Options{
		DBNo:               db.ConfigDB,
		TableNameSeparator: "|",
		KeySeparator:       "|",
	})

	return configDb
}
