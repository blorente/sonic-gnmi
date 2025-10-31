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

package utils

import (
	"fmt"

	"github.com/Azure/sonic-mgmt-common/cvl"
	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	log "github.com/golang/glog"
)

/* Needed to deserialize FEC info */
const (
	INTF_SEPARATOR                    = "<>"
	LANE_SEPARATOR                    = "|"
	SPEED_SEPARATOR                   = "--"
	FEC_SEPARATOR                     = ","
	INTF_TO_LANE_SEPARATOR            = "@"
	SPEED_TO_FEC_SEPARATOR            = "="
	LANE_TO_SPEED_SEPARATOR           = ":"
	DB_TABLE_NAME_FEC_INFO            = "INTERFACE"
	DB_KEY_NAME_FEC_INFO              = "FEC_INFO"
	DB_FIELD_NAME_DEFAULT_FEC_MODES   = "Intf_Default_Fec_Modes"
	DB_FIELD_NAME_SUPPORTED_FEC_MODES = "Intf_Supported_Fec_Modes"
)

// SortAsPerTblDeps - sort transformer result table list based on dependencies (using CVL API) tables to be used for CRUD operations
func SortAsPerTblDeps(tblLst []string) ([]string, error) {
	var resultTblLst []string
	var err error
	logStr := "Failure in CVL API to sort table list as per dependencies."

	cvSess, cvlRetSess := db.NewValidationSession()
	if cvlRetSess != nil {
		log.Errorf("Failure in creating CVL validation session object required to use CVl API(sort table list as per dependencies) - %v", cvlRetSess)
		err = fmt.Errorf("%v", logStr)
		return resultTblLst, err
	}
	cvlSortDepTblList, cvlRetDepTbl := cvSess.SortDepTables(tblLst)
	if cvlRetDepTbl != cvl.CVL_SUCCESS {
		log.V(lvl.WARNING).Infof("Failure in cvlSess.SortDepTables: %v", cvlRetDepTbl)
		cvl.ValidationSessClose(cvSess)
		err = fmt.Errorf("%v", logStr)
		return resultTblLst, err
	}
	log.V(lvl.DEBUG).Info("cvlSortDepTblList = ", cvlSortDepTblList)
	resultTblLst = cvlSortDepTblList

	cvl.ValidationSessClose(cvSess)
	return resultTblLst, err

}

// RemoveElement - Remove a specific string from a list of strings
func RemoveElement(sl []string, str string) []string {
	for i := 0; i < len(sl); i++ {
		if sl[i] == str {
			sl = append(sl[:i], sl[i+1:]...)
			i--
			break
		}
	}
	return sl
}
