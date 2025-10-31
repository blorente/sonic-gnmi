////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//  Copyright 2020 Broadcom. The term Broadcom refers to Broadcom Inc. and/or //
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

package transformer

import (
	"errors"
	"strconv"
)

func init() {
	XlateFuncBind("YangToDb_unnumbered_enabled_xfmr", YangToDb_unnumbered_enabled_xfmr)
	XlateFuncBind("DbToYang_unnumbered_enabled_xfmr", DbToYang_unnumbered_enabled_xfmr)
}

// YangToDb_unnumbered_enabled_xfmr is a YangToDB Field transformer for IPv6 unnumbered config "enabled".
var YangToDb_unnumbered_enabled_xfmr FieldXfmrYangToDb = func(inParams XfmrParams) (map[string]string, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, errors.New("YangToDb_unnumbered_enabled_xfmr - Error from getIntfTypeByName: " + err.Error())
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("YangToDb_unnumbered_enabled_xfmr - Invalid Interface type for unnumbered: " + strconv.Itoa(int(intfType)))
	}
	enabled, ok := inParams.param.(*bool)
	if !ok {
		return nil, errors.New("YangToDb_unnumbered_enabled_xfmr, Error: Invalid parameter")
	}
	var enStr string
	if enabled != nil {
		enStr = strconv.FormatBool(*enabled)
	}
	return map[string]string{"unnumbered_enabled": enStr}, nil
}

// DbToYang_unnumbered_enabled_xfmr is a DbToYang Field transformer for IPv6 config "enabled". */
var DbToYang_unnumbered_enabled_xfmr FieldXfmrDbtoYang = func(inParams XfmrParams) (map[string]interface{}, error) {
	pathInfo := NewPathInfo(inParams.uri)
	ifName := pathInfo.Var("name")

	intfType, _, err := getIntfTypeByName(ifName)
	if err != nil {
		return nil, errors.New("DbToYang_unnumbered_enabled_xfmr - Error from getIntfTypeByName: " + err.Error())
	}
	if intfType != IntfTypeEthernet && intfType != IntfTypePortChannel {
		return nil, errors.New("DbToYang_unnumbered_enabled_xfmr - Invalid Interface type for unnumbered: " + strconv.Itoa(int(intfType)))
	}

	intTbl, ok := IntfTypeTblMap[intfType]
	if !ok {
		return nil, errors.New("DbToYang_unnumbered_enabled_xfmr, Error: key not found")
	}
	tblName, err := getIntfTableNameByDBId(intTbl, inParams.curDb)
	if err != nil {
		return nil, err
	}
	prtInst, err := getDBValues(inParams, tblName)
	if err != nil {
		return nil, err
	}

	unnumberedStatus, ok := prtInst.Field["unnumbered_enabled"]
	if !ok {
		return nil, errors.New("unnumbered_enabled field not found in DB table.")
	}
	enStr, err := strconv.ParseBool(unnumberedStatus)
	if err != nil {
		return nil, errors.New("invalid value for unnumbered_enabled field in DB.")
	}
	return map[string]interface{}{"enabled": enStr}, nil
}
