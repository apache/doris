// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package load

import (
	jsoniter "github.com/json-iterator/go"
)

type LoadResponse struct {
	Status       LoadStatus
	Resp         RespContent
	ErrorMessage string
}

type LoadStatus int

const (
	FAILURE LoadStatus = iota
	SUCCESS
)

// String returns the string representation of LoadStatus
func (s LoadStatus) String() string {
	switch s {
	case SUCCESS:
		return "SUCCESS"
	case FAILURE:
		return "FAILURE"
	default:
		return "UNKNOWN"
	}
}

// RespContent represents the response from a stream load operation
type RespContent struct {
	TxnID                  int64  `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	TwoPhaseCommit         string `json:"TwoPhaseCommit"`
	ExistingJobStatus      string `json:"ExistingJobStatus"`
	Message                string `json:"Message"`
	NumberTotalRows        int64  `json:"NumberTotalRows"`
	NumberLoadedRows       int64  `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int64  `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	BeginTxnTimeMs         int    `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
	ErrorURL               string `json:"ErrorURL"`
}

// String returns a JSON representation of the response content
func (r *RespContent) String() string {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	bytes, err := json.Marshal(r)
	if err != nil {
		return ""
	}
	return string(bytes)
}
