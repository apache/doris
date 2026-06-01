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

package examples

import (
	"fmt"
	"strings"

	doris "github.com/apache/doris/sdk/go-doris-sdk"
)

func GzipExample() {
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:48939"},
		User:        "root",
		Password:    "",
		Database:    "test",
		Table:       "student",
		Format:      doris.DefaultJSONFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.OFF,
		EnableGzip:  true,
	}

	client, err := doris.NewLoadClient(config)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	jsonData := `{"id": 1001, "name": "Alice", "age": 20}
{"id": 1002, "name": "Bob", "age": 22}
{"id": 1003, "name": "Charlie", "age": 19}`

	response, err := client.Load(strings.NewReader(jsonData))
	if err != nil {
		fmt.Printf("Load failed: %v\n", err)
		return
	}

	fmt.Printf("Status: %s\n", response.Status)
	if response.Status == doris.SUCCESS {
		fmt.Printf("Loaded rows: %d\n", response.Resp.NumberLoadedRows)
		fmt.Printf("Load bytes: %d\n", response.Resp.LoadBytes)
	} else {
		fmt.Printf("Message: %s\n", response.Resp.Message)
		fmt.Printf("Error URL: %s\n", response.Resp.ErrorURL)
	}
}
