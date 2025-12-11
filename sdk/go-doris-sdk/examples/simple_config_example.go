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

	"github.com/apache/doris/sdk/go-doris-sdk"
)

func SimpleConfigExample() {
	// Directly construct Config struct using new default functions
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		Format:      doris.DefaultJSONFormat(), // Use new default JSON format
		Retry:       doris.DefaultRetry(),      // Use new default retry strategy
		GroupCommit: doris.ASYNC,
		Options: map[string]string{
			"strict_mode":      "true",
			"max_filter_ratio": "0.1",
		},
	}

	// Create client
	client, err := doris.NewLoadClient(config)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	// Prepare data
	jsonData := `{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}`

	// Execute load
	response, err := client.Load(strings.NewReader(jsonData))
	if err != nil {
		fmt.Printf("Load failed: %v\n", err)
		return
	}

	fmt.Printf("Load completed successfully!\n")
	fmt.Printf("Status: %s\n", response.Status)
	if response.Status == doris.SUCCESS {
		fmt.Printf("Loaded rows: %d\n", response.Resp.NumberLoadedRows)
		fmt.Printf("Load bytes: %d\n", response.Resp.LoadBytes)
	}
}
