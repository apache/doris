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

package main

import (
	"fmt"

	"github.com/apache/doris/sdk/go-doris-sdk"
)

func main() {
	fmt.Println("Doris SDK - Format Interface Demonstration")

	// ========== Demo 1: Using JSONFormat ==========
	fmt.Println("\n=== Demo 1: JSON Format Configuration ===")

	jsonConfig := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		LabelPrefix: "json_demo",
		// User directly constructs JSONFormat
		Format:      &doris.JSONFormat{Type: doris.JSONObjectLine},
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.ASYNC,
		Options: map[string]string{
			"strict_mode": "true",
		},
	}

	fmt.Printf("JSON Config Format Type: %s\n", jsonConfig.Format.GetFormatType())
	fmt.Printf("JSON Format Options: %+v\n", jsonConfig.Format.GetOptions())

	// ========== Demo 2: Using CSVFormat ==========
	fmt.Println("\n=== Demo 2: CSV Format Configuration ===")

	csvConfig := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		LabelPrefix: "csv_demo",
		// User directly constructs CSVFormat
		Format: &doris.CSVFormat{
			ColumnSeparator: ",",
			LineDelimiter:   "\n",
		},
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.SYNC,
		Options: map[string]string{
			"max_filter_ratio": "0.1",
		},
	}

	fmt.Printf("CSV Config Format Type: %s\n", csvConfig.Format.GetFormatType())
	fmt.Printf("CSV Format Options: %+v\n", csvConfig.Format.GetOptions())

	// ========== Demo 3: Other JSON Formats ==========
	fmt.Println("\n=== Demo 3: JSON Array Format ===")

	jsonArrayConfig := &doris.Config{
		Endpoints: []string{"http://10.16.10.6:8630"},
		User:      "root",
		Password:  "password",
		Database:  "test_db",
		Table:     "test_table",
		// Directly construct JSONFormat - Array type
		Format:      &doris.JSONFormat{Type: doris.JSONArray},
		Retry:       &doris.Retry{MaxRetryTimes: 3, BaseIntervalMs: 2000},
		GroupCommit: doris.OFF,
	}

	fmt.Printf("JSON Array Format Type: %s\n", jsonArrayConfig.Format.GetFormatType())
	fmt.Printf("JSON Array Format Options: %+v\n", jsonArrayConfig.Format.GetOptions())

	// ========== Configuration Validation ==========
	fmt.Println("\n=== Configuration Validation ===")

	configs := []*doris.Config{jsonConfig, csvConfig, jsonArrayConfig}
	configNames := []string{"JSON ObjectLine Config", "CSV Config", "JSON Array Config"}

	for i, config := range configs {
		if err := config.ValidateInternal(); err != nil {
			fmt.Printf("%s validation failed: %v\n", configNames[i], err)
		} else {
			fmt.Printf("%s validation passed!\n", configNames[i])
		}
	}

	fmt.Println("\nDemonstration complete!")
}
