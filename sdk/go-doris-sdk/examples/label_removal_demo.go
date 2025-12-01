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

// LabelRemovalDemo demonstrates the logging when labels are removed due to group commit
func LabelRemovalDemo() {
	fmt.Println("=== Label Removal Logging Demo ===")

	// Set log level to see warning messages
	doris.SetLogLevel(doris.LogLevelInfo)

	// Demo 1: Custom Label + Group Commit
	fmt.Println("\n--- Demo 1: Custom Label + Group Commit ---")
	configWithLabel := &doris.Config{
		Endpoints:   []string{"http://localhost:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		Label:       "my_custom_label_123", // User-specified custom label
		Format:      doris.DefaultJSONFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.ASYNC, // Enable group commit
	}

	client1, err := doris.NewLoadClient(configWithLabel)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	testData := `{"id": 1, "name": "test"}`
	fmt.Println("Attempting to load data, observe label removal logs...")
	_, err = client1.Load(strings.NewReader(testData))
	if err != nil {
		fmt.Printf("Expected connection error (test environment): %v\n", err)
	}

	// Demo 2: Label Prefix + Group Commit
	fmt.Println("\n--- Demo 2: Label Prefix + Group Commit ---")
	configWithPrefix := &doris.Config{
		Endpoints:   []string{"http://localhost:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		LabelPrefix: "batch_load", // User-specified label prefix
		Format:      doris.DefaultCSVFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.SYNC, // Enable group commit (SYNC mode)
	}

	client2, err := doris.NewLoadClient(configWithPrefix)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	csvData := "1,Alice,30\n2,Bob,25"
	fmt.Println("Attempting to load data, observe label prefix removal logs...")
	_, err = client2.Load(strings.NewReader(csvData))
	if err != nil {
		fmt.Printf("Expected connection error (test environment): %v\n", err)
	}

	// Demo 3: Both Label and LabelPrefix + Group Commit
	fmt.Println("\n--- Demo 3: Label + Label Prefix + Group Commit ---")
	configWithBoth := &doris.Config{
		Endpoints:   []string{"http://localhost:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		Label:       "specific_job_001", // Custom label
		LabelPrefix: "production",       // Label prefix
		Format:      doris.DefaultJSONFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.ASYNC, // Enable group commit
	}

	client3, err := doris.NewLoadClient(configWithBoth)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	jsonData := `{"id": 3, "name": "Charlie"}`
	fmt.Println("Attempting to load data, observe removal logs for both label configurations...")
	_, err = client3.Load(strings.NewReader(jsonData))
	if err != nil {
		fmt.Printf("Expected connection error (test environment): %v\n", err)
	}

	// Demo 4: Normal case without Group Commit
	fmt.Println("\n--- Demo 4: Normal Case (Group Commit OFF) ---")
	configNormal := &doris.Config{
		Endpoints:   []string{"http://localhost:8630"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "test_table",
		Label:       "normal_label_456",
		LabelPrefix: "normal_prefix",
		Format:      doris.DefaultJSONFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.OFF, // Group commit disabled
	}

	client4, err := doris.NewLoadClient(configNormal)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	fmt.Println("Attempting to load data, observe normal label generation logs...")
	_, err = client4.Load(strings.NewReader(testData))
	if err != nil {
		fmt.Printf("Expected connection error (test environment): %v\n", err)
	}

	fmt.Println("\n=== Demo Complete ===")
	fmt.Println("💡 Note: The above demonstrated the label removal logging when group commit is enabled")
	fmt.Println("📋 Log level descriptions:")
	fmt.Println("   - WARN: Warning when user-configured label/label_prefix is removed")
	fmt.Println("   - INFO: Compliance removal operation when group commit is enabled")
	fmt.Println("   - DEBUG: Normal label generation process")
}
