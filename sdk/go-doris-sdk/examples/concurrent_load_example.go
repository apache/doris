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

// Package examples demonstrates basic concurrent loading with enhanced logging and thread safety
// This example shows how multiple goroutines can safely share a single DorisLoadClient
// Key features: thread-safe client, enhanced logging with goroutine tracking, proper error handling
// Uses unified orders schema for consistency across all examples
package examples

import (
	"fmt"
	"sync"
	"time"

	doris "github.com/apache/doris/sdk/go-doris-sdk"
)

// workerFunction simulates a worker that loads data concurrently
func workerFunction(workerID int, client *doris.DorisLoadClient, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create context logger for this worker
	workerLogger := doris.NewContextLogger(fmt.Sprintf("Worker-%d", workerID))

	// Generate unique order data for this worker using unified schema
	data := GenerateSimpleOrderCSV(workerID)

	workerLogger.Infof("Starting load operation with %d bytes of order data", len(data))

	// Perform the load operation
	start := time.Now()
	response, err := client.Load(doris.StringReader(data))
	duration := time.Since(start)

	// Simple response handling
	if err != nil {
		fmt.Printf("❌ Worker-%d failed: %v\n", workerID, err)
		return
	}

	if response != nil && response.Status == doris.SUCCESS {
		fmt.Printf("✅ Worker-%d completed in %v\n", workerID, duration)
		if response.Resp.Label != "" {
			fmt.Printf("📋 Worker-%d: Label=%s, Rows=%d\n", workerID, response.Resp.Label, response.Resp.NumberLoadedRows)
		}
	} else {
		if response != nil {
			fmt.Printf("❌ Worker-%d failed with status: %v\n", workerID, response.Status)
		} else {
			fmt.Printf("❌ Worker-%d failed: no response\n", workerID)
		}
	}
}

// RunBasicConcurrentExample demonstrates basic concurrent loading capabilities
func RunBasicConcurrentExample() {
	fmt.Println("=== Basic Concurrent Loading Demo ===")

	// Enhanced logging configuration
	doris.SetLogLevel(doris.LogLevelInfo)

	// We can't directly call log.Infof, so create a context logger first
	logger := doris.NewContextLogger("ConcurrentDemo")
	logger.Infof("Starting concurrent loading demo with enhanced logging")

	// Create configuration using direct struct construction
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "",
		Database:    "test",
		Table:       "orders", // Unified orders table
		LabelPrefix: "demo_concurrent",
		Format:      doris.DefaultCSVFormat(), // Use default CSV format
		Retry:       doris.DefaultRetry(),     // 6 retries: [1s, 2s, 4s, 8s, 16s, 32s] = ~63s total
		GroupCommit: doris.ASYNC,
	}

	// Create client (this is thread-safe and can be shared across goroutines)
	client, err := doris.NewLoadClient(config)
	if err != nil {
		logger.Errorf("Failed to create load client: %v", err)
		return
	}

	logger.Infof("✅ Load client created successfully")

	// Demonstrate concurrent loading with multiple workers
	const numWorkers = 5
	var wg sync.WaitGroup

	fmt.Printf("🚀 Launching %d concurrent workers...\n", numWorkers)

	// Launch workers concurrently
	overallStart := time.Now()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go workerFunction(i, client, &wg)
		// Small delay to show worker launch sequence
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for all workers to complete
	wg.Wait()
	overallDuration := time.Since(overallStart)

	fmt.Printf("🎉 All %d workers completed in %v!\n", numWorkers, overallDuration)
	fmt.Println("=== Demo Complete ===")
}
