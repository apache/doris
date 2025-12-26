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

// Package examples demonstrates production-level concurrent large-scale data loading
// This example shows how to efficiently load 1 million records using 10 concurrent workers
// Each worker loads 100,000 records independently for maximum throughput
// Best practices: worker pools, progress monitoring, comprehensive error handling
// Uses unified orders schema for consistency across all examples
package examples

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	doris "github.com/apache/doris/sdk/go-doris-sdk"
)

const (
	// Production-level concurrent configuration
	TOTAL_RECORDS      = 1000000                     // 1 million records
	NUM_WORKERS        = 10                          // 10 concurrent workers
	RECORDS_PER_WORKER = TOTAL_RECORDS / NUM_WORKERS // 100k records per worker
)

// WorkerStats holds statistics for each worker
type WorkerStats struct {
	WorkerID      int
	RecordsLoaded int
	DataSize      int64
	LoadTime      time.Duration
	Success       bool
	Error         error
}

// GlobalStats holds overall statistics with atomic operations for thread safety
type GlobalStats struct {
	TotalRecordsProcessed int64
	TotalDataSize         int64
	SuccessfulWorkers     int64
	FailedWorkers         int64
}

// RunConcurrentExample demonstrates production-level concurrent large-scale data loading
func RunConcurrentExample() {
	fmt.Println("=== Production-Level Concurrent Large-Scale Loading Demo ===")

	fmt.Printf("📊 Scale: %d total records, %d workers, %d records per worker\n",
		TOTAL_RECORDS, NUM_WORKERS, RECORDS_PER_WORKER)

	// Production-level configuration optimized for concurrent loads
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "",
		Database:    "test",
		Table:       "orders", // Unified orders table
		LabelPrefix: "prod_concurrent",
		Format:      doris.DefaultCSVFormat(), // Default CSV format
		Retry:       doris.NewRetry(5, 1000),  // 5 retries with 1s base interval
		GroupCommit: doris.ASYNC,              // ASYNC mode for maximum throughput
	}

	// Create shared client (thread-safe)
	client, err := doris.NewLoadClient(config)
	if err != nil {
		fmt.Printf("Failed to create load client: %v\n", err)
		return
	}

	fmt.Println("✅ Load client created successfully")

	// Initialize global statistics and synchronization
	var globalStats GlobalStats
	var wg sync.WaitGroup
	resultChan := make(chan WorkerStats, NUM_WORKERS)
	progressDone := make(chan bool)

	// Start progress monitor
	go printProgressMonitor(progressDone, &globalStats)

	// Record overall start time
	overallStart := time.Now()

	// Launch concurrent workers
	fmt.Printf("🚀 Launching %d concurrent workers...\n", NUM_WORKERS)
	for i := 0; i < NUM_WORKERS; i++ {
		wg.Add(1)
		go loadWorker(i, client, &globalStats, &wg, resultChan)

		// Small delay between worker starts to stagger the load
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all workers to complete
	wg.Wait()
	progressDone <- true
	close(resultChan)

	// Calculate overall metrics
	overallTime := time.Since(overallStart)

	// Collect and analyze results
	var workerResults []WorkerStats
	for stats := range resultChan {
		workerResults = append(workerResults, stats)
	}

	// Simple results summary
	fmt.Println("\n🎉 === CONCURRENT LOAD COMPLETE ===")
	fmt.Printf("📊 Total records processed: %d/%d\n", atomic.LoadInt64(&globalStats.TotalRecordsProcessed), TOTAL_RECORDS)
	fmt.Printf("📈 Workers: %d successful, %d failed\n", atomic.LoadInt64(&globalStats.SuccessfulWorkers), atomic.LoadInt64(&globalStats.FailedWorkers))
	fmt.Printf("⏱️  Total time: %v\n", overallTime)
	fmt.Printf("🚀 Overall rate: %.0f records/sec\n", float64(atomic.LoadInt64(&globalStats.TotalRecordsProcessed))/overallTime.Seconds())
	fmt.Printf("💾 Data processed: %.1f MB\n", float64(atomic.LoadInt64(&globalStats.TotalDataSize))/1024/1024)

	fmt.Println("=== Demo Complete ===")
}

// loadWorker performs the actual data loading for a single worker using unified data generator
func loadWorker(workerID int, client *doris.DorisLoadClient, globalStats *GlobalStats, wg *sync.WaitGroup, resultChan chan<- WorkerStats) {
	defer wg.Done()

	stats := WorkerStats{
		WorkerID: workerID,
		Success:  false,
	}

	fmt.Printf("Starting load operation for %d records\n", RECORDS_PER_WORKER)
	overallStart := time.Now()

	// Generate data for this worker using unified data generator
	genConfig := DataGeneratorConfig{
		WorkerID:    workerID,
		BatchSize:   RECORDS_PER_WORKER,
		ContextName: fmt.Sprintf("DataGen-W%d", workerID),
	}
	data := GenerateOrderCSV(genConfig)
	stats.DataSize = int64(len(data))

	// Perform the load operation
	fmt.Println("Starting load operation...")
	loadStart := time.Now()

	response, err := client.Load(doris.StringReader(data))
	stats.LoadTime = time.Since(loadStart)

	// Simple response handling
	if err != nil {
		stats.Error = err
		fmt.Printf("❌ Worker-%d failed: %v\n", workerID, err)
		atomic.AddInt64(&globalStats.FailedWorkers, 1)
	} else if response != nil && response.Status == doris.SUCCESS {
		stats.Success = true
		stats.RecordsLoaded = RECORDS_PER_WORKER

		// Update global statistics atomically
		atomic.AddInt64(&globalStats.TotalRecordsProcessed, int64(RECORDS_PER_WORKER))
		atomic.AddInt64(&globalStats.TotalDataSize, stats.DataSize)
		atomic.AddInt64(&globalStats.SuccessfulWorkers, 1)

		fmt.Printf("✅ Worker-%d completed: %d records in %v\n", workerID, RECORDS_PER_WORKER, stats.LoadTime)
	} else {
		if response != nil {
			stats.Error = fmt.Errorf("load failed with status: %v", response.Status)
			fmt.Printf("❌ Worker-%d failed with status: %v\n", workerID, response.Status)
		} else {
			stats.Error = fmt.Errorf("load failed: no response received")
			fmt.Printf("❌ Worker-%d failed: no response\n", workerID)
		}
		atomic.AddInt64(&globalStats.FailedWorkers, 1)
	}

	totalTime := time.Since(overallStart)
	fmt.Printf("Worker completed in %v (load: %v)\n", totalTime, stats.LoadTime)

	resultChan <- stats
}

// printProgressMonitor monitors and prints progress during concurrent loading
func printProgressMonitor(done chan bool, globalStats *GlobalStats) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			processed := atomic.LoadInt64(&globalStats.TotalRecordsProcessed)
			successful := atomic.LoadInt64(&globalStats.SuccessfulWorkers)
			failed := atomic.LoadInt64(&globalStats.FailedWorkers)
			progress := float64(processed) / float64(TOTAL_RECORDS) * 100

			fmt.Printf("🔄 Progress: %.1f%% (%d/%d records), Workers: %d success, %d failed\n",
				progress, processed, TOTAL_RECORDS, successful, failed)
		}
	}
}
