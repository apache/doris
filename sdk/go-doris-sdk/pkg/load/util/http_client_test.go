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

package util

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// createTestClient creates an HTTP client with custom connection limits for testing
func createTestClient(maxIdleConnsPerHost, maxConnsPerHost int) *http.Client {
	transport := &http.Transport{
		MaxIdleConnsPerHost: maxIdleConnsPerHost, // Idle connection pool size, affects connection reuse efficiency
		MaxConnsPerHost:     maxConnsPerHost,     // Maximum concurrent connections, excess will queue
		MaxIdleConns:        50,                  // Global idle connection limit
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

// TestHTTPClientConcurrencyLimits tests the behavior when concurrent requests exceed connection limits
func TestHTTPClientConcurrencyLimits(t *testing.T) {
	// Create a test server that responds after 2 seconds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fixed 2-second delay for each request
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	// Create HTTP client with connection limits
	// MaxIdleConnsPerHost: 2, MaxConnsPerHost: 3
	client := createTestClient(2, 3)

	// Test configuration
	numRequests := 7
	results := make([]requestResult, numRequests)
	var wg sync.WaitGroup

	t.Logf("Testing with MaxIdleConnsPerHost=2, MaxConnsPerHost=3")
	t.Logf("Sending %d concurrent requests to server with 2s response delay", numRequests)
	t.Logf("Expected behavior:")
	t.Logf("- First 3 requests should start immediately (within connection limit)")
	t.Logf("- Remaining 4 requests should queue and wait for connections to be available")
	t.Logf("- First 3 requests should complete around 2s")
	t.Logf("- Next batch should complete around 4s")
	t.Logf("- Final batch should complete around 6s")

	testStartTime := time.Now()

	// Launch concurrent requests
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(requestID int) {
			defer wg.Done()

			startTime := time.Now()
			resp, err := client.Get(server.URL)
			endTime := time.Now()

			duration := endTime.Sub(startTime)
			relativeStartTime := startTime.Sub(testStartTime)
			relativeEndTime := endTime.Sub(testStartTime)

			results[requestID] = requestResult{
				ID:        requestID + 1,
				StartTime: relativeStartTime,
				EndTime:   relativeEndTime,
				Duration:  duration,
				Success:   err == nil && resp != nil,
				Error:     err,
			}

			if resp != nil {
				resp.Body.Close()
			}
		}(i)
	}

	// Wait for all requests to complete
	wg.Wait()

	// Analyze and display results
	t.Logf("\n=== Test Results ===")
	t.Logf("Request | Start Time | End Time   | Duration   | Success")
	t.Logf("--------|------------|------------|------------|--------")

	for _, result := range results {
		status := "OK"
		if !result.Success {
			status = fmt.Sprintf("FAIL: %v", result.Error)
		}

		t.Logf("   %d    | %8s   | %8s   | %8s   | %s",
			result.ID,
			formatDuration(result.StartTime),
			formatDuration(result.EndTime),
			formatDuration(result.Duration),
			status)
	}

	// Analyze connection behavior
	analyzeConnectionBehavior(t, results)
}

type requestResult struct {
	ID        int
	StartTime time.Duration // relative to test start
	EndTime   time.Duration // relative to test start
	Duration  time.Duration // request duration
	Success   bool
	Error     error
}

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func analyzeConnectionBehavior(t *testing.T, results []requestResult) {
	t.Logf("\n=== Connection Behavior Analysis ===")

	// Group requests by completion time ranges
	fastRequests := 0 // Completed within 2.5s (likely immediate connections)
	slowRequests := 0 // Completed after 3.5s (likely queued)

	for _, result := range results {
		if result.Success {
			if result.Duration < 2500*time.Millisecond {
				fastRequests++
			} else if result.Duration > 3500*time.Millisecond {
				slowRequests++
			}
		}
	}

	t.Logf("Fast requests (< 2.5s): %d (expected: 3 - within connection limit)", fastRequests)
	t.Logf("Slow requests (> 3.5s): %d (expected: 4 - queued requests)", slowRequests)

	// Verify expectations
	if fastRequests == 3 && slowRequests == 4 {
		t.Logf("✅ Connection pool behavior matches expectations!")
		t.Logf("   - MaxConnsPerHost=3 allowed 3 requests to proceed immediately")
		t.Logf("   - Remaining 4 requests were queued and waited for connections")
	} else {
		t.Logf("❌ Unexpected connection pool behavior")
		t.Logf("   - Expected 3 fast requests and 4 slow requests")
		t.Logf("   - Got %d fast requests and %d slow requests", fastRequests, slowRequests)
	}

	// Additional insights
	maxDuration := time.Duration(0)
	minDuration := time.Duration(1<<63 - 1) // max duration

	for _, result := range results {
		if result.Success {
			if result.Duration > maxDuration {
				maxDuration = result.Duration
			}
			if result.Duration < minDuration {
				minDuration = result.Duration
			}
		}
	}

	t.Logf("\nDuration range: %s - %s", formatDuration(minDuration), formatDuration(maxDuration))

	if maxDuration > minDuration+1500*time.Millisecond {
		t.Logf("✅ Significant duration difference indicates connection queuing is working")
	} else {
		t.Logf("⚠️  Small duration difference - connection limits might not be effective")
	}
}
