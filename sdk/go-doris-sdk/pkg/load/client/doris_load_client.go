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

// Package client provides the main client interface for Doris Stream Load
package client

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/config"
	loader "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/loader"
	log "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/logger"
)

// Pre-compiled error patterns for efficient matching
var (
	retryableErrorPatterns = []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"timeout",
		"network is unreachable",
		"no such host",
		"temporary failure",
		"dial tcp",
		"i/o timeout",
		"eof",
		"broken pipe",
		"connection aborted",
		"307 temporary redirect",
		"302 found",
		"301 moved permanently",
	}

	retryableResponsePatterns = []string{
		"connect",
		"unavailable",
		"timeout",
		"redirect",
	}

	// Pool for string builders to reduce allocations
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

// DorisLoadClient is the main client interface for loading data into Doris
type DorisLoadClient struct {
	streamLoader *loader.StreamLoader
	config       *config.Config
}

// NewDorisClient creates a new DorisLoadClient instance with the given configuration
func NewDorisClient(cfg *config.Config) (*DorisLoadClient, error) {
	// Validate the configuration
	if err := cfg.ValidateInternal(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &DorisLoadClient{
		streamLoader: loader.NewStreamLoader(),
		config:       cfg,
	}, nil
}

// isRetryableError determines if an error should trigger a retry
// Only network/connection issues should be retried
// Optimized to reduce memory allocations
func isRetryableError(err error, response *loader.LoadResponse) bool {
	if err != nil {
		// Avoid ToLower allocation by checking original error first
		errStr := err.Error()

		// Check net.Error interface first (most efficient)
		if netErr, ok := err.(net.Error); ok {
			if netErr.Timeout() || netErr.Temporary() {
				return true
			}
		}

		// Only convert to lowercase if necessary
		errStrLower := strings.ToLower(errStr)
		for _, pattern := range retryableErrorPatterns {
			if strings.Contains(errStrLower, pattern) {
				return true
			}
		}

		return false
	}

	// If no error but response indicates failure, check if it's a retryable response error
	if response != nil && response.Status == loader.FAILURE && response.ErrorMessage != "" {
		errMsgLower := strings.ToLower(response.ErrorMessage)
		for _, pattern := range retryableResponsePatterns {
			if strings.Contains(errMsgLower, pattern) {
				return true
			}
		}
	}

	return false
}

// calculateBackoffInterval calculates exponential backoff interval with dynamic maximum
// The maximum interval is constrained to ensure total retry time stays within limits
func calculateBackoffInterval(attempt int, baseIntervalMs int64, maxTotalTimeMs int64, currentRetryTimeMs int64) time.Duration {
	if attempt <= 0 {
		return 0
	}

	// Calculate exponential backoff: baseInterval * 2^(attempt-1)
	multiplier := int64(1 << (attempt - 1)) // 2^(attempt-1)
	intervalMs := baseIntervalMs * multiplier

	// If there's a total time limit, constrain the interval dynamically
	if maxTotalTimeMs > 0 {
		remainingTimeMs := maxTotalTimeMs - currentRetryTimeMs
		// Reserve some time for the actual request (estimate ~5 seconds)
		maxAllowedIntervalMs := remainingTimeMs - 5000

		if maxAllowedIntervalMs > 0 && intervalMs > maxAllowedIntervalMs {
			intervalMs = maxAllowedIntervalMs
		}
	}

	// Also apply a reasonable absolute maximum (e.g., 5 minutes) to prevent extreme cases
	const absoluteMaxIntervalMs = 300000 // 5 minutes
	if intervalMs > absoluteMaxIntervalMs {
		intervalMs = absoluteMaxIntervalMs
	}

	// Ensure interval is non-negative
	if intervalMs < 0 {
		intervalMs = 0
	}

	return time.Duration(intervalMs) * time.Millisecond
}

// Load sends data to Doris via HTTP stream load with retry logic
func (c *DorisLoadClient) Load(reader io.Reader) (*loader.LoadResponse, error) {
	operationStartTime := time.Now()

	// Step 1: Configuration preparation
	retry := c.config.Retry
	if retry == nil {
		retry = &config.Retry{MaxRetryTimes: 6, BaseIntervalMs: 1000, MaxTotalTimeMs: 60000}
	}

	maxRetries := retry.MaxRetryTimes
	baseIntervalMs := retry.BaseIntervalMs
	maxTotalTimeMs := retry.MaxTotalTimeMs

	log.Infof("Starting stream load operation")
	log.Infof("Target: %s.%s", c.config.Database, c.config.Table)

	// Show the actual retry strategy to avoid confusion
	if maxRetries > 0 {
		// Calculate and show the actual retry intervals
		var intervals []string
		totalTimeMs := int64(0)
		for i := 1; i <= maxRetries; i++ {
			// Calculate what the interval would be at this point
			simulatedInterval := calculateBackoffInterval(i, baseIntervalMs, maxTotalTimeMs, totalTimeMs)
			intervalMs := simulatedInterval.Milliseconds()
			intervals = append(intervals, fmt.Sprintf("%dms", intervalMs))
			totalTimeMs += intervalMs
		}
		log.Debugf("Retry strategy: exponential backoff with max %d attempts, intervals: [%s], estimated max time: %dms (limit: %dms)",
			maxRetries, strings.Join(intervals, ", "), totalTimeMs, maxTotalTimeMs)
	} else {
		log.Debugf("Retry disabled (maxRetries=0)")
	}

	// Prepare for retries by handling reader consumption
	var getBodyFunc func() (io.Reader, error)

	// Check if reader supports seeking
	if seeker, ok := reader.(io.Seeker); ok {
		// Reader supports seeking, we can reuse it
		getBodyFunc = func() (io.Reader, error) {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("failed to seek to start: %w", err)
			}
			return reader, nil
		}
	} else {
		// Reader doesn't support seeking, buffer the content
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, fmt.Errorf("failed to buffer reader content: %w", err)
		}

		getBodyFunc = func() (io.Reader, error) {
			// Return a copy of the buffer so it's not consumed
			return bytes.NewReader(buf.Bytes()), nil
		}
	}

	var lastErr error
	var response *loader.LoadResponse
	startTime := time.Now()
	totalRetryTime := int64(0)

	// Try the operation with retries
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Infof("Retry attempt %d/%d", attempt, maxRetries)
		} else {
			log.Infof("Initial load attempt")
		}

		// Calculate and apply backoff delay for retries
		if attempt > 0 {
			backoffInterval := calculateBackoffInterval(attempt, baseIntervalMs, maxTotalTimeMs, totalRetryTime)

			// Check if this delay would exceed the total time limit
			if maxTotalTimeMs > 0 && totalRetryTime+backoffInterval.Milliseconds() > maxTotalTimeMs {
				log.Warnf("Next retry delay (%v) would exceed total time limit (%dms). Current total retry time: %dms. Stopping retries.",
					backoffInterval, maxTotalTimeMs, totalRetryTime)
				break
			}

			log.Infof("Waiting %v before retry attempt (total retry time so far: %dms)", backoffInterval, totalRetryTime)
			time.Sleep(backoffInterval)
			totalRetryTime += backoffInterval.Milliseconds()
		}

		// Get a fresh reader for this attempt
		currentReader, err := getBodyFunc()
		if err != nil {
			log.Errorf("Failed to get reader for attempt %d: %v", attempt+1, err)
			lastErr = fmt.Errorf("failed to get reader: %w", err)
			break
		}

		// Create the HTTP request
		req, err := loader.CreateStreamLoadRequest(c.config, currentReader, attempt)
		if err != nil {
			log.Errorf("Failed to create HTTP request: %v", err)
			lastErr = fmt.Errorf("failed to create request: %w", err)
			// Request creation failure is usually not retryable (config issue)
			break
		}

		// Execute the actual load operation
		response, lastErr = c.streamLoader.Load(req)

		// If successful, return immediately
		if lastErr == nil && response != nil && response.Status == loader.SUCCESS {
			log.Infof("Stream load operation completed successfully on attempt %d", attempt+1)
			return response, nil
		}

		// Check if this error/response should be retried
		shouldRetry := isRetryableError(lastErr, response)

		if lastErr != nil {
			log.Errorf("Attempt %d failed with error: %v (retryable: %t)", attempt+1, lastErr, shouldRetry)
		} else if response != nil && response.Status == loader.FAILURE {
			log.Errorf("Attempt %d failed with status: %s (retryable: %t)", attempt+1, response.Resp.Status, shouldRetry)
			if response.ErrorMessage != "" {
				log.Errorf("Error details: %s", response.ErrorMessage)
			}
		}

		// Early exit for non-retryable errors
		if !shouldRetry {
			log.Warnf("Error is not retryable, stopping retry attempts")
			break
		}

		// If this is the last attempt, don't continue
		if attempt == maxRetries {
			log.Warnf("Reached maximum retry attempts (%d), stopping", maxRetries)
			break
		}

		// Check total elapsed time (including processing time, not just retry delays)
		elapsedTime := time.Since(startTime)
		if maxTotalTimeMs > 0 && elapsedTime.Milliseconds() > maxTotalTimeMs {
			log.Warnf("Total operation time (%v) exceeded limit (%dms), stopping retries", elapsedTime, maxTotalTimeMs)
			break
		}
	}

	// Final result logging
	totalOperationTime := time.Since(operationStartTime)
	log.Debugf("[TIMING] Total operation time: %v", totalOperationTime)

	if lastErr != nil {
		log.Errorf("Stream load operation failed after %d attempts: %v", maxRetries+1, lastErr)
		return response, lastErr
	}

	if response != nil {
		log.Errorf("Stream load operation failed with final status: %v", response.Status)
		return response, fmt.Errorf("load failed with status: %v", response.Status)
	}

	log.Errorf("Stream load operation failed with unknown error after %d attempts (total time: %v)", maxRetries+1)
	return nil, fmt.Errorf("load failed: unknown error")
}
