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
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/exception"
	log "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/logger"
	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/util"

	jsoniter "github.com/json-iterator/go"
)

// StreamLoader handles loading data into Doris via HTTP stream load
type StreamLoader struct {
	httpClient *http.Client
	json       jsoniter.API
}

// NewStreamLoader creates a new StreamLoader
func NewStreamLoader() *StreamLoader {
	return &StreamLoader{
		httpClient: util.GetHttpClient(),
		json:       jsoniter.ConfigCompatibleWithStandardLibrary,
	}
}

// Load sends the HTTP request to Doris via stream load
func (s *StreamLoader) Load(req *http.Request) (*LoadResponse, error) {
	// Execute the request - this is the main performance bottleneck
	log.Debugf("[TIMING] Sending HTTP request...")
	requestStartTime := time.Now()
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to execute HTTP request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	requestDuration := time.Since(requestStartTime)
	log.Debugf("[TIMING] HTTP request completed: %v", requestDuration)

	// Handle the response
	result, err := s.handleResponse(resp)

	return result, err
}

// handleResponse processes the HTTP response from a stream load request
func (s *StreamLoader) handleResponse(resp *http.Response) (*LoadResponse, error) {
	statusCode := resp.StatusCode
	log.Debugf("Received HTTP response with status code: %d", statusCode)

	if statusCode == http.StatusOK && resp.Body != nil {
		// Read the response body with limited buffer
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024)) // 1MB limit
		if err != nil {
			log.Errorf("Failed to read response body: %v", err)
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		log.Infof("Stream Load Response: %s", string(body))

		// Parse the response
		var respContent RespContent
		if err := s.json.Unmarshal(body, &respContent); err != nil {
			log.Errorf("Failed to unmarshal JSON response: %v", err)
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}

		// Check status and return result
		if isSuccessStatus(respContent.Status) {
			log.Infof("Load operation completed successfully")
			return &LoadResponse{
				Status: SUCCESS,
				Resp:   respContent,
			}, nil
		} else {
			log.Errorf("Load operation failed with status: %s", respContent.Status)
			errorMessage := ""
			if respContent.Message != "" {
				errorMessage = fmt.Sprintf("load failed. cause by: %s, please check more detail from url: %s",
					respContent.Message, respContent.ErrorURL)
			} else {
				errorMessage = string(body)
			}

			return &LoadResponse{
				Status:       FAILURE,
				Resp:         respContent,
				ErrorMessage: errorMessage,
			}, nil
		}
	}

	// For non-200 status codes, return an error that can be retried
	log.Errorf("Stream load failed with HTTP status: %s", resp.Status)

	return nil, exception.NewStreamLoadError(fmt.Sprintf("stream load error: %s", resp.Status))
}

// isSuccessStatus checks if the status indicates success
func isSuccessStatus(status string) bool {
	return strings.EqualFold(status, "success")
}
