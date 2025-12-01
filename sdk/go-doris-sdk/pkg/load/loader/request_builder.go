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
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/config"
	log "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/logger"
	"github.com/google/uuid"
)

const (
	StreamLoadPattern = "http://%s/api/%s/%s/_stream_load"
)

// getNode randomly selects an endpoint and returns the parsed host
func getNode(endpoints []string) (string, error) {
	if len(endpoints) == 0 {
		return "", fmt.Errorf("no endpoints available")
	}

	// Use global rand.Intn which is thread-safe in Go 1.0+
	randomIndex := rand.Intn(len(endpoints))
	endpoint := endpoints[randomIndex]

	// Parse the endpoint URL to extract the host
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %v", err)
	}

	return endpointURL.Host, nil
}

// CreateStreamLoadRequest creates an HTTP PUT request for Doris stream load
func CreateStreamLoadRequest(cfg *config.Config, data io.Reader, attempt int) (*http.Request, error) {
	// Get a random endpoint host
	host, err := getNode(cfg.Endpoints)
	if err != nil {
		return nil, err
	}

	// Construct the load URL
	loadURL := fmt.Sprintf(StreamLoadPattern, host, cfg.Database, cfg.Table)

	// Create the HTTP PUT request
	req, err := http.NewRequest(http.MethodPut, loadURL, data)
	if err != nil {
		return nil, err
	}

	// Add basic authentication
	authInfo := fmt.Sprintf("%s:%s", cfg.User, cfg.Password)
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(authInfo))
	req.Header.Set("Authorization", "Basic "+encodedAuth)

	// Add common headers
	req.Header.Set("Expect", "100-continue")

	// Build and add all stream load options as headers
	allOptions := buildStreamLoadOptions(cfg)
	for key, value := range allOptions {
		req.Header.Set(key, value)
	}

	// Handle label generation based on group commit usage
	handleLabelForRequest(cfg, req, allOptions, attempt)

	return req, nil
}

// handleLabelForRequest handles label generation and setting based on group commit configuration
func handleLabelForRequest(cfg *config.Config, req *http.Request, allOptions map[string]string, attempt int) {
	// Check if group commit is enabled
	_, isGroupCommitEnabled := allOptions["group_commit"]

	if isGroupCommitEnabled {
		// Group commit is enabled, labels are not allowed
		if cfg.Label != "" {
			// User provided a custom label but group commit is enabled
			log.Warnf("Custom label '%s' specified but group_commit is enabled. Labels are not allowed with group commit, removing label.", cfg.Label)
		}
		if cfg.LabelPrefix != "" {
			// User provided a label prefix but group commit is enabled
			log.Warnf("Label prefix '%s' specified but group_commit is enabled. Labels are not allowed with group commit, removing label prefix.", cfg.LabelPrefix)
		}
		// Log the removal action
		log.Infof("Group commit is enabled - all labels removed from request headers for compliance")
		// Do not set any label when group commit is enabled
		return
	}

	// Group commit is not enabled, generate and set label
	label := generateLabel(cfg, attempt)
	req.Header.Set("label", label)

	if attempt > 0 {
		log.Debugf("Generated retry label for attempt %d: %s", attempt, label)
	} else {
		log.Debugf("Generated label: %s", label)
	}
}

// buildStreamLoadOptions builds all stream load options from configuration
func buildStreamLoadOptions(cfg *config.Config) map[string]string {
	result := make(map[string]string)

	// Add user-defined options first
	for k, v := range cfg.Options {
		result[k] = v
	}

	// Add format-specific options
	if cfg.Format != nil {
		for k, v := range cfg.Format.GetOptions() {
			result[k] = v
		}
	}

	// Add group commit options
	switch cfg.GroupCommit {
	case config.SYNC:
		result["group_commit"] = "sync_mode"
	case config.ASYNC:
		result["group_commit"] = "async_mode"
	case config.OFF:
		// Don't add group_commit option
	}

	return result
}

// generateLabel creates a unique label for the load job, considering retry attempts
func generateLabel(cfg *config.Config, attempt int) string {
	currentTimeMillis := time.Now().UnixMilli()
	id := uuid.New()

	// If user provided a custom label, handle retry scenarios
	if cfg.Label != "" {
		if attempt == 0 {
			// First attempt: use the original label
			return cfg.Label
		} else {
			// Retry attempts: append retry suffix to ensure uniqueness
			return fmt.Sprintf("%s_retry_%d_%d_%s", cfg.Label, attempt, currentTimeMillis, id.String()[:8])
		}
	}

	// Generate a unique label when no custom label is provided
	prefix := cfg.LabelPrefix
	if prefix == "" {
		prefix = "load"
	}

	if attempt == 0 {
		// First attempt
		return fmt.Sprintf("%s_%s_%s_%d_%s", prefix, cfg.Database, cfg.Table, currentTimeMillis, id.String())
	} else {
		// Retry attempts: include attempt number for uniqueness
		return fmt.Sprintf("%s_%s_%s_%d_retry_%d_%s", prefix, cfg.Database, cfg.Table, currentTimeMillis, attempt, id.String())
	}
}
