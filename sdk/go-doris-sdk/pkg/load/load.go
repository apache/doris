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

// Package load provides the Doris Stream Load client functionality
package load

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/client"
	"github.com/apache/doris/sdk/go-doris-sdk/pkg/load/config"
	loader "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/loader"
	log "github.com/apache/doris/sdk/go-doris-sdk/pkg/load/logger"
)

// ================================
// Public API Types
// ================================

// Config is the main configuration structure for stream load operations
type Config = config.Config

// DorisLoadClient provides functionality to load data into Doris using stream load API
type DorisLoadClient = client.DorisLoadClient

// Format aliases
type Format = config.Format
type JSONFormatType = config.JSONFormatType
type JSONFormat = config.JSONFormat
type CSVFormat = config.CSVFormat

// Config aliases (for backward compatibility)
type LoadSetting = config.Config
type BatchMode = config.GroupCommitMode
type GroupCommitMode = config.GroupCommitMode
type Retry = config.Retry

// Log aliases
type LogLevel = log.Level
type LogFunc = log.LogFunc
type ContextLogger = log.ContextLogger

// Load aliases
type LoadResponse = loader.LoadResponse
type LoadStatus = loader.LoadStatus
type RespContent = loader.RespContent

// ================================
// Constants
// ================================

const (
	// JSON format constants
	JSONObjectLine = config.JSONObjectLine
	JSONArray      = config.JSONArray

	// Batch mode constants
	SYNC  = config.SYNC
	ASYNC = config.ASYNC
	OFF   = config.OFF

	// Load status constants
	SUCCESS = loader.SUCCESS
	FAILURE = loader.FAILURE

	// Log level constants
	LogLevelDebug = log.LevelDebug
	LogLevelInfo  = log.LevelInfo
	LogLevelWarn  = log.LevelWarn
	LogLevelError = log.LevelError
)

// ================================
// Client Creation Functions
// ================================

// NewLoadClient creates a new Doris stream load client with the given configuration
func NewLoadClient(cfg *Config) (*DorisLoadClient, error) {
	return client.NewDorisClient(cfg)
}

// ================================
// Retry Configuration
// ================================

// NewRetry creates a new retry configuration
func NewRetry(maxRetryTimes int, baseIntervalMs int64) *Retry {
	return &Retry{
		MaxRetryTimes:  maxRetryTimes,
		BaseIntervalMs: baseIntervalMs,
		MaxTotalTimeMs: 60000, // Default 60 seconds total
	}
}

// DefaultRetry creates a new retry configuration with default values (6 retries, 1 second base interval, 60s total)
// Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s = ~63 seconds total retry time
func DefaultRetry() *Retry {
	return &Retry{
		MaxRetryTimes:  6,     // Maximum 6 retries
		BaseIntervalMs: 1000,  // 1 second base interval
		MaxTotalTimeMs: 60000, // 60 seconds total limit
	}
}

// NewDefaultRetry creates a new retry configuration with default values (6 retries, 1 second base interval, 60s total)
// Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s = ~63 seconds total retry time
func NewDefaultRetry() *Retry {
	return DefaultRetry()
}

// ================================
// Format Configuration
// ================================

// DefaultJSONFormat creates a default JSON format configuration (JSONObjectLine)
func DefaultJSONFormat() *JSONFormat {
	return &JSONFormat{Type: JSONObjectLine}
}

// DefaultCSVFormat creates a default CSV format configuration (comma separator, newline delimiter)
func DefaultCSVFormat() *CSVFormat {
	return &CSVFormat{
		ColumnSeparator: ",",
		LineDelimiter:   "\\n",
	}
}

// ================================
// Data Conversion Helpers
// ================================

// StringReader converts string data to io.Reader
func StringReader(data string) io.Reader {
	return strings.NewReader(data)
}

// BytesReader converts byte data to io.Reader
func BytesReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// JSONReader converts any JSON-serializable object to io.Reader
func JSONReader(data interface{}) (io.Reader, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(string(jsonBytes)), nil
}

// ================================
// Log Control Functions
// ================================

// SetLogLevel sets the minimum log level for the SDK
// Available levels: LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError
func SetLogLevel(level LogLevel) {
	log.SetLevel(level)
}

// SetLogOutput sets the output destination for SDK logs
func SetLogOutput(output *os.File) {
	log.SetOutput(output)
}

// DisableLogging completely disables all SDK logging
func DisableLogging() {
	log.SetLevel(log.Level(999))
}

// SetCustomLogFunc allows users to integrate their own logging systems
func SetCustomLogFunc(level LogLevel, fn LogFunc) {
	switch level {
	case log.LevelDebug:
		log.SetDebugFunc(fn)
	case log.LevelInfo:
		log.SetInfoFunc(fn)
	case log.LevelWarn:
		log.SetWarnFunc(fn)
	case log.LevelError:
		log.SetErrorFunc(fn)
	}
}

// SetCustomLogFuncs allows setting all log functions at once
func SetCustomLogFuncs(debugFn, infoFn, warnFn, errorFn LogFunc) {
	if debugFn != nil {
		log.SetDebugFunc(debugFn)
	}
	if infoFn != nil {
		log.SetInfoFunc(infoFn)
	}
	if warnFn != nil {
		log.SetWarnFunc(warnFn)
	}
	if errorFn != nil {
		log.SetErrorFunc(errorFn)
	}
}

// NewContextLogger creates a context logger with the given context string
func NewContextLogger(context string) *ContextLogger {
	return log.NewContextLogger(context)
}
