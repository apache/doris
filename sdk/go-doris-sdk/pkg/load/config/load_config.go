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

package config

import (
	"fmt"
)

// Format interface defines the data format for stream load
type Format interface {
	// GetFormatType returns the format type (json or csv)
	GetFormatType() string
	// GetOptions returns format-specific options as map for headers
	GetOptions() map[string]string
}

// JSONFormatType defines JSON format subtypes
type JSONFormatType string

const (
	JSONObjectLine JSONFormatType = "object_line" // JSON objects separated by newlines
	JSONArray      JSONFormatType = "array"       // JSON array
)

// JSONFormat represents JSON format configuration
// Usage: &JSONFormat{Type: JSONObjectLine} or &JSONFormat{Type: JSONArray}
type JSONFormat struct {
	Type JSONFormatType
}

// GetFormatType implements Format interface
func (f *JSONFormat) GetFormatType() string {
	return "json"
}

// GetOptions implements Format interface - returns headers for JSON format
func (f *JSONFormat) GetOptions() map[string]string {
	options := make(map[string]string)
	options["format"] = "json"

	switch f.Type {
	case JSONObjectLine:
		options["strip_outer_array"] = "false"
		options["read_json_by_line"] = "true"
	case JSONArray:
		options["strip_outer_array"] = "true"
	}

	return options
}

// CSVFormat represents CSV format configuration
// Usage: &CSVFormat{ColumnSeparator: ",", LineDelimiter: "\n"}
type CSVFormat struct {
	ColumnSeparator string
	LineDelimiter   string
}

// GetFormatType implements Format interface
func (f *CSVFormat) GetFormatType() string {
	return "csv"
}

// GetOptions implements Format interface - returns headers for CSV format
func (f *CSVFormat) GetOptions() map[string]string {
	options := make(map[string]string)
	options["format"] = "csv"
	options["column_separator"] = f.ColumnSeparator
	options["line_delimiter"] = f.LineDelimiter
	return options
}

// GroupCommitMode defines the group commit mode
type GroupCommitMode int

const (
	// SYNC represents synchronous group commit mode
	SYNC GroupCommitMode = iota
	// ASYNC represents asynchronous group commit mode
	ASYNC
	// OFF represents disabled group commit mode
	OFF
)

// Retry contains configuration for retry attempts when loading data
type Retry struct {
	MaxRetryTimes  int   // Maximum number of retry attempts
	BaseIntervalMs int64 // Base interval in milliseconds for exponential backoff
	MaxTotalTimeMs int64 // Maximum total time for all retries in milliseconds
}

// Config contains all configuration for stream load operations
type Config struct {
	Endpoints   []string
	User        string
	Password    string
	Database    string
	Table       string
	LabelPrefix string
	Label       string
	Format      Format // Can be &JSONFormat{...} or &CSVFormat{...}
	Retry       *Retry
	GroupCommit GroupCommitMode
	Options     map[string]string
}

// ValidateInternal validates the configuration
func (c *Config) ValidateInternal() error {
	if c.User == "" {
		return fmt.Errorf("user cannot be empty")
	}

	if c.Database == "" {
		return fmt.Errorf("database cannot be empty")
	}

	if c.Table == "" {
		return fmt.Errorf("table cannot be empty")
	}

	if len(c.Endpoints) == 0 {
		return fmt.Errorf("endpoints cannot be empty")
	}

	if c.Format == nil {
		return fmt.Errorf("format cannot be nil")
	}

	if c.Retry != nil {
		if c.Retry.MaxRetryTimes < 0 {
			return fmt.Errorf("maxRetryTimes cannot be negative")
		}
		if c.Retry.BaseIntervalMs < 0 {
			return fmt.Errorf("retryIntervalMs cannot be negative")
		}
		if c.Retry.MaxTotalTimeMs < 0 {
			return fmt.Errorf("maxTotalTimeMs cannot be negative")
		}
	}

	return nil
}
