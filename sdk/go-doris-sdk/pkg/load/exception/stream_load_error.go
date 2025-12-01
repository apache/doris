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

// Package exception provides error types used in the Doris Stream Load client
package exception

// StreamLoadError represents an error that occurred during a stream load operation
type StreamLoadError struct {
	Message string
}

// Error returns the error message
func (e *StreamLoadError) Error() string {
	return e.Message
}

// NewStreamLoadError creates a new StreamLoadError with the given message
func NewStreamLoadError(message string) *StreamLoadError {
	return &StreamLoadError{
		Message: message,
	}
}
