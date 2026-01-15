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

// Package log provides a simple logging interface for the Doris Stream Load Client
// Enhanced with millisecond precision and goroutine tracking for concurrent scenarios
package logger

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// LogFunc is a function type for logging with formatting
type LogFunc func(format string, args ...interface{})

// Level represents log levels
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String returns the string representation of the log level
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO "
	case LevelWarn:
		return "WARN "
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Global logging configuration
var (
	// Current minimum log level
	// Default to INFO for security reasons (DEBUG may expose sensitive information like passwords)
	currentLevel = LevelInfo

	// Enhanced logger with custom formatter
	stdLogger = log.New(os.Stdout, "", 0) // Remove default flags, we'll format ourselves

	// Debug logging function - can be customized
	DebugFunc LogFunc = defaultLogFunc(LevelDebug)

	// Info logging function - can be customized
	InfoFunc LogFunc = defaultLogFunc(LevelInfo)

	// Warn logging function - can be customized
	WarnFunc LogFunc = defaultLogFunc(LevelWarn)

	// Error logging function - can be customized
	ErrorFunc LogFunc = defaultLogFunc(LevelError)
)

// getGoroutineID returns the current goroutine ID for log tracking
func getGoroutineID() uint64 {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	// Parse "goroutine 123 [running]:"
	buf = buf[len("goroutine "):]
	idx := 0
	for i, b := range buf {
		if b < '0' || b > '9' {
			idx = i
			break
		}
	}
	id, _ := strconv.ParseUint(string(buf[:idx]), 10, 64)
	return id
}

// formatTimestamp returns a timestamp with millisecond precision
func formatTimestamp() string {
	now := time.Now()
	return now.Format("2006/01/02 15:04:05.000")
}

// getCallerInfo returns the file and line number of the caller
func getCallerInfo() string {
	// We need to skip more levels to get to the actual caller
	// Let's try different skip levels to find the right one
	for skip := 3; skip <= 8; skip++ {
		_, file, line, ok := runtime.Caller(skip)
		if !ok {
			continue
		}

		// Only show the filename, not the full path
		if idx := strings.LastIndex(file, "/"); idx >= 0 {
			file = file[idx+1:]
		}

		// Skip our own log package files and go runtime files
		if !strings.Contains(file, "log.go") &&
			!strings.Contains(file, "asm_") &&
			!strings.Contains(file, "proc.go") {
			return fmt.Sprintf("%s:%d", file, line)
		}
	}

	// Fallback if we can't find the right caller
	return "unknown:0"
}

// defaultLogFunc creates an enhanced logging function for the given level
func defaultLogFunc(level Level) LogFunc {
	return func(format string, args ...interface{}) {
		if level >= currentLevel {
			// Enhanced format: [TIMESTAMP] [LEVEL] [goroutine-ID] [file:line] message
			timestamp := formatTimestamp()
			gid := getGoroutineID()
			caller := getCallerInfo()

			var message string
			if len(args) == 0 {
				message = format
			} else {
				message = fmt.Sprintf(format, args...)
			}

			logLine := fmt.Sprintf("[%s] [%s] [G-%d] [%s] %s",
				timestamp, level.String(), gid, caller, message)

			stdLogger.Output(1, logLine)
		}
	}
}

// SetLevel sets the minimum log level
func SetLevel(level Level) {
	currentLevel = level
}

// SetOutput sets the output destination for the default logger
func SetOutput(output *os.File) {
	stdLogger.SetOutput(output)
}

// SetDebugFunc sets a custom debug logging function
func SetDebugFunc(fn LogFunc) {
	DebugFunc = fn
}

// SetInfoFunc sets a custom info logging function
func SetInfoFunc(fn LogFunc) {
	InfoFunc = fn
}

// SetWarnFunc sets a custom warn logging function
func SetWarnFunc(fn LogFunc) {
	WarnFunc = fn
}

// SetErrorFunc sets a custom error logging function
func SetErrorFunc(fn LogFunc) {
	ErrorFunc = fn
}

// Package level logging functions - enhanced for concurrent scenarios

// Debugf logs a debug message with formatting
func Debugf(format string, args ...interface{}) {
	DebugFunc(format, args...)
}

// Infof logs an info message with formatting
func Infof(format string, args ...interface{}) {
	InfoFunc(format, args...)
}

// Warnf logs a warning message with formatting
func Warnf(format string, args ...interface{}) {
	WarnFunc(format, args...)
}

// Errorf logs an error message with formatting
func Errorf(format string, args ...interface{}) {
	ErrorFunc(format, args...)
}

// Debug logs a debug message without formatting
func Debug(args ...interface{}) {
	DebugFunc(fmt.Sprint(args...), nil)
}

// Info logs an info message without formatting
func Info(args ...interface{}) {
	InfoFunc(fmt.Sprint(args...), nil)
}

// Warn logs a warning message without formatting
func Warn(args ...interface{}) {
	WarnFunc(fmt.Sprint(args...), nil)
}

// Error logs an error message without formatting
func Error(args ...interface{}) {
	ErrorFunc(fmt.Sprint(args...), nil)
}

// WithContext creates a contextual logger that includes additional information
type ContextLogger struct {
	context string
}

// NewContextLogger creates a new context logger with the given context string
func NewContextLogger(context string) *ContextLogger {
	return &ContextLogger{context: context}
}

// Debugf logs a debug message with context
func (cl *ContextLogger) Debugf(format string, args ...interface{}) {
	Debugf("[%s] %s", cl.context, fmt.Sprintf(format, args...))
}

// Infof logs an info message with context
func (cl *ContextLogger) Infof(format string, args ...interface{}) {
	Infof("[%s] %s", cl.context, fmt.Sprintf(format, args...))
}

// Warnf logs a warning message with context
func (cl *ContextLogger) Warnf(format string, args ...interface{}) {
	Warnf("[%s] %s", cl.context, fmt.Sprintf(format, args...))
}

// Errorf logs an error message with context
func (cl *ContextLogger) Errorf(format string, args ...interface{}) {
	Errorf("[%s] %s", cl.context, fmt.Sprintf(format, args...))
}
