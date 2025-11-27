<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 🚀 Doris Go SDK

[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.19-blue.svg)](https://golang.org/)
[![Thread Safe](https://img.shields.io/badge/Thread%20Safe-✅-brightgreen.svg)](#-thread-safety)

A lightweight Apache Doris import client (Go version) with easy-to-use, high-performance, and production-ready features, continuously maintained by the Apache Doris core contributor team.

## ✨ Key Features

**Easy to Use**: Provides a simple user experience with internal encapsulation of complex logic such as HTTP parameter configuration, multiple data format support, and intelligent retry mechanisms.

**High Performance**: Supports extremely high write throughput with built-in optimizations including efficient concurrency handling, and batch import practices.

**Production Ready**: Validated in large-scale, high-pressure production environments with excellent full-chain observability.

## 📦 Quick Installation

```bash
go get github.com/apache/doris/sdk/go-doris-sdk
```

## 🚀 Quick Start

### Basic CSV Loading

```go
package main

import (
	"fmt"
	"github.com/apache/doris/sdk/go-doris-sdk"
)

func main() {
	// 🎯 New API: Direct configuration construction
	config := &doris.Config{
		Endpoints:   []string{"http://127.0.0.1:8030"},
		User:        "root",
		Password:    "password",
		Database:    "test_db",
		Table:       "users",
		Format:      doris.DefaultCSVFormat(),
		Retry:       doris.DefaultRetry(),
		GroupCommit: doris.ASYNC,
	}

	// Create client
	client, err := doris.NewLoadClient(config)
	if err != nil {
		panic(err)
	}

	// Load data
	data := "1,Alice,25\n2,Bob,30\n3,Charlie,35"
	response, err := client.Load(doris.StringReader(data))
	
	if err != nil {
		fmt.Printf("❌ Load failed: %v\n", err)
		return
	}

	if response.Status == doris.SUCCESS {
		fmt.Printf("✅ Successfully loaded %d rows!\n", response.Resp.NumberLoadedRows)
	}
}
```
[
### JSON Data Loading

```go
config := &doris.Config{
	Endpoints:   []string{"http://127.0.0.1:8030"},
	User:        "root",
	Password:    "password", 
	Database:    "test_db",
	Table:       "users",
	Format:      doris.DefaultJSONFormat(), // JSON Lines format
	Retry:       doris.DefaultRetry(),
	GroupCommit: doris.ASYNC,
}

client, _ := doris.NewLoadClient(config)

// JSON Lines data
jsonData := `{"id":1,"name":"Alice","age":25}
{"id":2,"name":"Bob","age":30}
{"id":3,"name":"Charlie","age":35}`

response, err := client.Load(doris.StringReader(jsonData))
```

## 🛠️ Configuration Guide

### Basic Configuration

```go
config := &doris.Config{
	// Required fields
	Endpoints: []string{
		"http://fe1:8630",
		"http://fe2:8630",    // Multiple FE nodes supported, auto load balancing
	},
	User:     "your_username",
	Password: "your_password",
	Database: "your_database",
	Table:    "your_table",
	
	// Optional fields
	LabelPrefix: "my_app",           // Label prefix
	Label:       "custom_label_001", // Custom label
	Format:      doris.DefaultCSVFormat(),
	Retry:       doris.DefaultRetry(),
	GroupCommit: doris.ASYNC,
	Options: map[string]string{
		"timeout":           "3600",
		"max_filter_ratio":  "0.1",
		"strict_mode":       "true",
	},
}
```

### Data Format Configuration

```go
// 1. Use default formats (recommended)
Format: doris.DefaultJSONFormat()  // JSON Lines, read_json_by_line=true
Format: doris.DefaultCSVFormat()   // CSV, comma separated, newline delimiter

// 2. Custom JSON format
Format: &doris.JSONFormat{Type: doris.JSONObjectLine}  // JSON Lines
Format: &doris.JSONFormat{Type: doris.JSONArray}       // JSON Array

// 3. Custom CSV format
Format: &doris.CSVFormat{
	ColumnSeparator: "|",     // Pipe separator
	LineDelimiter:   "\n",    // Newline delimiter
}
```

### Retry Strategy Configuration

```go
// 1. Use default retry (recommended)
Retry: doris.DefaultRetry()  // 6 retries, 60 seconds total
// Retry intervals: [1s, 2s, 4s, 8s, 16s, 32s]

// 2. Custom retry
Retry: &doris.Retry{
	MaxRetryTimes:  3,      // Maximum retry times
	BaseIntervalMs: 2000,   // Base interval 2 seconds
	MaxTotalTimeMs: 30000,  // Total time limit 30 seconds
}

// 3. Disable retry
Retry: nil
```

### Group Commit Mode

```go
GroupCommit: doris.ASYNC,  // Async mode, highest throughput
GroupCommit: doris.SYNC,   // Sync mode, immediately visible
GroupCommit: doris.OFF,    // Off, use traditional mode
```

> ⚠️ **Note**: When Group Commit is enabled, all Label configurations are automatically ignored and warning logs are recorded.

## 🔄 Concurrent Usage

### Basic Concurrency Example

```go
func worker(id int, client *doris.DorisLoadClient, wg *sync.WaitGroup) {
	defer wg.Done()
	
	// ✅ Each worker uses independent data
	data := fmt.Sprintf("%d,Worker_%d,Data", id, id)
	
	response, err := client.Load(doris.StringReader(data))
	if err != nil {
		fmt.Printf("Worker %d failed: %v\n", id, err)
		return
	}
	
	if response.Status == doris.SUCCESS {
		fmt.Printf("✅ Worker %d successfully loaded %d rows\n", id, response.Resp.NumberLoadedRows)
	}
}

func main() {
	client, _ := doris.NewLoadClient(config)
	
	var wg sync.WaitGroup
	// 🚀 Launch 10 concurrent workers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(i, client, &wg)
	}
	wg.Wait()
}
```

### ⚠️ Thread Safety Notes

- ✅ **DorisLoadClient is thread-safe** - Can be shared across multiple goroutines
- ❌ **Readers should not be shared** - Each goroutine should use independent data sources

```go
// ✅ Correct concurrent pattern
for i := 0; i < numWorkers; i++ {
	go func(workerID int) {
		data := generateWorkerData(workerID)  // Independent data
		response, err := client.Load(doris.StringReader(data))
	}(i)
}

// ❌ Wrong concurrent pattern - Don't do this!
file, _ := os.Open("data.csv")
for i := 0; i < 10; i++ {
	go func() {
		client.Load(file)  // ❌ Multiple goroutines sharing same Reader
	}()
}
```

## 📊 Response Handling

```go
response, err := client.Load(data)

// 1. Check system-level errors
if err != nil {
	fmt.Printf("System error: %v\n", err)
	return
}

// 2. Check load status
switch response.Status {
case doris.SUCCESS:
	fmt.Printf("✅ Load successful!\n")
	fmt.Printf("📊 Statistics:\n")
	fmt.Printf("  - Loaded rows: %d\n", response.Resp.NumberLoadedRows)
	fmt.Printf("  - Loaded bytes: %d\n", response.Resp.LoadBytes)
	fmt.Printf("  - Time: %d ms\n", response.Resp.LoadTimeMs)
	fmt.Printf("  - Label: %s\n", response.Resp.Label)
	
case doris.FAILURE:
	fmt.Printf("❌ Load failed: %s\n", response.ErrorMessage)
	
	// Get detailed error information
	if response.Resp.ErrorURL != "" {
		fmt.Printf("🔍 Error details: %s\n", response.Resp.ErrorURL)
	}
}
```

## 🔍 Log Control

### Basic Log Configuration

```go
// Set log level
doris.SetLogLevel(doris.LogLevelInfo)   // Recommended for production
doris.SetLogLevel(doris.LogLevelDebug)  // For development debugging
doris.SetLogLevel(doris.LogLevelError)  // Only show errors

// Disable all logs
doris.DisableLogging()

// Output to file
file, _ := os.OpenFile("doris.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
doris.SetLogOutput(file)
```

### Concurrent Scenario Logging

```go
// Create context logger for each worker
logger := doris.NewContextLogger("Worker-1")
logger.Infof("Starting batch %d", batchID)
logger.Warnf("Retry detected, attempt: %d", retryCount)
```

### Integrate Third-party Logging Libraries

```go
import "github.com/sirupsen/logrus"

logger := logrus.New()
logger.SetLevel(logrus.InfoLevel)

// Integrate with Doris SDK
doris.SetCustomLogFuncs(
	logger.Debugf,  // Debug level
	logger.Infof,   // Info level
	logger.Warnf,   // Warn level
	logger.Errorf,  // Error level
)
```

## 📈 Production Examples

We provide complete production-level examples:

```bash
# Run all examples
go run cmd/examples/main.go all

# Individual examples
go run cmd/examples/main.go single       # Large batch load (100k records)
go run cmd/examples/main.go concurrent   # Concurrent load (1M records, 10 workers)
go run cmd/examples/main.go json         # JSON load (50k records)
go run cmd/examples/main.go basic        # Basic concurrency (5 workers)
```

## 🛠️ Utility Tools

### Data Conversion Helpers

```go
// String to Reader
reader := doris.StringReader("1,Alice,25\n2,Bob,30")

// Byte array to Reader
data := []byte("1,Alice,25\n2,Bob,30")
reader := doris.BytesReader(data)

// Struct to JSON Reader
users := []User{{ID: 1, Name: "Alice"}}
reader, err := doris.JSONReader(users)
```

### Default Configuration Builders

```go
// Quick create common configurations
retry := doris.DefaultRetry()           // 6 retries, 60 seconds total
jsonFormat := doris.DefaultJSONFormat() // JSON Lines format
csvFormat := doris.DefaultCSVFormat()   // Standard CSV format

// Custom configuration
customRetry := doris.NewRetry(3, 1000) // 3 retries, 1 second base interval
```

## 📚 Documentation and Examples

- 📖 [API Migration Guide](docs/API_MIGRATION_GUIDE.md) - Guide for upgrading from old API
- 🧵 [Thread Safety Analysis](docs/THREAD_SAFETY_ANALYSIS.md) - Detailed concurrency safety documentation
- 🔍 [Reader Concurrency Analysis](docs/READER_CONCURRENCY_ANALYSIS.md) - Reader usage best practices
- 📝 [Example Details](examples/README.md) - Detailed explanation of all examples

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 🙏 Acknowledgments

Maintained by the Apache Doris core contributor team.
