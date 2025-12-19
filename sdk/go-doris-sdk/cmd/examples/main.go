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

// Package main provides a unified entry point for running all Doris Stream Load examples
// Usage: go run cmd/examples/main.go [example_name]
// Available examples: single, concurrent, json, basic
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/doris/sdk/go-doris-sdk/examples"
)

const usage = `
Doris Stream Load Client - Production Examples Runner

Usage: go run cmd/examples/main.go [example_name]

Available Examples:
  single      - Production single batch loading (100,000 records)
  concurrent  - Production concurrent loading (1,000,000 records across 10 workers)
  json        - Production JSON data loading (50,000 JSON records)
  basic       - Basic concurrent loading demo (5 workers)
  all         - Run all examples sequentially

Examples:
  go run cmd/examples/main.go single
  go run cmd/examples/main.go concurrent
  go run cmd/examples/main.go json
  go run cmd/examples/main.go basic
  go run cmd/examples/main.go all

Description:
  single      - Demonstrates single-threaded large batch loading with realistic product data
  concurrent  - Shows high-throughput concurrent loading with 10 workers processing order data
  json        - Illustrates JSON Lines format loading with structured user activity data
  basic       - Simple concurrent example for learning and development
  all         - Runs all examples in sequence for comprehensive testing

For more details, see examples/README.md
`

func printUsage() {
	fmt.Print(usage)
}

func runExample(name string) {
	switch strings.ToLower(name) {
	case "single":
		fmt.Println("Running Production Single Batch Example...")
		examples.RunSingleBatchExample()
	case "concurrent":
		fmt.Println("Running Production Concurrent Example...")
		examples.RunConcurrentExample()
	case "json":
		fmt.Println("Running Production JSON Example...")
		examples.RunJSONExample()
	case "basic":
		fmt.Println("Running Basic Concurrent Example...")
		examples.RunBasicConcurrentExample()
	case "all":
		fmt.Println("Running All Examples...")
		fmt.Println("\n" + strings.Repeat("=", 80))
		examples.RunSingleBatchExample()
		fmt.Println("\n" + strings.Repeat("=", 80))
		examples.RunConcurrentExample()
		fmt.Println("\n" + strings.Repeat("=", 80))
		examples.RunJSONExample()
		fmt.Println("\n" + strings.Repeat("=", 80))
		examples.RunBasicConcurrentExample()
		fmt.Println("\n" + strings.Repeat("=", 80))
		fmt.Println("All examples completed!")
	default:
		fmt.Printf("❌ Unknown example: %s\n\n", name)
		printUsage()
		os.Exit(1)
	}
}

func main() {
	fmt.Println("🚀 Doris Stream Load Client - Examples Runner")
	fmt.Println("=" + strings.Repeat("=", 50))

	if len(os.Args) < 2 {
		fmt.Println("❌ No example specified\n")
		printUsage()
		os.Exit(1)
	}

	exampleName := os.Args[1]

	// Show what we're about to run
	fmt.Printf("📋 Selected example: %s\n", exampleName)
	fmt.Println(strings.Repeat("-", 50))

	runExample(exampleName)
}
