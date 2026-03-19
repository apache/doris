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

// Usage:
//   go run cmd/compress_bench/main.go
//
// Generates sample CSV and JSON data at various batch sizes,
// compresses with gzip, and prints original/compressed sizes and ratio.

package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
)

func gzipSize(data string) int {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte(data))
	gz.Close()
	return buf.Len()
}

// buildCSV generates n rows of CSV data
func buildCSV(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "%d,User_%d,%d\n", 1000+i, i, 20+i%50)
	}
	return sb.String()
}

// buildJSON generates n rows of JSON Lines data
func buildJSON(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "{\"id\":%d,\"name\":\"User_%d\",\"age\":%d}\n", 1000+i, i, 20+i%50)
	}
	return sb.String()
}

func printResult(label string, data string) {
	original := len(data)
	compressed := gzipSize(data)
	compressedBy := (1 - float64(compressed)/float64(original)) * 100
	fmt.Printf("  %-40s original=%8d B  after_gzip=%8d B  compressed_by=%.1f%%\n",
		label, original, compressed, compressedBy)
}

func main() {
	sizes := []int{100, 1000, 10000, 100000, 1000000, 10000000}

	fmt.Println("=== Gzip Compression Benchmark ===")
	fmt.Println()

	fmt.Println("[CSV format]")
	for _, n := range sizes {
		data := buildCSV(n)
		printResult(fmt.Sprintf("%d rows (~%d KB)", n, len(data)/1024+1), data)
	}

	fmt.Println()
	fmt.Println("[JSON format]")
	for _, n := range sizes {
		data := buildJSON(n)
		printResult(fmt.Sprintf("%d rows (~%d KB)", n, len(data)/1024+1), data)
	}
}
