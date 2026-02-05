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

// Package examples provides unified data generation utilities for all Doris Stream Load examples
// This file centralizes all mock data generation logic for better code reuse and maintainability
// All examples use unified Order schema for consistency
package examples

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	doris "github.com/apache/doris/sdk/go-doris-sdk"
)

// Common data for generating realistic order test data
var (
	// Unified order-related data
	Categories = []string{"Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Automotive", "Food", "Health", "Toys"}
	Brands     = []string{"Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Canon", "Dell", "HP", "Xiaomi", "Huawei", "Lenovo"}
	Statuses   = []string{"active", "inactive", "pending", "discontinued", "completed", "cancelled"}
	Regions    = []string{"North", "South", "East", "West", "Central"}

	// Additional data for variety
	Countries = []string{"US", "CN", "JP", "DE", "UK", "FR", "CA", "AU", "IN", "BR"}
	Devices   = []string{"mobile", "desktop", "tablet"}
)

// OrderRecord represents a unified e-commerce order record (used by all examples)
type OrderRecord struct {
	OrderID     int     `json:"order_id"`
	CustomerID  int     `json:"customer_id"`
	ProductName string  `json:"product_name"`
	Category    string  `json:"category"`
	Brand       string  `json:"brand"`
	Quantity    int     `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	TotalAmount float64 `json:"total_amount"`
	Status      string  `json:"status"`
	OrderDate   string  `json:"order_date"`
	Region      string  `json:"region"`
}

// DataGeneratorConfig holds configuration for data generation
type DataGeneratorConfig struct {
	WorkerID    int    // For concurrent scenarios
	BatchSize   int    // Number of records to generate
	ContextName string // For logging context
	RandomSeed  int64  // For reproducible random data
}

// GenerateOrderCSV creates realistic order data in CSV format (unified for all examples)
func GenerateOrderCSV(config DataGeneratorConfig) string {
	contextLogger := doris.NewContextLogger(config.ContextName)
	contextLogger.Infof("Generating %d order records...", config.BatchSize)
	start := time.Now()

	// Pre-allocate builder for memory efficiency
	estimatedSize := config.BatchSize * 200 // ~200 bytes per record
	var builder strings.Builder
	builder.Grow(estimatedSize)

	// Worker-specific random seed to avoid collisions
	seed := config.RandomSeed
	if seed == 0 {
		seed = time.Now().UnixNano() + int64(config.WorkerID*1000)
	}
	rng := rand.New(rand.NewSource(seed))
	baseOrderID := config.WorkerID * config.BatchSize

	for i := 1; i <= config.BatchSize; i++ {
		quantity := rng.Intn(10) + 1
		unitPrice := float64(rng.Intn(50000)) / 100.0 // $0.01 to $500.00
		totalAmount := float64(quantity) * unitPrice

		record := OrderRecord{
			OrderID:     baseOrderID + i,
			CustomerID:  rng.Intn(100000) + 1,
			ProductName: fmt.Sprintf("Product_%s_%d", Brands[rng.Intn(len(Brands))], rng.Intn(1000)),
			Category:    Categories[rng.Intn(len(Categories))],
			Brand:       Brands[rng.Intn(len(Brands))],
			Quantity:    quantity,
			UnitPrice:   unitPrice,
			TotalAmount: totalAmount,
			Status:      Statuses[rng.Intn(len(Statuses))],
			OrderDate:   time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour).Format("2006-01-02 15:04:05"),
			Region:      Regions[rng.Intn(len(Regions))],
		}

		// CSV format: order_id,customer_id,product_name,category,brand,quantity,unit_price,total_amount,status,order_date,region
		builder.WriteString(fmt.Sprintf("%d,%d,\"%s\",%s,%s,%d,%.2f,%.2f,%s,%s,%s\n",
			record.OrderID,
			record.CustomerID,
			record.ProductName,
			record.Category,
			record.Brand,
			record.Quantity,
			record.UnitPrice,
			record.TotalAmount,
			record.Status,
			record.OrderDate,
			record.Region,
		))

		// Progress indicator for large datasets
		if i%10000 == 0 {
			contextLogger.Infof("Generated %d/%d records (%.1f%%)", i, config.BatchSize, float64(i)/float64(config.BatchSize)*100)
		}
	}

	generationTime := time.Since(start)
	dataSize := builder.Len()
	contextLogger.Infof("Order data generation completed: %d records, %d bytes, took %v", config.BatchSize, dataSize, generationTime)
	contextLogger.Infof("Generation rate: %.0f records/sec, %.1f MB/sec",
		float64(config.BatchSize)/generationTime.Seconds(),
		float64(dataSize)/1024/1024/generationTime.Seconds())

	return builder.String()
}

// GenerateOrderJSON creates realistic order data in JSON Lines format (unified schema)
func GenerateOrderJSON(config DataGeneratorConfig) string {
	contextLogger := doris.NewContextLogger(config.ContextName)
	contextLogger.Infof("Generating %d JSON order records...", config.BatchSize)
	start := time.Now()

	// Pre-allocate builder for memory efficiency
	estimatedSize := config.BatchSize * 300 // JSON records are larger
	var builder strings.Builder
	builder.Grow(estimatedSize)

	// Worker-specific random seed
	seed := config.RandomSeed
	if seed == 0 {
		seed = time.Now().UnixNano() + int64(config.WorkerID*1000)
	}
	rng := rand.New(rand.NewSource(seed))
	baseOrderID := config.WorkerID * config.BatchSize

	for i := 1; i <= config.BatchSize; i++ {
		quantity := rng.Intn(10) + 1
		unitPrice := float64(rng.Intn(50000)) / 100.0 // $0.01 to $500.00
		totalAmount := float64(quantity) * unitPrice

		record := OrderRecord{
			OrderID:     baseOrderID + i,
			CustomerID:  rng.Intn(100000) + 1,
			ProductName: fmt.Sprintf("Product_%s_%d", Brands[rng.Intn(len(Brands))], rng.Intn(1000)),
			Category:    Categories[rng.Intn(len(Categories))],
			Brand:       Brands[rng.Intn(len(Brands))],
			Quantity:    quantity,
			UnitPrice:   unitPrice,
			TotalAmount: totalAmount,
			Status:      Statuses[rng.Intn(len(Statuses))],
			OrderDate:   time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour).Format("2006-01-02T15:04:05Z"),
			Region:      Regions[rng.Intn(len(Regions))],
		}

		// Manual JSON construction for better control
		jsonRecord := fmt.Sprintf(`{"OrderID":%d,"CustomerID":%d,"ProductName":"%s","Category":"%s","Brand":"%s","Quantity":%d,"UnitPrice":%.2f,"TotalAmount":%.2f,"Status":"%s","OrderDate":"%s","Region":"%s"}`,
			record.OrderID,
			record.CustomerID,
			record.ProductName,
			record.Category,
			record.Brand,
			record.Quantity,
			record.UnitPrice,
			record.TotalAmount,
			record.Status,
			record.OrderDate,
			record.Region,
		)

		builder.WriteString(jsonRecord)
		builder.WriteString("\n") // JSON lines format

		// Progress indicator for large datasets
		if i%10000 == 0 {
			contextLogger.Infof("Generated %d/%d JSON records (%.1f%%)", i, config.BatchSize, float64(i)/float64(config.BatchSize)*100)
		}
	}

	generationTime := time.Since(start)
	dataSize := builder.Len()
	contextLogger.Infof("JSON order data generation completed: %d records, %d bytes, took %v", config.BatchSize, dataSize, generationTime)
	contextLogger.Infof("Generation rate: %.0f records/sec, %.1f MB/sec",
		float64(config.BatchSize)/generationTime.Seconds(),
		float64(dataSize)/1024/1024/generationTime.Seconds())

	return builder.String()
}

// GenerateSimpleOrderCSV creates simple order data for basic examples
func GenerateSimpleOrderCSV(workerID int) string {
	quantity := 1
	unitPrice := 10.0 + float64(workerID)
	totalAmount := float64(quantity) * unitPrice

	return fmt.Sprintf("order_id,customer_id,product_name,category,brand,quantity,unit_price,total_amount,status,order_date,region\n%d,%d,\"Product_%d\",Electronics,TestBrand,%d,%.2f,%.2f,active,2024-01-01 12:00:00,Central\n",
		workerID,
		workerID+1000,
		workerID,
		quantity,
		unitPrice,
		totalAmount)
}
