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

package org.apache.doris.sdk.examples;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Unified data generation utilities for all Doris Stream Load examples.
 * All examples use a unified Order schema for consistency.
 *
 * <p>Table DDL (orders):
 * <pre>
 * CREATE TABLE orders (
 *   order_id      BIGINT,
 *   customer_id   BIGINT,
 *   product_name  VARCHAR(200),
 *   category      VARCHAR(50),
 *   brand         VARCHAR(50),
 *   quantity      INT,
 *   unit_price    DECIMAL(10,2),
 *   total_amount  DECIMAL(10,2),
 *   status        VARCHAR(20),
 *   order_date    DATETIME,
 *   region        VARCHAR(20)
 * ) DISTRIBUTED BY HASH(order_id) BUCKETS 10
 * PROPERTIES (
 * "replication_allocation" = "tag.location.default: 1",
 * )
 * </pre>
 */
public class DataGenerator {

    private static final String[] CATEGORIES = {
            "Electronics", "Clothing", "Books", "Home", "Sports",
            "Beauty", "Automotive", "Food", "Health", "Toys"
    };

    private static final String[] BRANDS = {
            "Apple", "Samsung", "Nike", "Adidas", "Sony",
            "LG", "Canon", "Dell", "HP", "Xiaomi", "Huawei", "Lenovo"
    };

    private static final String[] STATUSES = {
            "active", "inactive", "pending", "discontinued", "completed", "cancelled"
    };

    private static final String[] REGIONS = {"North", "South", "East", "West", "Central"};

    /**
     * Generates order data in CSV format.
     *
     * @param workerID  worker ID (0 for single-threaded), used to offset order IDs
     * @param batchSize number of records to generate
     * @return CSV string (no header row)
     */
    public static String generateOrderCSV(int workerID, int batchSize) {
        long seed = System.nanoTime() + (long) workerID * 1000;
        Random rng = new Random(seed);
        int baseOrderID = workerID * batchSize;

        StringBuilder sb = new StringBuilder(batchSize * 200);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (int i = 1; i <= batchSize; i++) {
            int quantity = rng.nextInt(10) + 1;
            double unitPrice = (rng.nextInt(50000) + 1) / 100.0;
            double totalAmount = quantity * unitPrice;
            String productName = "Product_" + BRANDS[rng.nextInt(BRANDS.length)] + "_" + rng.nextInt(1000);
            String category = CATEGORIES[rng.nextInt(CATEGORIES.length)];
            String brand = BRANDS[rng.nextInt(BRANDS.length)];
            String status = STATUSES[rng.nextInt(STATUSES.length)];
            String region = REGIONS[rng.nextInt(REGIONS.length)];
            long offsetMs = (long) rng.nextInt(365 * 24 * 3600) * 1000L;
            String orderDate = sdf.format(new Date(System.currentTimeMillis() - offsetMs));

            sb.append(baseOrderID + i).append(",")
              .append(rng.nextInt(100000) + 1).append(",")
              .append("\"").append(productName).append("\"").append(",")
              .append(category).append(",")
              .append(brand).append(",")
              .append(quantity).append(",")
              .append(String.format("%.2f", unitPrice)).append(",")
              .append(String.format("%.2f", totalAmount)).append(",")
              .append(status).append(",")
              .append(orderDate).append(",")
              .append(region).append("\n");
        }
        return sb.toString();
    }

    /**
     * Generates order data in JSON Lines format (one JSON object per line).
     *
     * @param workerID  worker ID (0 for single-threaded)
     * @param batchSize number of records to generate
     * @return JSON Lines string
     */
    public static String generateOrderJSON(int workerID, int batchSize) {
        long seed = System.nanoTime() + (long) workerID * 1000;
        Random rng = new Random(seed);
        int baseOrderID = workerID * batchSize;

        StringBuilder sb = new StringBuilder(batchSize * 300);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        for (int i = 1; i <= batchSize; i++) {
            int quantity = rng.nextInt(10) + 1;
            double unitPrice = (rng.nextInt(50000) + 1) / 100.0;
            double totalAmount = quantity * unitPrice;
            String productName = "Product_" + BRANDS[rng.nextInt(BRANDS.length)] + "_" + rng.nextInt(1000);
            String category = CATEGORIES[rng.nextInt(CATEGORIES.length)];
            String brand = BRANDS[rng.nextInt(BRANDS.length)];
            String status = STATUSES[rng.nextInt(STATUSES.length)];
            String region = REGIONS[rng.nextInt(REGIONS.length)];
            long offsetMs = (long) rng.nextInt(365 * 24 * 3600) * 1000L;
            String orderDate = sdf.format(new Date(System.currentTimeMillis() - offsetMs));

            sb.append(String.format(
                    "{\"order_id\":%d,\"customer_id\":%d,\"product_name\":\"%s\","
                    + "\"category\":\"%s\",\"brand\":\"%s\",\"quantity\":%d,"
                    + "\"unit_price\":%.2f,\"total_amount\":%.2f,"
                    + "\"status\":\"%s\",\"order_date\":\"%s\",\"region\":\"%s\"}",
                    baseOrderID + i,
                    rng.nextInt(100000) + 1,
                    productName, category, brand, quantity,
                    unitPrice, totalAmount, status, orderDate, region));
            sb.append("\n");
        }
        return sb.toString();
    }
}
