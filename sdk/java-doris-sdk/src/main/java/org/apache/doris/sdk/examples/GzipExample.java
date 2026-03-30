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

import org.apache.doris.sdk.DorisClient;
import org.apache.doris.sdk.load.DorisLoadClient;
import org.apache.doris.sdk.load.config.DorisConfig;
import org.apache.doris.sdk.load.config.GroupCommitMode;
import org.apache.doris.sdk.load.model.LoadResponse;

import java.util.Arrays;

/**
 * Gzip compression example.
 * The SDK compresses the request body transparently before sending.
 * No need to pre-compress the data — just set enableGzip(true).
 * Mirrors Go SDK's GzipExample.
 */
public class GzipExample {

    public static void run() {
        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://localhost:8030"))
                .user("root")
                .password("")
                .database("test")
                .table("orders")
                .format(DorisConfig.defaultJsonFormat())
                .retry(DorisConfig.defaultRetry())
                .groupCommit(GroupCommitMode.OFF)
                .enableGzip(true) // SDK compresses the body and sets compress_type=gz header automatically
                .build();

        try (DorisLoadClient client = DorisClient.newClient(config)) {
            String jsonData =
                    "{\"order_id\": 1001, \"customer_id\": 201, \"product_name\": \"Laptop\","
                    + " \"category\": \"Electronics\", \"brand\": \"Dell\", \"quantity\": 1,"
                    + " \"unit_price\": 999.99, \"total_amount\": 999.99, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-01 12:00:00\", \"region\": \"North\"}\n"
                    + "{\"order_id\": 1002, \"customer_id\": 202, \"product_name\": \"Phone\","
                    + " \"category\": \"Electronics\", \"brand\": \"Apple\", \"quantity\": 1,"
                    + " \"unit_price\": 799.99, \"total_amount\": 799.99, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-02 10:00:00\", \"region\": \"South\"}\n"
                    + "{\"order_id\": 1003, \"customer_id\": 203, \"product_name\": \"Tablet\","
                    + " \"category\": \"Electronics\", \"brand\": \"Samsung\", \"quantity\": 2,"
                    + " \"unit_price\": 349.99, \"total_amount\": 699.98, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-03 09:00:00\", \"region\": \"East\"}\n";

            LoadResponse response = client.load(DorisClient.stringStream(jsonData));

            System.out.println("Status: " + response.getStatus());
            if (response.getStatus() == LoadResponse.Status.SUCCESS) {
                System.out.println("Loaded rows: " + response.getRespContent().getNumberLoadedRows());
                System.out.println("Load bytes: " + response.getRespContent().getLoadBytes());
                System.out.println("Label: " + response.getRespContent().getLabel());
            } else {
                System.out.println("Message: " + response.getRespContent().getMessage());
                System.out.println("Error URL: " + response.getRespContent().getErrorUrl());
            }
        } catch (Exception e) {
            System.err.println("Load failed: " + e.getMessage());
        }
    }
}
