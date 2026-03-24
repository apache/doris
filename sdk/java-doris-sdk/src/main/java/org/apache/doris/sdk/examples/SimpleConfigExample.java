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
import java.util.HashMap;
import java.util.Map;

/**
 * Simple configuration example — basic JSON Lines load with Group Commit ASYNC.
 * Mirrors Go SDK's SimpleConfigExample.
 */
public class SimpleConfigExample {

    public static void run() {
        Map<String, String> options = new HashMap<>();
        options.put("strict_mode", "true");
        options.put("max_filter_ratio", "0.1");

        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://localhost:8030"))
                .user("root")
                .password("")
                .database("test")
                .table("orders")
                .format(DorisConfig.defaultJsonFormat())
                .retry(DorisConfig.defaultRetry())
                .groupCommit(GroupCommitMode.ASYNC)
                .options(options)
                .build();

        try (DorisLoadClient client = DorisClient.newClient(config)) {
            String jsonData = "{\"order_id\": 1, \"customer_id\": 101, \"product_name\": \"Laptop\","
                    + " \"category\": \"Electronics\", \"brand\": \"Dell\", \"quantity\": 1,"
                    + " \"unit_price\": 999.99, \"total_amount\": 999.99, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-01 12:00:00\", \"region\": \"North\"}\n"
                    + "{\"order_id\": 2, \"customer_id\": 102, \"product_name\": \"Phone\","
                    + " \"category\": \"Electronics\", \"brand\": \"Samsung\", \"quantity\": 2,"
                    + " \"unit_price\": 499.99, \"total_amount\": 999.98, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-02 10:00:00\", \"region\": \"South\"}\n"
                    + "{\"order_id\": 3, \"customer_id\": 103, \"product_name\": \"Headphones\","
                    + " \"category\": \"Electronics\", \"brand\": \"Sony\", \"quantity\": 1,"
                    + " \"unit_price\": 199.99, \"total_amount\": 199.99, \"status\": \"active\","
                    + " \"order_date\": \"2026-01-03 09:00:00\", \"region\": \"East\"}\n";

            LoadResponse response = client.load(DorisClient.stringStream(jsonData));

            System.out.println("Load completed!");
            System.out.println("Status: " + response.getStatus());
            if (response.getStatus() == LoadResponse.Status.SUCCESS) {
                System.out.println("Loaded rows: " + response.getRespContent().getNumberLoadedRows());
                System.out.println("Load bytes: " + response.getRespContent().getLoadBytes());
            } else {
                System.out.println("Error: " + response.getErrorMessage());
            }
        } catch (Exception e) {
            System.err.println("Load failed: " + e.getMessage());
        }
    }
}
