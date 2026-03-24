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
import org.apache.doris.sdk.load.config.JsonFormat;
import org.apache.doris.sdk.load.config.RetryConfig;
import org.apache.doris.sdk.load.model.LoadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Production-level JSON data loading demo.
 * Loads 50,000 JSON Lines order records in a single stream load call.
 * Mirrors Go SDK's RunJSONExample.
 */
public class JsonExample {

    private static final Logger log = LoggerFactory.getLogger(JsonExample.class);
    private static final int JSON_BATCH_SIZE = 50_000;

    public static void run() {
        System.out.println("=== Production-Level JSON Data Loading Demo ===");
        log.info("Starting JSON loading demo with {} order records", JSON_BATCH_SIZE);

        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://localhost:8030"))
                .user("root")
                .password("")
                .database("test")
                .table("orders")
                .labelPrefix("prod_json")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .retry(RetryConfig.builder().maxRetryTimes(3).baseIntervalMs(2000).maxTotalTimeMs(60000).build())
                .groupCommit(GroupCommitMode.ASYNC)
                .build();

        try (DorisLoadClient client = DorisClient.newClient(config)) {
            log.info("JSON load client created successfully");

            // Generate realistic JSON Lines order data
            log.info("Generating {} JSON order records...", JSON_BATCH_SIZE);
            String jsonData = DataGenerator.generateOrderJSON(0, JSON_BATCH_SIZE);

            log.info("Starting JSON load operation for {} order records...", JSON_BATCH_SIZE);
            long loadStart = System.currentTimeMillis();

            LoadResponse response = client.load(DorisClient.stringStream(jsonData));

            long loadTimeMs = System.currentTimeMillis() - loadStart;
            double loadTimeSec = loadTimeMs / 1000.0;

            if (response.getStatus() == LoadResponse.Status.SUCCESS) {
                double sizeMB = jsonData.length() / 1024.0 / 1024.0;
                System.out.println("JSON load completed successfully!");
                System.out.printf("JSON Records: %d, Size: %.1f MB, Time: %.2f s%n",
                        JSON_BATCH_SIZE, sizeMB, loadTimeSec);
                System.out.printf("JSON Rate: %.0f records/sec, %.1f MB/sec%n",
                        JSON_BATCH_SIZE / loadTimeSec, sizeMB / loadTimeSec);
                System.out.printf("Label: %s, Loaded: %d rows%n",
                        response.getRespContent().getLabel(),
                        response.getRespContent().getNumberLoadedRows());
                if (response.getRespContent().getLoadBytes() > 0) {
                    double avgBytes = (double) response.getRespContent().getLoadBytes()
                            / response.getRespContent().getNumberLoadedRows();
                    System.out.printf("Average bytes per JSON record: %.1f%n", avgBytes);
                }
            } else {
                System.err.println("JSON load failed: " + response.getErrorMessage());
            }
        } catch (Exception e) {
            System.err.println("JSON load failed: " + e.getMessage());
        }

        System.out.println("=== JSON Demo Complete ===");
    }
}
