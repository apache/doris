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
import org.apache.doris.sdk.load.config.RetryConfig;
import org.apache.doris.sdk.load.model.LoadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Production-level single-threaded large batch loading demo.
 * Loads 100,000 CSV order records in a single stream load call.
 * Mirrors Go SDK's RunSingleBatchExample.
 */
public class SingleBatchExample {

    private static final Logger log = LoggerFactory.getLogger(SingleBatchExample.class);
    private static final int BATCH_SIZE = 100_000;

    public static void run() {
        System.out.println("=== Production-Level Large Batch Loading Demo ===");
        log.info("Starting large batch loading demo with {} order records", BATCH_SIZE);

        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://localhost:8030"))
                .user("root")
                .password("")
                .database("test")
                .table("orders")
                .labelPrefix("prod_batch")
                .format(DorisConfig.defaultCsvFormat())
                .retry(RetryConfig.builder().maxRetryTimes(3).baseIntervalMs(2000).maxTotalTimeMs(60000).build())
                .groupCommit(GroupCommitMode.OFF)
                .build();

        try (DorisLoadClient client = DorisClient.newClient(config)) {
            log.info("Load client created successfully");

            // Generate large batch of realistic order data
            log.info("Generating {} order records...", BATCH_SIZE);
            String data = DataGenerator.generateOrderCSV(0, BATCH_SIZE);
            log.info("Data generated: {} MB", data.length() / 1024.0 / 1024.0);

            log.info("Starting load operation for {} order records...", BATCH_SIZE);
            long loadStart = System.currentTimeMillis();

            LoadResponse response = client.load(DorisClient.stringStream(data));

            long loadTimeMs = System.currentTimeMillis() - loadStart;
            double loadTimeSec = loadTimeMs / 1000.0;

            if (response.getStatus() == LoadResponse.Status.SUCCESS) {
                double sizeMB = data.length() / 1024.0 / 1024.0;
                System.out.println("Load completed successfully!");
                System.out.printf("Records: %d, Size: %.1f MB, Time: %.2f s%n", BATCH_SIZE, sizeMB, loadTimeSec);
                System.out.printf("Rate: %.0f records/sec, %.1f MB/sec%n",
                        BATCH_SIZE / loadTimeSec, sizeMB / loadTimeSec);
                System.out.printf("Label: %s, Loaded: %d rows%n",
                        response.getRespContent().getLabel(),
                        response.getRespContent().getNumberLoadedRows());
            } else {
                System.err.println("Load failed: " + response.getErrorMessage());
            }
        } catch (Exception e) {
            System.err.println("Load failed: " + e.getMessage());
        }

        System.out.println("=== Demo Complete ===");
    }
}
