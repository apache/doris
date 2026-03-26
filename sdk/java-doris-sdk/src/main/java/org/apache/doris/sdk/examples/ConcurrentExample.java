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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-level concurrent large-scale data loading demo.
 * Loads 1,000,000 records using 10 concurrent threads, each loading 100,000 records.
 *
 * <p>Thread safety: DorisLoadClient is shared across all workers.
 * Each worker uses its own independent InputStream — never share streams across threads.
 *
 * Mirrors Go SDK's RunConcurrentExample.
 */
public class ConcurrentExample {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentExample.class);

    private static final int TOTAL_RECORDS = 1_000_000;
    private static final int NUM_WORKERS = 10;
    private static final int RECORDS_PER_WORKER = TOTAL_RECORDS / NUM_WORKERS;

    /** Per-worker result. */
    static class WorkerResult {
        final int workerId;
        final boolean success;
        final int recordsLoaded;
        final long dataSizeBytes;
        final long loadTimeMs;
        final String error;

        WorkerResult(int workerId, boolean success, int recordsLoaded,
                     long dataSizeBytes, long loadTimeMs, String error) {
            this.workerId = workerId;
            this.success = success;
            this.recordsLoaded = recordsLoaded;
            this.dataSizeBytes = dataSizeBytes;
            this.loadTimeMs = loadTimeMs;
            this.error = error;
        }
    }

    public static void run() {
        System.out.println("=== Production-Level Concurrent Large-Scale Loading Demo ===");
        System.out.printf("Scale: %d total records, %d workers, %d records per worker%n",
                TOTAL_RECORDS, NUM_WORKERS, RECORDS_PER_WORKER);

        // Single shared client — thread-safe
        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://localhost:8030"))
                .user("root")
                .password("")
                .database("test")
                .table("orders")
                .labelPrefix("prod_concurrent")
                .format(DorisConfig.defaultCsvFormat())
                .retry(RetryConfig.builder().maxRetryTimes(5).baseIntervalMs(1000).maxTotalTimeMs(60000).build())
                .groupCommit(GroupCommitMode.ASYNC)
                .build();

        try (DorisLoadClient client = DorisClient.newClient(config)) {
            System.out.println("Load client created successfully");

            AtomicLong totalRecordsLoaded = new AtomicLong(0);
            AtomicLong totalDataBytes = new AtomicLong(0);
            AtomicLong successWorkers = new AtomicLong(0);
            AtomicLong failedWorkers = new AtomicLong(0);

            ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
            List<Future<WorkerResult>> futures = new ArrayList<>();
            CountDownLatch startLatch = new CountDownLatch(1);

            System.out.printf("Launching %d concurrent workers...%n", NUM_WORKERS);
            long overallStart = System.currentTimeMillis();

            for (int i = 0; i < NUM_WORKERS; i++) {
                final int workerId = i;
                futures.add(executor.submit(() -> {
                    startLatch.await(); // all workers start simultaneously
                    return runWorker(workerId, client);
                }));
                // Small stagger to avoid thundering herd
                Thread.sleep(100);
            }

            // Release all workers
            startLatch.countDown();

            // Collect results
            List<WorkerResult> results = new ArrayList<>();
            for (Future<WorkerResult> f : futures) {
                WorkerResult r = f.get();
                results.add(r);
                if (r.success) {
                    totalRecordsLoaded.addAndGet(r.recordsLoaded);
                    totalDataBytes.addAndGet(r.dataSizeBytes);
                    successWorkers.incrementAndGet();
                } else {
                    failedWorkers.incrementAndGet();
                }
            }

            executor.shutdown();
            long overallTimeMs = System.currentTimeMillis() - overallStart;
            double overallTimeSec = overallTimeMs / 1000.0;

            // Summary
            System.out.println("\n=== CONCURRENT LOAD COMPLETE ===");
            System.out.printf("Total records loaded: %d/%d%n",
                    totalRecordsLoaded.get(), TOTAL_RECORDS);
            System.out.printf("Workers: %d successful, %d failed%n",
                    successWorkers.get(), failedWorkers.get());
            System.out.printf("Total time: %.2f s%n", overallTimeSec);
            System.out.printf("Overall rate: %.0f records/sec%n",
                    totalRecordsLoaded.get() / overallTimeSec);
            System.out.printf("Data processed: %.1f MB%n",
                    totalDataBytes.get() / 1024.0 / 1024.0);

        } catch (Exception e) {
            System.err.println("Concurrent load failed: " + e.getMessage());
        }

        System.out.println("=== Demo Complete ===");
    }

    private static WorkerResult runWorker(int workerId, DorisLoadClient client) {
        log.info("Worker-{} starting load of {} records", workerId, RECORDS_PER_WORKER);
        long workerStart = System.currentTimeMillis();

        // Each worker generates its own independent data — never share streams across threads
        String data = DataGenerator.generateOrderCSV(workerId, RECORDS_PER_WORKER);
        long dataSizeBytes = data.length();

        try {
            long loadStart = System.currentTimeMillis();
            InputStream inputStream = DorisClient.stringStream(data);
            LoadResponse response = client.load(inputStream);
            long loadTimeMs = System.currentTimeMillis() - loadStart;

            if (response.getStatus() == LoadResponse.Status.SUCCESS) {
                log.info("Worker-{} completed: {} records in {} ms",
                        workerId, RECORDS_PER_WORKER, loadTimeMs);
                return new WorkerResult(workerId, true, RECORDS_PER_WORKER,
                        dataSizeBytes, loadTimeMs, null);
            } else {
                log.error("Worker-{} failed: {}", workerId, response.getErrorMessage());
                return new WorkerResult(workerId, false, 0, dataSizeBytes, loadTimeMs,
                        response.getErrorMessage());
            }
        } catch (Exception e) {
            long loadTimeMs = System.currentTimeMillis() - workerStart;
            log.error("Worker-{} error: {}", workerId, e.getMessage());
            return new WorkerResult(workerId, false, 0, dataSizeBytes, loadTimeMs,
                    e.getMessage());
        }
    }
}
