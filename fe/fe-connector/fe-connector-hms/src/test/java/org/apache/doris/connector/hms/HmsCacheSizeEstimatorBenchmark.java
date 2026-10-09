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

package org.apache.doris.connector.hms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Dependency-free microbenchmark for the production HMS admission estimators. */
public final class HmsCacheSizeEstimatorBenchmark {
    private static final int WARMUP_WINDOWS = 5;
    private static final int MEASURE_WINDOWS = 15;
    private static volatile long blackhole;

    private HmsCacheSizeEstimatorBenchmark() {
    }

    public static void main(String[] args) {
        for (int size : new int[] {1_000, 100_000}) {
            List<String> names = partitionNames(size);
            int operationsPerWindow = size <= 1_000 ? 1_000 : 20;
            Result result = measure(
                    () -> HmsCacheSizeEstimator.estimatePartitionNames("db.t", names).getBytes(),
                    operationsPerWindow);
            System.out.printf("hms_partition_names=%d estimated_bytes=%d ns_op=%d operations=%d%n",
                    size, HmsCacheSizeEstimator.estimatePartitionNames("db.t", names).getBytes(),
                    result.medianNanos, result.operations);
        }

        HmsTableInfo table = skewedTable();
        Result tableResult = measure(
                () -> HmsCacheSizeEstimator.estimateTable("db.t", table).getBytes(), 1_000);
        System.out.printf("hms_table_properties=%d estimated_bytes=%d ns_op=%d operations=%d%n",
                table.getParameters().size(), HmsCacheSizeEstimator.estimateTable("db.t", table).getBytes(),
                tableResult.medianNanos, tableResult.operations);
    }

    private static List<String> partitionNames(int size) {
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add("dt=2026-09-01/hour=" + (i % 24) + "/bucket=" + i);
        }
        return values;
    }

    private static HmsTableInfo skewedTable() {
        Map<String, String> parameters = new LinkedHashMap<>();
        for (int i = 0; i < 99; i++) {
            parameters.put("small-" + i, "x");
        }
        parameters.put("large-tail", "x".repeat(2 * 1024 * 1024));
        return HmsTableInfo.builder()
                .dbName("db")
                .tableName("t")
                .parameters(parameters)
                .build();
    }

    private static Result measure(LongOperation operation, int operationsPerWindow) {
        for (int i = 0; i < WARMUP_WINDOWS; i++) {
            runWindow(operation, operationsPerWindow);
        }
        long[] nanosPerOperation = new long[MEASURE_WINDOWS];
        for (int i = 0; i < MEASURE_WINDOWS; i++) {
            long start = System.nanoTime();
            runWindow(operation, operationsPerWindow);
            nanosPerOperation[i] = (System.nanoTime() - start) / operationsPerWindow;
        }
        Arrays.sort(nanosPerOperation);
        return new Result(nanosPerOperation[MEASURE_WINDOWS / 2],
                (long) operationsPerWindow * MEASURE_WINDOWS);
    }

    private static void runWindow(LongOperation operation, int operations) {
        long value = 0L;
        for (int i = 0; i < operations; i++) {
            value ^= operation.run();
        }
        blackhole = value;
    }

    @FunctionalInterface
    private interface LongOperation {
        long run();
    }

    private static final class Result {
        private final long medianNanos;
        private final long operations;

        private Result(long medianNanos, long operations) {
            this.medianNanos = medianNanos;
            this.operations = operations;
        }
    }
}
