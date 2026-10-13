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

package org.apache.doris.connector.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dependency-free microbenchmark for bounded estimators versus an exact list traversal.
 * Run after test compilation with the module's target/classes and target/test-classes on the classpath.
 */
public final class MetaCacheEstimatorBenchmark {
    private static final int WARMUP_WINDOWS = 5;
    private static final int MEASURE_WINDOWS = 15;
    private static volatile long blackhole;

    private MetaCacheEstimatorBenchmark() {
    }

    public static void main(String[] args) {
        for (int size : new int[] {1_000, 100_000}) {
            List<String> values = fixture(size);
            Result sampled = measure(() -> ReflectiveObjectSizeEstimator.estimate(values), 10_000);
            Result typedSampled = measure(() -> JvmSizeUtils.sampledListPayload(
                    values, 16, JvmSizeUtils::stringSize), 10_000);
            int exactOperationsPerWindow = size <= 1_000 ? 1_000 : 20;
            Result exact = measure(() -> exactStringListSize(values), exactOperationsPerWindow);
            Result boundedAdmission = measure(
                    () -> completeOrRejected(values), exactOperationsPerWindow);
            System.out.printf(
                    "uniform_size=%d reflective_sampled_ns_op=%d typed_sampled_ns_op=%d "
                            + "exact_ns_op=%d bounded_admission_ns_op=%d reflective_sampled_ops=%d "
                            + "typed_sampled_ops=%d exact_ops=%d bounded_admission_ops=%d%n",
                    size, sampled.medianNanos, typedSampled.medianNanos, exact.medianNanos,
                    boundedAdmission.medianNanos, sampled.operations, typedSampled.operations,
                    exact.operations, boundedAdmission.operations);
        }

        Map<String, String> skewed = skewedFixture();
        long sampledBytes = ReflectiveObjectSizeEstimator.estimate(skewed);
        long completeBytes = ReflectiveObjectSizeEstimator.estimateComplete(skewed);
        Result sampled = measure(() -> ReflectiveObjectSizeEstimator.estimate(skewed), 10_000);
        Result complete = measure(() -> ReflectiveObjectSizeEstimator.estimateComplete(skewed), 1_000);
        System.out.printf(
                "skewed_map_size=%d sampled_bytes=%d complete_bytes=%d sampled_ns_op=%d "
                        + "complete_ns_op=%d sampled_ops=%d complete_ops=%d%n",
                skewed.size(), sampledBytes, completeBytes, sampled.medianNanos, complete.medianNanos,
                sampled.operations, complete.operations);
    }

    private static List<String> fixture(int size) {
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add("partition-" + i + "-value-0123456789");
        }
        return values;
    }

    private static long exactStringListSize(List<String> values) {
        long bytes = JvmSizeUtils.arrayListSize(values.size());
        for (String value : values) {
            bytes = JvmSizeUtils.saturatedAdd(bytes, JvmSizeUtils.stringSize(value));
        }
        return bytes;
    }

    private static long completeOrRejected(Object value) {
        try {
            return ReflectiveObjectSizeEstimator.estimateComplete(value);
        } catch (IllegalStateException expected) {
            return -1L;
        }
    }

    private static Map<String, String> skewedFixture() {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < 99; i++) {
            values.put("small-" + i, "x");
        }
        values.put("large-tail", "x".repeat(2 * 1024 * 1024));
        return values;
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
