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

package org.apache.doris.benchmark;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Small dependency-free harness for opt-in FE microbenchmarks. */
public final class BenchmarkHarness {
    private static final long WARMUP_MILLIS = Long.getLong("benchmark.warmup.millis", 500L);
    private static final long MEASUREMENT_MILLIS = Long.getLong("benchmark.measurement.millis", 500L);
    private static final int MEASUREMENT_ITERATIONS = Integer.getInteger("benchmark.iterations", 3);
    private static final boolean PRINT_RESULT = Boolean.getBoolean("benchmark.print.result");
    private static volatile Object sink;

    private BenchmarkHarness() {
    }

    @FunctionalInterface
    public interface Operation {
        Object run() throws Exception;
    }

    public static void measure(String name, TimeUnit outputUnit, Operation operation) throws Exception {
        runWindow(operation, WARMUP_MILLIS);
        double totalNanosPerOperation = 0.0D;
        long totalOperations = 0L;
        for (int iteration = 0; iteration < MEASUREMENT_ITERATIONS; iteration++) {
            Window result = runWindow(operation, MEASUREMENT_MILLIS);
            totalNanosPerOperation += result.nanosPerOperation;
            totalOperations += result.operations;
        }
        double averageNanos = totalNanosPerOperation / MEASUREMENT_ITERATIONS;
        String result = PRINT_RESULT ? ", result=" + sink : "";
        System.out.printf(Locale.ROOT, "%-72s %12.3f %s/op  (%d ops%s)%n",
                name, convertFromNanos(averageNanos, outputUnit), unitName(outputUnit), totalOperations, result);
    }

    private static Window runWindow(Operation operation, long minimumMillis) throws Exception {
        long start = System.nanoTime();
        long deadline = start + TimeUnit.MILLISECONDS.toNanos(minimumMillis);
        long operations = 0L;
        do {
            sink = operation.run();
            operations++;
        } while (System.nanoTime() < deadline);
        long elapsed = System.nanoTime() - start;
        return new Window(operations, (double) elapsed / operations);
    }

    private static double convertFromNanos(double nanos, TimeUnit outputUnit) {
        if (outputUnit == TimeUnit.NANOSECONDS) {
            return nanos;
        } else if (outputUnit == TimeUnit.MICROSECONDS) {
            return nanos / 1_000.0D;
        } else if (outputUnit == TimeUnit.MILLISECONDS) {
            return nanos / 1_000_000.0D;
        } else if (outputUnit == TimeUnit.SECONDS) {
            return nanos / 1_000_000_000.0D;
        }
        throw new IllegalArgumentException("unsupported benchmark time unit: " + outputUnit);
    }

    private static String unitName(TimeUnit outputUnit) {
        if (outputUnit == TimeUnit.NANOSECONDS) {
            return "ns";
        } else if (outputUnit == TimeUnit.MICROSECONDS) {
            return "us";
        } else if (outputUnit == TimeUnit.MILLISECONDS) {
            return "ms";
        } else if (outputUnit == TimeUnit.SECONDS) {
            return "s";
        }
        return outputUnit.name().toLowerCase(Locale.ROOT);
    }

    private static final class Window {
        private final long operations;
        private final double nanosPerOperation;

        private Window(long operations, double nanosPerOperation) {
            this.operations = operations;
            this.nanosPerOperation = nanosPerOperation;
        }
    }
}
