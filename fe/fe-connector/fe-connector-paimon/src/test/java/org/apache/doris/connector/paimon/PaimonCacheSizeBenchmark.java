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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.apache.paimon.partition.Partition;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Offline microbenchmark, not JMH. Uses the production partition collector with recording catalog fakes:
 * no RPC, filesystem access, cache-hit timing, or data-file scans. Compares collection alone against collection
 * plus weighted-view construction; publication-only timing includes its list copy. Prepared-weight timing is
 * just the provider callback, including loop/volatile-sink overhead, not the complete cache-admission path.
 *
 * <p>Compile via run-fe-ut.sh; run this main with the module's test classpath and the FE's java.lang/java.util
 * opens. Run multiple fresh JVMs before drawing conclusions. Raw estimates are reported, not asserted to be
 * exact retained heap. TAIL_END hits the former 16/5-element sampling positions; TAIL_INTERIOR misses both.
 */
public final class PaimonCacheSizeBenchmark {
    private static final int WARMUP_WINDOWS = 5;
    private static final int MEASURE_WINDOWS = 15;
    private static final long MIN_WINDOW_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static volatile Object blackhole;

    private PaimonCacheSizeBenchmark() {
    }

    enum Distribution {
        UNIFORM,
        TAIL_END,
        TAIL_INTERIOR
    }

    public static void main(String[] args) {
        for (int fieldCount : new int[] {10, 100}) {
            for (int partitionCount : new int[] {1_000, 10_000}) {
                for (Distribution distribution : Distribution.values()) {
                    Fixture fixture = new Fixture(fieldCount, partitionCount, distribution);
                    List<ConnectorPartitionInfo> partitions = fixture.load(false);
                    PaimonPartitionView prepared = new PaimonPartitionView(fixture.key, partitions);
                    if (!prepared.getSizeEstimate().isComplete()) {
                        throw new IllegalStateException("Incomplete benchmark estimate: "
                                + prepared.getSizeEstimate().getIncompleteReason());
                    }
                    String label = "fields=" + fieldCount + " partitions=" + partitionCount
                            + " distribution=" + distribution;
                    System.out.printf("%s estimated_bytes=%d large_value_chars=%d%n", label,
                            prepared.getSizeEstimate().getBytes(), distribution == Distribution.UNIFORM
                                    ? 0 : Fixture.LARGE_VALUE_CHARS);
                    Map<String, Supplier<?>> cases = new LinkedHashMap<>();
                    cases.put("collect", () -> fixture.load(false));
                    cases.put("collect_weighted", () -> fixture.load(true));
                    cases.put("publish_only", () -> new PaimonPartitionView(fixture.key, partitions));
                    cases.put("prepared_weight", () -> PaimonPartitionViewSizeEstimator
                            .estimateEntry(fixture.key, prepared));
                    measure(label, cases);
                }
            }
        }
    }

    static final class Fixture {
        static final int LARGE_VALUE_CHARS = 1024 * 1024;
        final ConnectorTableKey key = new ConnectorTableKey("db1", "t1", 1L, -1L);
        private final RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        private final PaimonConnectorMetadata metadata;
        private final PaimonTableHandle handle;

        Fixture(int fieldCount, int partitionCount, Distribution distribution) {
            RowType.Builder schema = RowType.builder().field("region", DataTypes.STRING());
            for (int field = 1; field < fieldCount; field++) {
                schema.field("column_" + field, DataTypes.STRING());
            }
            FakePaimonTable table = new FakePaimonTable("t1", schema.build(),
                    Collections.singletonList("region"), Collections.emptyList());
            table.setOptions(Collections.singletonMap("partition.legacy-name", "true"));
            ops.table = table;
            ops.partitions = new ArrayList<>(partitionCount);
            int largeIndex = distribution == Distribution.TAIL_END ? partitionCount - 1
                    : distribution == Distribution.TAIL_INTERIOR ? partitionCount - 2 : -1;
            for (int index = 0; index < partitionCount; index++) {
                String value = "region-" + index;
                if (index == largeIndex) {
                    value += "x".repeat(LARGE_VALUE_CHARS);
                }
                ops.partitions.add(new Partition(Collections.singletonMap("region", value),
                        1L, 1L, 1, 1L, true));
            }
            metadata = new PaimonConnectorMetadata(ops, PaimonCatalogProperties.of(Collections.emptyMap()),
                    new RecordingConnectorContext());
            handle = new PaimonTableHandle("db1", "t1", Collections.singletonList("region"),
                    Collections.emptyList());
            handle.setPaimonTable(table);
        }

        List<ConnectorPartitionInfo> load(boolean weighted) {
            // Bound the recording fake's log in BOTH paths; never measure an ever-growing fixture.
            ops.log.clear();
            List<ConnectorPartitionInfo> partitions = metadata.listPartitions(null, handle, Optional.empty());
            // Mirrors cachedPartitions' post-collection branch, without timing cache lookup or admission locks.
            return weighted ? new PaimonPartitionView(key, partitions) : partitions;
        }
    }

    private static void measure(String label, Map<String, Supplier<?>> cases) {
        List<String> names = new ArrayList<>(cases.keySet());
        List<Supplier<?>> operations = new ArrayList<>(cases.values());
        int[] batchSizes = new int[operations.size()];
        long[][] samples = new long[operations.size()][MEASURE_WINDOWS];
        for (int index = 0; index < operations.size(); index++) {
            int batchSize = 1;
            while (runWindow(operations.get(index), batchSize) < MIN_WINDOW_NANOS && batchSize < 1 << 20) {
                batchSize *= 2;
            }
            batchSizes[index] = batchSize;
        }
        for (int window = 0; window < WARMUP_WINDOWS + MEASURE_WINDOWS; window++) {
            // Rotate case order to avoid always measuring the weighted path after the baseline's allocations.
            for (int offset = 0; offset < operations.size(); offset++) {
                int index = (window + offset) % operations.size();
                long elapsed = runWindow(operations.get(index), batchSizes[index]);
                if (window >= WARMUP_WINDOWS) {
                    samples[index][window - WARMUP_WINDOWS] = elapsed / batchSizes[index];
                }
            }
        }
        for (int index = 0; index < operations.size(); index++) {
            Arrays.sort(samples[index]);
            System.out.printf("%s operation=%s median_ns_op=%d min_ns_op=%d max_ns_op=%d operations=%d%n",
                    label, names.get(index), samples[index][MEASURE_WINDOWS / 2], samples[index][0],
                    samples[index][MEASURE_WINDOWS - 1], (long) batchSizes[index] * MEASURE_WINDOWS);
        }
    }

    private static long runWindow(Supplier<?> operation, int count) {
        long start = System.nanoTime();
        for (int index = 0; index < count; index++) {
            // Make allocated results escape; prepared-weight results intentionally retain the same estimate.
            blackhole = operation.get();
        }
        return System.nanoTime() - start;
    }
}
