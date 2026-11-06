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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Opt-in scale acceptance; use the planning-only checkpoint fixture, never scan its absent data objects. */
@EnabledIfSystemProperty(named = "deltalake.planning.table", matches = ".+")
class DeltaLakePlanningScaleTest {
    @Test
    void collectsOneMillionFileStatisticsWithoutBlockingTheColdLookup() throws Exception {
        Path table = Path.of(System.getProperty("deltalake.planning.table")).toAbsolutePath();
        try (Connector connector = new DeltaLakeConnectorProvider().create(
                Map.of("warehouse", table.getParent().getParent().toString()), new RecordingConnectorContext())) {
            TestingConnectorSession session = new TestingConnectorSession("statistics-scale");
            try (ConnectorMetadata metadata = connector.getMetadata(session)) {
                ConnectorTableHandle handle = metadata.getTableHandle(session,
                        table.getParent().getFileName().toString(), table.getFileName().toString()).orElseThrow();
                long started = System.nanoTime();
                Optional<ConnectorTableStatistics> result = metadata.getTableStatistics(session, handle);
                long coldReturned = System.nanoTime();
                long peakHeap = 0;
                while (result.isEmpty() || result.get().getRowCount() < 0) {
                    Assertions.assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(60),
                            "million-file statistics did not complete");
                    Thread.sleep(10);
                    peakHeap = Math.max(peakHeap, ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                    result = metadata.getTableStatistics(session, handle);
                }
                Assertions.assertEquals(1_000_000, result.get().getRowCount());
                Assertions.assertEquals(527_000_000, result.get().getDataSize());
                ConnectorScanPlanProvider provider = connector.getScanPlanProvider(handle);
                Assertions.assertEquals(1_000_000,
                        provider.streamingSplitEstimate(session, handle, Optional.empty(), false),
                        "completed statistics must keep large tables streaming");
                System.out.printf("DELTA_STATS_SCALE cold_ms=%.3f total_ms=%.3f rows=%d "
                                + "peak_heap_bytes=%d max_heap_bytes=%d%n", (coldReturned - started) / 1e6,
                        (System.nanoTime() - started) / 1e6, result.get().getRowCount(), peakHeap,
                        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
            }
        }
    }

    @Test
    void streamsOneMillionRangesInABoundedHeap() throws Exception {
        Path table = Path.of(System.getProperty("deltalake.planning.table")).toAbsolutePath();
        String database = table.getParent().getFileName().toString();
        String name = table.getFileName().toString();
        try (Connector connector = new DeltaLakeConnectorProvider().create(
                Map.of("warehouse", table.getParent().getParent().toString()), new RecordingConnectorContext())) {
            for (int run = 0; run < 3; run++) {
                TestingConnectorSession session = new TestingConnectorSession("scale-" + run,
                        Map.of("num_files_in_batch_mode", "1"));
                long started = System.nanoTime();
                try (ConnectorMetadata metadata = connector.getMetadata(session)) {
                    ConnectorTableHandle handle = metadata.getTableHandle(session, database, name).orElseThrow();
                    long snapshotDone = System.nanoTime();
                    ConnectorScanPlanProvider provider = connector.getScanPlanProvider(handle);
                    long estimate = provider.streamingSplitEstimate(session, handle, Optional.empty(), false);
                    Assertions.assertEquals(run == 0 ? 1 : 1_000_000, estimate,
                            "later queries reuse the complete count and remain streaming");
                    long gateDone = System.nanoTime();
                    long files = 0;
                    long bytes = 0;
                    long firstSplit = 0;
                    long peakHeap = 0;
                    try (ConnectorSplitSource source = provider.streamSplits(session,
                            ConnectorScanRequest.builder(handle, List.of()).build())) {
                        while (source.hasNext()) {
                            ConnectorScanRange range = source.next();
                            if (files == 0) {
                                firstSplit = System.nanoTime();
                            }
                            files++;
                            bytes += range.getLength();
                            if (files % 10000 == 0) {
                                peakHeap = Math.max(peakHeap,
                                        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                            }
                        }
                    }
                    long finished = System.nanoTime();
                    Assertions.assertEquals(1_000_000, files);
                    Assertions.assertEquals(527_000_000, bytes);
                    System.out.printf("DELTA_SCALE run=%d snapshot_ms=%.3f gate_ms=%.3f first_split_ms=%.3f "
                                    + "enumeration_ms=%.3f files=%d peak_heap_bytes=%d max_heap_bytes=%d%n",
                            run, (snapshotDone - started) / 1e6, (gateDone - snapshotDone) / 1e6,
                            (firstSplit - gateDone) / 1e6, (finished - gateDone) / 1e6, files, peakHeap,
                            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
                }
            }
        }
    }
}
