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
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Opt-in scale acceptance; use the planning-only checkpoint fixture, never scan its absent data objects. */
@EnabledIfSystemProperty(named = "deltalake.planning.table", matches = ".+")
class DeltaLakePlanningScaleTest {
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
                    Assertions.assertTrue(provider.streamingSplitEstimate(session, handle, Optional.empty(), false) >= 0);
                    long gateDone = System.nanoTime();
                    long files = 0;
                    long bytes = 0;
                    long firstSplit = 0;
                    long peakHeap = 0;
                    try (ConnectorSplitSource source = provider.streamSplits(session, handle, List.of(),
                            Optional.empty(), -1)) {
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
