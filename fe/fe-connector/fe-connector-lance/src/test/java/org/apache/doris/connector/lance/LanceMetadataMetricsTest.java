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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceMetadataMetrics.Operation;
import org.apache.doris.connector.lance.LanceMetadataMetrics.Stage;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class LanceMetadataMetricsTest {

    /** A failed read counts once, as a call and a failure, with the stages it reached. */
    @Test
    void failedReadKeepsAccurateTotals() {
        LanceMetadataMetrics metrics = new LanceMetadataMetrics();
        IllegalStateException original = new IllegalStateException("read failed");
        LanceMetadataMetrics.Timer failedRead = metrics.start(Operation.METADATA_READ);
        Assertions.assertSame(original, Assertions.assertThrows(IllegalStateException.class,
                () -> failedRead.measure(Stage.DATASET_OPEN, () -> {
                    throw original;
                })));
        failedRead.close();
        failedRead.close();
        try (LanceMetadataMetrics.Timer read = metrics.start(Operation.METADATA_READ)) {
            Assertions.assertEquals("schema", read.measure(Stage.SCHEMA, () -> "schema"));
            read.succeeded();
        }
        try (LanceMetadataMetrics.Timer plan = metrics.start(Operation.SPLIT_PLANNING)) {
            plan.succeeded();
        }

        Map<String, String> values = metrics.drain();
        Assertions.assertEquals(Arrays.asList("MetadataReadTime", "MetadataReadCalls", "MetadataReadFailures",
                "SplitPlanningTime", "SplitPlanningCalls", "SplitPlanningFailures", "DatasetOpenTime",
                "DatasetOpenCalls", "SchemaReadTime", "SchemaReadCalls"), new ArrayList<>(values.keySet()));
        Assertions.assertEquals("2", values.get("MetadataReadCalls"));
        Assertions.assertEquals("1", values.get("MetadataReadFailures"));
        Assertions.assertEquals("1", values.get("SplitPlanningCalls"));
        Assertions.assertEquals("0", values.get("SplitPlanningFailures"));
        Assertions.assertEquals("1", values.get("DatasetOpenCalls"));
        Assertions.assertEquals("1", values.get("SchemaReadCalls"));
        // Drained: the next scan of the table reports only what comes after.
        Assertions.assertTrue(metrics.drain().isEmpty());
    }

    @Test
    void concurrentReadsAccumulateWithoutOverwritingOneAnother() throws Exception {
        LanceMetadataMetrics metrics = new LanceMetadataMetrics();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> work = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                work.add(executor.submit(() -> {
                    try (LanceMetadataMetrics.Timer read = metrics.start(Operation.METADATA_READ)) {
                        read.measure(Stage.SCHEMA, () -> "schema");
                        read.succeeded();
                    }
                }));
            }
            for (Future<?> future : work) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        Map<String, String> values = metrics.drain();
        Assertions.assertEquals("100", values.get("MetadataReadCalls"));
        Assertions.assertEquals("0", values.get("MetadataReadFailures"));
        Assertions.assertEquals("100", values.get("SchemaReadCalls"));
    }

    /**
     * The totals are per table and per statement, and each scan publishes and restarts its table's totals; the
     * engine collects the published profiles once.
     */
    @Test
    void eachScanPublishesItsTablesTotalsOnce() {
        LanceTestSession session = new LanceTestSession();
        LanceMetadataMetrics items = LanceMetadataMetrics.forTable(session, "default", "items");
        Assertions.assertSame(items, LanceMetadataMetrics.forTable(session, "default", "items"));
        Assertions.assertNotSame(items, LanceMetadataMetrics.forTable(session, "default", "other"));
        Assertions.assertNotSame(items, LanceMetadataMetrics.forTable(new LanceTestSession(), "default", "items"));
        try (LanceMetadataMetrics.Timer read = items.start(Operation.METADATA_READ)) {
            read.succeeded();
        }

        LanceMetadataMetrics.publishScan(session, "default", "items", items);
        List<ConnectorScanProfile> profiles = LanceMetadataMetrics.collectScanProfiles(session);
        Assertions.assertEquals(1, profiles.size());
        Assertions.assertEquals("Lance Metadata Metrics", profiles.get(0).getGroupName());
        Assertions.assertEquals("Table Scan (default.items)", profiles.get(0).getScanLabel());
        Assertions.assertEquals("1", profiles.get(0).getMetrics().get("MetadataReadCalls"));
        Assertions.assertTrue(LanceMetadataMetrics.collectScanProfiles(session).isEmpty());

        // A second scan of the table in the statement reports only what came after the first.
        LanceMetadataMetrics.publishScan(session, "default", "items", items);
        Assertions.assertTrue(LanceMetadataMetrics.collectScanProfiles(session).get(0).getMetrics().isEmpty());
    }

    /** Outside a live statement a read still runs and fails as it would, and nothing is ever published. */
    @Test
    void readsOutsideAStatementRunAndReportNothing() {
        LanceTestSession offline = LanceTestSession.withoutStatement();
        LanceMetadataMetrics metrics = LanceMetadataMetrics.forTable(offline, "default", "items");
        Assertions.assertNotSame(metrics, LanceMetadataMetrics.forTable(offline, "default", "items"));
        try (LanceMetadataMetrics.Timer read = metrics.start(Operation.METADATA_READ)) {
            Assertions.assertEquals(42, read.measure(Stage.SCHEMA, () -> 42));
            IllegalArgumentException failure = new IllegalArgumentException("bad metadata");
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                    () -> read.measure(Stage.INDEXES, () -> {
                        throw failure;
                    })));
        }
        LanceMetadataMetrics.publishScan(offline, "default", "items", metrics);
        Assertions.assertTrue(LanceMetadataMetrics.collectScanProfiles(offline).isEmpty());
        Assertions.assertNotSame(LanceMetadataMetrics.forTable(null, "default", "items"),
                LanceMetadataMetrics.forTable(null, "default", "items"));
    }

    @Test
    void timesAreRenderedAsTheProfileRendersNanosecondTimers() {
        Assertions.assertEquals("0ns", LanceMetadataMetrics.prettyNanos(0));
        Assertions.assertEquals("999ns", LanceMetadataMetrics.prettyNanos(999));
        Assertions.assertEquals("1.000us", LanceMetadataMetrics.prettyNanos(1_000));
        Assertions.assertEquals("12.005us", LanceMetadataMetrics.prettyNanos(12_005));
        Assertions.assertEquals("1.005ms", LanceMetadataMetrics.prettyNanos(1_005_000));
        Assertions.assertEquals("999.999ms", LanceMetadataMetrics.prettyNanos(999_999_999));
        Assertions.assertEquals("1sec0ms", LanceMetadataMetrics.prettyNanos(TimeUnit.SECONDS.toNanos(1)));
        Assertions.assertEquals("2sec5ms", LanceMetadataMetrics.prettyNanos(TimeUnit.MILLISECONDS.toNanos(2_005)));
        Assertions.assertEquals("1min1sec", LanceMetadataMetrics.prettyNanos(TimeUnit.MILLISECONDS.toNanos(61_500)));
        Assertions.assertEquals("1hour2min", LanceMetadataMetrics.prettyNanos(
                TimeUnit.MINUTES.toNanos(62) + TimeUnit.SECONDS.toNanos(3)));
    }
}
