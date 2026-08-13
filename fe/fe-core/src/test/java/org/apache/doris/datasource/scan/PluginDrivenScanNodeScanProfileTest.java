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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.datasource.split.SplitAssignment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FIX-SCAN-METRICS — guards {@link PluginDrivenScanNode#writeScanProfilesInto}, the connector-agnostic
 * transcription of connector-supplied scan diagnostics into the query profile execution summary. WHY it
 * matters: the plugin migration dropped the paimon/iceberg SDK scan metrics (manifest cache hit/miss, scan
 * durations) from the profile; this generic writer restores them without the engine knowing any connector
 * specifics — it only get-or-creates a group and transcribes the connector's labels + metric strings.
 */
public class PluginDrivenScanNodeScanProfileTest {

    private static final ConnectorTableHandle HANDLE = new ConnectorTableHandle() {
    };

    private static final class RacingRuntimeProfile extends RuntimeProfile {
        private final AtomicInteger unsynchronizedAdds = new AtomicInteger();
        private final CountDownLatch firstAddEntered = new CountDownLatch(1);
        private final CountDownLatch secondAddEntered = new CountDownLatch(1);

        RacingRuntimeProfile(String name) {
            super(name);
        }

        @Override
        public void addChild(RuntimeProfile child, boolean indent) {
            // The fixed implementation invokes addChild while holding the summary monitor. For the old
            // unsynchronized lookup/add sequence, force both writers past their null lookup before either add.
            if (!Thread.holdsLock(this)) {
                int call = unsynchronizedAdds.incrementAndGet();
                if (call == 1) {
                    firstAddEntered.countDown();
                    await(secondAddEntered);
                } else if (call == 2) {
                    secondAddEntered.countDown();
                }
            }
            super.addChild(child, indent);
        }

        private static void await(CountDownLatch latch) {
            try {
                Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static final class CloseTrackingSplitSource implements ConnectorSplitSource {
        private final AtomicBoolean closed;

        CloseTrackingSplitSource(AtomicBoolean closed) {
            this.closed = closed;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ConnectorScanRange next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static ConnectorScanProfile profile(String group, String label, String... kv) {
        Map<String, String> metrics = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            metrics.put(kv[i], kv[i + 1]);
        }
        return new ConnectorScanProfile(group, label, metrics);
    }

    @Test
    public void nullOrEmptyIsNoOp() {
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, null);
        PluginDrivenScanNode.writeScanProfilesInto(summary, Collections.emptyList());
        Assertions.assertTrue(summary.getChildMap().isEmpty(), "no profiles -> no group written");
        // null summary must not throw.
        PluginDrivenScanNode.writeScanProfilesInto(null, Collections.singletonList(profile("G", "L")));
    }

    @Test
    public void writesGroupChildAndMetrics() {
        // THE load-bearing RED assertion: one profile becomes a group -> "Table Scan (...)" child -> info
        // strings. A mutation that skips writing leaves the summary childless.
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, Collections.singletonList(
                profile("Paimon Scan Metrics", "Table Scan (db.t)",
                        "manifest_hit_cache", "4", "manifest_missed_cache", "1")));

        RuntimeProfile group = summary.getChildMap().get("Paimon Scan Metrics");
        Assertions.assertNotNull(group, "group must be created");
        RuntimeProfile scan = group.getChildMap().get("Table Scan (db.t)");
        Assertions.assertNotNull(scan, "per-scan child must be created");
        Assertions.assertEquals("4", scan.getInfoString("manifest_hit_cache"));
        Assertions.assertEquals("1", scan.getInfoString("manifest_missed_cache"));
    }

    @Test
    public void sharesGroupAcrossScans() {
        // Two scans of the same connector go under ONE get-or-created group as two children (a join over two
        // paimon tables must not create two "Paimon Scan Metrics" groups).
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        PluginDrivenScanNode.writeScanProfilesInto(summary, Arrays.asList(
                profile("Iceberg Scan Metrics", "Table Scan (db.a)", "data_files", "3"),
                profile("Iceberg Scan Metrics", "Table Scan (db.b)", "data_files", "5")));

        RuntimeProfile group = summary.getChildMap().get("Iceberg Scan Metrics");
        Assertions.assertNotNull(group);
        Assertions.assertEquals(2, group.getChildMap().size(), "one group, two scan children");
        Assertions.assertEquals("3", group.getChildMap().get("Table Scan (db.a)").getInfoString("data_files"));
        Assertions.assertEquals("5", group.getChildMap().get("Table Scan (db.b)").getInfoString("data_files"));
    }

    @Test
    public void concurrentStreamingScansShareOneGroup() throws Exception {
        RuntimeProfile summary = new RacingRuntimeProfile("Execution Summary");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> PluginDrivenScanNode.writeScanProfilesInto(
                    summary, Collections.singletonList(
                            profile("Iceberg Scan Metrics", "Table Scan (db.a)", "data_files", "1"))));
            Future<?> second = executor.submit(() -> PluginDrivenScanNode.writeScanProfilesInto(
                    summary, Collections.singletonList(
                            profile("Iceberg Scan Metrics", "Table Scan (db.b)", "data_files", "1"))));
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        RuntimeProfile group = summary.getChildMap().get("Iceberg Scan Metrics");
        Assertions.assertNotNull(group);
        Assertions.assertEquals(2, group.getChildMap().size(),
                "concurrent batch scans must not replace the shared profile group");
    }

    @Test
    public void streamingCompletionClosesCollectsWritesThenFinishes() throws Exception {
        AtomicBoolean sourceClosed = new AtomicBoolean(false);
        RuntimeProfile summary = new RuntimeProfile("Execution Summary");
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        Mockito.when(assignment.isStop()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(sourceClosed.get(), "the split source must close before completion");
            Assertions.assertNotNull(summary.getChildMap().get("Iceberg Scan Metrics"),
                    "profiles must be visible before finishSchedule publishes completion");
            return null;
        }).when(assignment).finishSchedule();
        PluginDrivenScanNode.StreamingSplitSourceHandle sourceHandle =
                new PluginDrivenScanNode.StreamingSplitSourceHandle();
        sourceHandle.attach(new CloseTrackingSplitSource(sourceClosed));

        PluginDrivenScanNode.completeStreamingSplit(sourceHandle, () -> {
            Assertions.assertTrue(sourceClosed.get(), "profile collection must run after source close");
            return Collections.singletonList(
                    profile("Iceberg Scan Metrics", "Table Scan (db.a)", "data_files", "1"));
        }, summary, assignment, null, HANDLE);

        Mockito.verify(assignment).finishSchedule();
        Mockito.verify(assignment, Mockito.never()).setException(Mockito.any());
    }

    @Test
    public void streamingProfileFailurePreservesOriginalScanFailure() throws IOException {
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        Mockito.when(assignment.isStop()).thenReturn(false);
        PluginDrivenScanNode.StreamingSplitSourceHandle sourceHandle =
                new PluginDrivenScanNode.StreamingSplitSourceHandle();
        sourceHandle.attach(new CloseTrackingSplitSource(new AtomicBoolean()));
        UserException scanFailure = new UserException("scan failed");

        PluginDrivenScanNode.completeStreamingSplit(sourceHandle, () -> {
            throw new IllegalStateException("profile failed");
        }, new RuntimeProfile("Execution Summary"), assignment, scanFailure, HANDLE);

        ArgumentCaptor<UserException> failure = ArgumentCaptor.forClass(UserException.class);
        Mockito.verify(assignment).setException(failure.capture());
        Assertions.assertSame(scanFailure, failure.getValue());
        Assertions.assertEquals(1, scanFailure.getSuppressed().length);
        Assertions.assertTrue(scanFailure.getSuppressed()[0].getMessage().contains("profile failed"));
        Mockito.verify(assignment, Mockito.never()).finishSchedule();
    }
}
