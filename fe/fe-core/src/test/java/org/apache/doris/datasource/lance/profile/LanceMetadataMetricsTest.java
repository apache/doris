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

package org.apache.doris.datasource.lance.profile;

import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics.Operation;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics.Stage;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LanceMetadataMetricsTest {
    @Test
    public void testCapturedProfileAndFailedReadKeepAccurateTotals() {
        ConnectContext previous = ConnectContext.get();
        SummaryProfile summary = new SummaryProfile();
        ConnectContext context = new ConnectContext();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getSummaryProfile()).thenReturn(summary);
        context.setExecutor(executor);
        context.setThreadLocalInfo();
        try {
            LanceMetadataMetrics failedRead = LanceMetadataMetrics.startMetadataRead();
            ConnectContext.remove(); // Publishing must use the profile captured at entry.
            IllegalStateException original = new IllegalStateException("read failed");
            Assertions.assertSame(original, Assertions.assertThrows(IllegalStateException.class,
                    () -> failedRead.measure(Stage.DATASET_OPEN, () -> {
                        throw original;
                    })));
            failedRead.close();
            failedRead.close();
            try (LanceMetadataMetrics read = new LanceMetadataMetrics(summary, Operation.METADATA_READ)) {
                Assertions.assertEquals("schema", read.measure(Stage.SCHEMA, () -> "schema"));
                read.succeeded();
            }
            try (LanceMetadataMetrics plan = new LanceMetadataMetrics(summary, Operation.SPLIT_PLANNING)) {
                plan.succeeded();
            }
            RuntimeProfile group = summary.getExecutionSummary().getChildMap().get(LanceMetadataMetrics.GROUP_NAME);
            Assertions.assertEquals(2, value(group, "MetadataReadCalls"));
            Assertions.assertEquals(1, value(group, "MetadataReadFailures"));
            Assertions.assertEquals(1, value(group, "DatasetOpenCalls"));
            Assertions.assertEquals(1, value(group, "SchemaReadCalls"));
            Assertions.assertEquals(1, value(group, "SplitPlanningCalls"));
            Assertions.assertEquals(0, value(group, "SplitPlanningFailures"));
            Assertions.assertFalse(group.getCounterMap().containsKey("IndexMetadataReadTime"));
            Assertions.assertEquals(TUnit.TIME_NS, group.getCounterMap().get("MetadataReadTime").getType());
            Assertions.assertEquals(TimeUnit.NANOSECONDS.toMillis(value(group, "MetadataReadTime"))
                    + TimeUnit.NANOSECONDS.toMillis(value(group, "SplitPlanningTime")),
                    summary.getExternalCatalogMetaTimeMs());
            Assertions.assertTrue(summary.getExecutionSummary().toString().contains("Lance Metadata Metrics"));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testConcurrentReadsAccumulateWithoutOverwritingOneAnother() throws Exception {
        SummaryProfile summary = new SummaryProfile();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> work = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                work.add(executor.submit(() -> {
                    try (LanceMetadataMetrics metrics = new LanceMetadataMetrics(summary, Operation.METADATA_READ)) {
                        metrics.measure(Stage.SCHEMA, () -> "schema");
                        metrics.succeeded();
                    }
                }));
            }
            for (Future<?> future : work) {
                future.get(10, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(1, summary.getExecutionSummary().getChildList().size());
            RuntimeProfile group = summary.getExecutionSummary().getChildMap().get(LanceMetadataMetrics.GROUP_NAME);
            Assertions.assertEquals(100, value(group, "MetadataReadCalls"));
            Assertions.assertEquals(100, value(group, "SchemaReadCalls"));
            Assertions.assertEquals(0, value(group, "MetadataReadFailures"));
            Assertions.assertEquals(TimeUnit.NANOSECONDS.toMillis(value(group, "MetadataReadTime")),
                    summary.getExternalCatalogMetaTimeMs());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReadWithoutQueryContextStillExecutesAndPropagatesFailure() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        try (LanceMetadataMetrics metrics = LanceMetadataMetrics.startMetadataRead()) {
            Assertions.assertSame(LanceMetadataMetrics.disabled(), metrics);
            Assertions.assertEquals(42, metrics.measure(Stage.SCHEMA, () -> 42));
            IllegalArgumentException failure = new IllegalArgumentException("bad metadata");
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                    () -> metrics.measure(Stage.INDEXES, () -> {
                        throw failure;
                    })));
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static long value(RuntimeProfile group, String counter) {
        return group.getCounterMap().get(counter).getValue();
    }
}
