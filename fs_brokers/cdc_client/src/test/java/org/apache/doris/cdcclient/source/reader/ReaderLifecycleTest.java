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

package org.apache.doris.cdcclient.source.reader;

import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_INCREMENTAL;
import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.JOB_SNAPSHOT;
import static org.apache.doris.cdcclient.common.CdcResourceManager.ReaderGroup.STANDALONE_TVF;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.relational.TableId;

import org.apache.doris.cdcclient.common.CdcResourceManager;
import org.apache.doris.cdcclient.common.ManagedReader;
import org.apache.doris.cdcclient.common.ReaderBatch;
import org.apache.doris.cdcclient.common.ReaderQuotaUnavailableException;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.apache.doris.cdcclient.source.reader.postgres.PostgresSourceReader;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;
import org.apache.doris.job.cdc.request.WriteRecordRequest;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.Fetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

@Isolated
class ReaderLifecycleTest {
    private final CdcResourceManager resources = Env.getCurrentEnv().getCdcResourceManager();

    @Test
    void canConfirmTerminationOfPostgresConnectorExecutors() {
        JdbcIncrementalSourceReader source = new PostgresSourceReader();
        JobBaseConfig config =
                new JobBaseConfig(
                        "123",
                        "POSTGRES",
                        Map.of(
                                DataSourceConfigKeys.JDBC_URL,
                                        "jdbc:postgresql://localhost:5432/testdb",
                                DataSourceConfigKeys.USER, "user",
                                DataSourceConfigKeys.PASSWORD, "pass",
                                DataSourceConfigKeys.DATABASE, "testdb",
                                DataSourceConfigKeys.SCHEMA, "public",
                                DataSourceConfigKeys.TABLE, "orders",
                                DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL),
                        null);
        assertTrue(
                JdbcIncrementalSourceReader.closeFetcher(source.getSnapshotSplitReader(config, 0))
                        .isTerminated());
        assertTrue(
                JdbcIncrementalSourceReader.closeFetcher(source.getBinlogSplitReader(config))
                        .isTerminated());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void incrementalReuseKeepsOneLeaseAndRebuildReplacesIt(boolean mysql) throws Exception {
        Fixture fixture = new Fixture(mysql);
        WriteRecordRequest request = request(mysql, 100);
        try {
            fixture.source.prepareAndSubmitSplit(request);
            long reserved = resources.getReaderBytes(JOB_INCREMENTAL);
            assertTrue(reserved > 0);
            fixture.source.prepareAndSubmitSplit(request);
            assertEquals(1, fixture.created.get());
            assertEquals(reserved, resources.getReaderBytes(JOB_INCREMENTAL));
            fixture.source.prepareAndSubmitSplit(request(mysql, 200));
            assertEquals(2, fixture.created.get());
            assertEquals(1, fixture.closed.get());
            assertEquals(reserved, resources.getReaderBytes(JOB_INCREMENTAL));
        } finally {
            fixture.source.release(request);
            fixture.source.release(request);
        }
        assertEquals(2, fixture.closed.get());
        assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void failedSubmissionClosesReaderEvenWhenReuseWasRequested(boolean mysql) {
        Fixture fixture = new Fixture(mysql);
        fixture.failSubmit = true;
        WriteRecordRequest request = request(mysql, 100);
        request.setReuseReader(true);
        try {
            assertThrows(Exception.class, () -> fixture.source.prepareAndSubmitSplit(request));
            assertEquals(1, fixture.closed.get());
            assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
        } finally {
            fixture.source.release(request);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void partialSnapshotCreationRetainsQuotaUntilStartedSubmitReturns(boolean mysql)
            throws Exception {
        Fixture fixture = new Fixture(mysql);
        fixture.failSecondCreation = true;
        fixture.submitStarted = new CountDownLatch(1);
        fixture.finishSubmit = new CountDownLatch(1);
        WriteRecordRequest request = request(mysql, 100);
        FutureTask<Void> task =
                new FutureTask<>(
                        () -> {
                            InvocationTargetException failure =
                                    assertThrows(
                                            InvocationTargetException.class,
                                            () -> prepareSnapshots(fixture, mysql, request, 2));
                            assertTrue(failure.getCause() instanceof IllegalStateException);
                            return null;
                        });
        new Thread(task).start();
        try {
            assertTrue(fixture.submitStarted.await(5, TimeUnit.SECONDS));
            task.get(5, TimeUnit.SECONDS);
            assertTrue(resources.getReaderBytes(JOB_SNAPSHOT) > 0);
            assertEquals(0, fixture.closed.get());
        } finally {
            fixture.finishSubmit.countDown();
            fixture.source.release(request);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (resources.getReaderBytes(JOB_SNAPSHOT) != 0 && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertEquals(1, fixture.closed.get());
        assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
    }

    @Test
    void ordinaryTvfAndJobTvfUseDifferentAccounts() {
        AbstractCdcSourceReader source = new MySqlSourceReader();
        FetchRecordRequest request = new FetchRecordRequest();
        request.setDataSource("MYSQL");
        request.setJobId("8a7f82fe-19bc-4b7c-a4d0-adc3d0667aac");
        request.setTaskId("user-supplied-task");
        try (ManagedReader<Object> standalone =
                source.createIncrementalReader(
                        request, Object::new, ignored -> terminatedWorker())) {
            long bytes = resources.getReaderBytes(STANDALONE_TVF);
            assertTrue(bytes > 0);
            request.setJobId("123");
            try (ManagedReader<Object> job =
                    source.createIncrementalReader(
                            request, Object::new, ignored -> terminatedWorker())) {
                assertEquals(bytes, resources.getReaderBytes(STANDALONE_TVF));
                assertEquals(bytes, resources.getReaderBytes(JOB_INCREMENTAL));
            }
        }
        assertEquals(0, resources.getReaderBytes(STANDALONE_TVF));
        assertEquals(0, resources.getReaderBytes(JOB_INCREMENTAL));
    }

    @Test
    void exhaustedQuotaRejectsStandaloneTvfWithoutOpeningAReader() {
        Fixture fixture = new Fixture(true);
        FetchRecordRequest tvf = new FetchRecordRequest();
        tvf.setJobId("8a7f82fe-19bc-4b7c-a4d0-adc3d0667aac");
        try (CdcResourceManager.SnapshotQuota occupied =
                resources.newSnapshotQuota("holder", JOB_SNAPSHOT)) {
            assertTrue(occupied.tryReserveSlots(Integer.MAX_VALUE) > 0);
            InvocationTargetException rejected =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> prepareSnapshots(fixture, true, tvf, 2));
            assertTrue(rejected.getCause() instanceof ReaderQuotaUnavailableException);
            assertEquals(0, fixture.created.get());
        } finally {
            fixture.source.release(tvf);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void finishedSplitLetsNextSplitStartWhileSiblingIsStillReading(boolean mysql) throws Exception {
        Fixture fixture = new Fixture(mysql);
        fixture.emitWatermarks = true;
        WriteRecordRequest request = request(mysql, 100);
        int slots =
                (int)
                        (resources.getReaderMemoryQuotaBytes()
                                / resources.getReaderReservationBytes());
        assertTrue(slots >= 2, "The test JVM must allow at least two readers");
        try (CdcResourceManager.SnapshotQuota occupied =
                resources.newSnapshotQuota("holder", STANDALONE_TVF)) {
            assertEquals(slots - 2, occupied.tryReserveSlots(slots - 2));
            try {
                SplitReadResult result = prepareSnapshots(fixture, mysql, request, 5);
                assertEquals(5, result.getSplits().size());
                assertEquals(2, fixture.created.get());
                await().atMost(Duration.ofSeconds(5))
                        .until(
                                () -> {
                                    pollWatermarks(fixture);
                                    return fixture.created.get() == 5;
                                });
                assertTrue(fixture.closed.get() >= 3);
                assertFalse(fixture.source.isSnapshotFinished());
                fixture.finishFirstSplit.set(true);
                await().atMost(Duration.ofSeconds(5))
                        .until(
                                () -> {
                                    pollWatermarks(fixture);
                                    return fixture.source.isSnapshotFinished();
                                });
                assertEquals(5, fixture.closed.get());
                assertEquals(0, resources.getReaderBytes(JOB_SNAPSHOT));
            } finally {
                fixture.source.release(request);
            }
        }
    }

    private static void pollWatermarks(Fixture fixture) throws Exception {
        try (ReaderBatch<?> batch = fixture.source.pollRecords()) {
            assertFalse(batch.hasNext(), "Watermarks are consumed by the reader");
        }
    }

    private static SplitReadResult prepareSnapshots(
            Fixture fixture, boolean mysql, JobBaseRecordRequest request, int count)
            throws Exception {
        TableId table = TableId.parse("test.orders");
        List<Object> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String splitId = "orders:" + i;
            splits.add(
                    mysql
                            ? new MySqlSnapshotSplit(
                                    table,
                                    splitId,
                                    new RowType(List.of()),
                                    null,
                                    null,
                                    null,
                                    Map.of())
                            : new org.apache.flink.cdc.connectors.base.source.meta.split
                                    .SnapshotSplit(
                                    table,
                                    splitId,
                                    new RowType(List.of()),
                                    null,
                                    null,
                                    null,
                                    Map.of()));
        }
        Method prepare =
                (mysql ? MySqlSourceReader.class : JdbcIncrementalSourceReader.class)
                        .getDeclaredMethod(
                                "prepareSnapshotSplits", List.class, JobBaseRecordRequest.class);
        prepare.setAccessible(true);
        return (SplitReadResult) prepare.invoke(fixture.source, splits, request);
    }

    private static WriteRecordRequest request(boolean mysql, long offset) {
        WriteRecordRequest request = new WriteRecordRequest();
        request.setJobId("123");
        request.setTaskId("task");
        request.setDataSource(mysql ? "MYSQL" : "POSTGRES");
        request.setConfig(
                mysql
                        ? Map.of(
                                DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://localhost:3306/test",
                                DataSourceConfigKeys.USER, "user",
                                DataSourceConfigKeys.PASSWORD, "password",
                                DataSourceConfigKeys.DATABASE, "test",
                                DataSourceConfigKeys.INCLUDE_TABLES, "orders",
                                DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL)
                        : Map.of());
        request.setMeta(
                Map.of(
                        "splitId",
                        "binlog-split",
                        "startingOffset",
                        mysql
                                ? Map.of("file", "binlog.000001", "pos", Long.toString(offset))
                                : Map.of("lsn", Long.toString(offset))));
        return request;
    }

    private static ExecutorService terminatedWorker() {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        worker.shutdown();
        return worker;
    }

    private static class Fixture {
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        final AbstractCdcSourceReader source;
        final AtomicBoolean finishFirstSplit = new AtomicBoolean();
        boolean emitWatermarks;
        boolean failSubmit;
        boolean failSecondCreation;
        CountDownLatch submitStarted;
        CountDownLatch finishSubmit;

        Fixture(boolean mysql) {
            source =
                    mysql
                            ? new MySqlSourceReader() {
                                @Override
                                protected SnapshotSplitReader getSnapshotSplitReader(
                                        JobBaseConfig request, int index) {
                                    return fakeReader(true, true);
                                }

                                @Override
                                protected <R> ManagedReader<R> createIncrementalReader(
                                        JobBaseRecordRequest request,
                                        Supplier<R> factory,
                                        Function<R, ExecutorService> closer) {
                                    return super.createIncrementalReader(
                                            request, () -> fakeReader(true, false), closer);
                                }
                            }
                            : new PostgresSourceReader() {
                                @Override
                                protected IncrementalSourceScanFetcher getSnapshotSplitReader(
                                        JobBaseConfig request, int index) {
                                    return fakeReader(false, true);
                                }

                                @Override
                                protected <R> ManagedReader<R> createIncrementalReader(
                                        JobBaseRecordRequest request,
                                        Supplier<R> factory,
                                        Function<R, ExecutorService> closer) {
                                    return super.createIncrementalReader(
                                            request,
                                            () -> fakeReader(false, false),
                                            reader -> {
                                                ((Fetcher<?, ?>) reader).close();
                                                return terminatedWorker();
                                            });
                                }

                                @Override
                                protected void validateStreamSource(
                                        Offset offset, JobBaseRecordRequest request) {}

                                @Override
                                protected FetchTask<SourceSplitBase> createFetchTaskFromSplit(
                                        JobBaseConfig request, SourceSplitBase split) {
                                    return new FetchTask<>() {
                                        public void execute(Context context) {}

                                        public boolean isRunning() {
                                            return false;
                                        }

                                        public SourceSplitBase getSplit() {
                                            return split;
                                        }

                                        public void close() {}
                                    };
                                }
                            };
            source.setTableSchemas(new HashMap<>());
        }

        @SuppressWarnings("unchecked")
        private <R> R fakeReader(boolean mysql, boolean snapshot) {
            int index = created.getAndIncrement();
            if (index == 1 && failSecondCreation) {
                try {
                    assertTrue(submitStarted.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new IllegalStateException("second creation failed");
            }
            if (!mysql && snapshot) {
                return (R)
                        new IncrementalSourceScanFetcher(null, 0) {
                            @Override
                            public void submitTask(FetchTask<SourceSplitBase> task) {
                                submitted();
                            }

                            @Override
                            public Iterator<SourceRecords> pollSplitRecords() {
                                SourceRecord watermark = watermark(index);
                                return watermark == null
                                        ? null
                                        : List.of(new SourceRecords(List.of(watermark))).iterator();
                            }

                            @Override
                            public void close() {
                                getExecutorService().shutdownNow();
                                closed.incrementAndGet();
                            }
                        };
            }
            if (!mysql) {
                return (R)
                        new Fetcher<SourceRecords, SourceSplitBase>() {
                            public void submitTask(FetchTask<SourceSplitBase> task) {
                                submitted();
                            }

                            public Iterator<SourceRecords> pollSplitRecords() {
                                return null;
                            }

                            public boolean isFinished() {
                                return false;
                            }

                            public void close() {
                                closed.incrementAndGet();
                            }
                        };
            }
            if (snapshot) {
                return (R)
                        new SnapshotSplitReader((StatefulTaskContext) null, 0) {
                            @Override
                            public void submitSplit(MySqlSplit split) {
                                submitted();
                            }

                            @Override
                            public Iterator<
                                            org.apache.flink.cdc.connectors.mysql.source.split
                                                    .SourceRecords>
                                    pollSplitRecords() {
                                SourceRecord event = watermark(index);
                                if (event == null) {
                                    return null;
                                }
                                SourceRecord mysqlEvent =
                                        new SourceRecord(
                                                event.sourcePartition(),
                                                Map.of("file", "binlog.000001", "pos", 100),
                                                event.topic(),
                                                event.keySchema(),
                                                event.key(),
                                                event.valueSchema(),
                                                event.value());
                                return List.of(
                                                new org.apache.flink.cdc.connectors.mysql.source
                                                        .split.SourceRecords(List.of(mysqlEvent)))
                                        .iterator();
                            }

                            @Override
                            public void close() {
                                getExecutorService().shutdownNow();
                                closed.incrementAndGet();
                            }
                        };
            }
            MySqlSourceConfig config =
                    new MySqlSourceConfigFactory()
                            .hostname("localhost")
                            .username("user")
                            .password("password")
                            .databaseList("test")
                            .tableList("test.orders")
                            .serverTimeZone("UTC")
                            .createConfig(0);
            return (R)
                    new BinlogSplitReader(config, 0) {
                        @Override
                        public void submitSplit(MySqlSplit split) {
                            submitted();
                        }

                        @Override
                        public void close() {
                            getExecutorService().shutdownNow();
                            closed.incrementAndGet();
                        }
                    };
        }

        private SourceRecord watermark(int index) {
            if (!emitWatermarks || (index == 0 && !finishFirstSplit.get())) {
                return null;
            }
            return WatermarkEvent.create(
                    Map.of(),
                    "test",
                    "orders:" + index,
                    WatermarkKind.HIGH,
                    PostgresOffset.INITIAL_OFFSET);
        }

        private void submitted() {
            if (failSubmit) {
                throw new IllegalStateException("submit failed");
            }
            if (submitStarted != null) {
                submitStarted.countDown();
                try {
                    assertTrue(finishSubmit.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
