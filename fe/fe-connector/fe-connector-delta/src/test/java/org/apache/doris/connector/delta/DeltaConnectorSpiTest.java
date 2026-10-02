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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.thrift.TConnectorFileCommitData;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

/** Exercises the live SPI entry points, not the plugin's Kernel convenience methods. */
public class DeltaConnectorSpiTest {
    @TempDir
    Path directory;

    @Test
    public void testProviderAcceptsTheDeltaEngineName() {
        Assertions.assertEquals(java.util.Set.of("delta"),
                new DeltaConnectorProvider().acceptedCreateTableEngineNames());
    }

    @Test
    public void testHandleExposesItsPinnedCopyOnWriteSnapshotVersion() {
        DeltaTableHandle handle = new DeltaTableHandle("default", "events", "file:///delta/events", 7);
        Assertions.assertEquals(OptionalLong.of(7), handle.getCopyOnWriteSnapshotVersion());
        Assertions.assertEquals(OptionalLong.of(8), handle.withSnapshotVersion(8).getCopyOnWriteSnapshotVersion());
        Assertions.assertEquals(OptionalLong.of(7), handle.getCopyOnWriteSnapshotVersion());
    }

    @Test
    public void testStatementsPinReadWriteAndHistoricalVersions() throws Exception {
        DeltaConnector connector = createConnector("statement-pins");
        TestSession statement = new TestSession("first");
        ConnectorMetadata metadata = connector.getMetadata(statement);
        ConnectorTableHandle base = metadata.getTableHandle(statement, "default", "events").orElseThrow();
        ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(statement, base).orElseThrow();
        Assertions.assertEquals(0, pin.getSnapshotId());
        Assertions.assertTrue(DeltaStatementScope.TABLE_NAMESPACE.startsWith(new DeltaConnectorProvider().getType()));

        TestSession writer = new TestSession("external-write");
        append(connector, writer, "first.parquet", 3);
        Assertions.assertSame(base, metadata.getTableHandle(statement, "default", "events").orElseThrow());
        Assertions.assertTrue(connector.getScanPlanProvider().planScan(statement,
                ConnectorScanRequest.builder(metadata.applySnapshot(statement, base, pin), List.of()).build()).isEmpty());
        Assertions.assertEquals(connector.getWritePlanProvider()
                .getWriteColumns(statement, base, Optional.empty()).orElseThrow(),
                metadata.getTableSchema(statement, base, pin).getColumns());

        TestSession next = new TestSession("next");
        ConnectorMetadata nextMetadata = connector.getMetadata(next);
        Assertions.assertNotSame(metadata, nextMetadata);
        ConnectorTableHandle latest = nextMetadata.getTableHandle(next, "default", "events").orElseThrow();
        Assertions.assertEquals(1, nextMetadata.beginQuerySnapshot(next, latest).orElseThrow().getSnapshotId());
        ConnectorMvccSnapshot historical = nextMetadata.resolveTimeTravel(next, latest,
                ConnectorTimeTravelSpec.snapshotId("0")).orElseThrow();
        Assertions.assertEquals(0, historical.getSchemaId());
        Assertions.assertEquals(2, nextMetadata.getColumnHandles(next, latest, historical).size());
        Assertions.assertTrue(nextMetadata.supportsColumnHandleSnapshotPin(next));
        Assertions.assertTrue(connector.getScanPlanProvider().planScan(next, ConnectorScanRequest
                .builder(nextMetadata.applySnapshot(next, latest, historical), List.of()).build()).isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> nextMetadata.resolveTimeTravel(next, latest, ConnectorTimeTravelSpec.branch("main")));
        metadata.close();
        metadata.close();
        nextMetadata.close();
    }

    @Test
    public void testWritePlanCommitsDeduplicatedReportsAndTracksOverwriteRows() throws Exception {
        DeltaConnector connector = createConnector("transaction");
        TestSession session = new TestSession("append");
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle table = metadata.getTableHandle(session, "default", "events").orElseThrow();
        ConnectorTransaction transaction = metadata.beginTransaction(session, table);
        session.setCurrentTransaction(transaction);
        ConnectorSinkPlan plan = connector.getWritePlanProvider().planWrite(
                session, writeHandle(metadata, session, table, false));
        Assertions.assertTrue(plan.getDataSink().getHiveTableSink().isConnectorFileSink());
        Assertions.assertFalse(plan.getDataSink().getHiveTableSink().isOverwrite());
        Assertions.assertTrue(connector.getWritePlanProvider().supportsCopyOnWriteDml());

        byte[] report = report(directory.resolve("transaction/first.parquet"), 3);
        transaction.addCommitData(report);
        transaction.addCommitData(report);
        Assertions.assertEquals(3, transaction.getUpdateCnt());
        Assertions.assertEquals(OptionalLong.empty(), transaction.getOriginalRowCount());
        transaction.commit();
        transaction.close();
        transaction.close();
        DeltaTableHandle afterAppend = connector.getCatalogAdapter().getTableHandle("default", "events").orElseThrow();
        Assertions.assertEquals(1, afterAppend.getSnapshotVersion());
        Assertions.assertEquals(1, connector.getCatalogAdapter().loadSnapshot(afterAppend).getActiveFiles().size());

        TestSession overwrite = new TestSession("empty-overwrite");
        ConnectorMetadata overwriteMetadata = connector.getMetadata(overwrite);
        ConnectorTableHandle overwriteBase = overwriteMetadata.getTableHandle(
                overwrite, "default", "events").orElseThrow();
        ConnectorTransaction replacement = overwriteMetadata.beginTransaction(overwrite, overwriteBase);
        overwrite.setCurrentTransaction(replacement);
        ConnectorSinkPlan overwritePlan = connector.getWritePlanProvider().planWrite(
                overwrite, writeHandle(overwriteMetadata, overwrite, overwriteBase, true));
        Assertions.assertFalse(overwritePlan.getDataSink().getHiveTableSink().isOverwrite());
        Assertions.assertEquals(OptionalLong.of(3), replacement.getOriginalRowCount());
        Assertions.assertEquals(0, replacement.getUpdateCnt());
        // An actual empty replacement is legal. The FE execution counter/file-report guard decides whether
        // zero reports mean empty input or a missing BE report before it reaches transaction.commit().
        replacement.commit();
        replacement.close();
        DeltaTableHandle empty = connector.getCatalogAdapter().getTableHandle("default", "events").orElseThrow();
        Assertions.assertEquals(2, empty.getSnapshotVersion());
        Assertions.assertTrue(connector.getCatalogAdapter().loadSnapshot(empty).getActiveFiles().isEmpty());
    }

    @Test
    public void testConflictingReportsRollbackAndStaleBaseFailsBeforeWriting() throws Exception {
        DeltaConnector connector = createConnector("rejection");
        TestSession statement = new TestSession("bad-report");
        ConnectorMetadata metadata = connector.getMetadata(statement);
        ConnectorTableHandle original = metadata.getTableHandle(statement, "default", "events").orElseThrow();
        ConnectorTransaction transaction = metadata.beginTransaction(statement, original);
        statement.setCurrentTransaction(transaction);
        connector.getWritePlanProvider().planWrite(statement, writeHandle(metadata, statement, original, false));
        Path file = directory.resolve("rejection/aborted.parquet");
        transaction.addCommitData(report(file, 2));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> transaction.addCommitData(report(file, 3)));
        transaction.rollback();
        transaction.rollback();
        transaction.close();
        Assertions.assertEquals(0, connector.getCatalogAdapter().getTableHandle(
                "default", "events").orElseThrow().getSnapshotVersion());

        append(connector, new TestSession("concurrent-append"), "committed.parquet", 1);
        TestSession stale = new TestSession("stale-overwrite");
        ConnectorMetadata staleMetadata = connector.getMetadata(stale);
        ConnectorTransaction overwrite = staleMetadata.beginTransaction(stale, original);
        stale.setCurrentTransaction(overwrite);
        Assertions.assertThrows(DorisConnectorException.class, () -> connector.getWritePlanProvider()
                .planWrite(stale, writeHandle(staleMetadata, stale, original, true)));
        overwrite.rollback();
        overwrite.close();
        Assertions.assertEquals(1, connector.getCatalogAdapter().getTableHandle(
                "default", "events").orElseThrow().getSnapshotVersion());
    }

    @Test
    public void testDdlRejectsUnsupportedClausesAndTruncatePartition() throws Exception {
        DeltaConnector connector = createConnector("ddl");
        ConnectorMetadata metadata = connector.getMetadata(null);
        ConnectorTableHandle handle = metadata.getTableHandle(null, "default", "events").orElseThrow();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.truncateTable(null, handle, List.of("p1")));
        ConnectorCreateTableRequest bucketed = ConnectorCreateTableRequest.builder()
                .dbName("default").tableName("events").columns(List.of(column("id")))
                .bucketSpec(new ConnectorBucketSpec(List.of("id"), 2, "doris_default")).build();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> metadata.createTable(null, bucketed));
        Assertions.assertEquals(0, ((DeltaTableHandle) handle).getSnapshotVersion());
    }

    private DeltaConnector createConnector(String name) {
        DeltaConnector connector = new DeltaConnectorProvider().create(Map.of(
                "type", "delta", DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_PATH,
                DeltaConnectorProperties.DATABASE, "default", DeltaConnectorProperties.TABLE, "events",
                DeltaConnectorProperties.TABLE_PATH, directory.resolve(name).toUri().toString(),
                DeltaConnectorProperties.WRITE_ENABLED, "true"), new ConnectorContext() {
                    @Override
                    public String getCatalogName() {
                        return "delta_spi_test";
                    }

                    @Override
                    public long getCatalogId() {
                        return 1;
                    }
                });
        connector.getMetadata(null).createTable(null, ConnectorCreateTableRequest.builder()
                .dbName("default").tableName("events").columns(List.of(column("id"), column("value"))).build());
        return connector;
    }

    private static ConnectorColumn column(String name) {
        return new ConnectorColumn(name, ConnectorType.of("BIGINT"), "", false, null);
    }

    private void append(DeltaConnector connector, TestSession session, String fileName, long rows) throws Exception {
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle table = metadata.getTableHandle(session, "default", "events").orElseThrow();
        ConnectorTransaction transaction = metadata.beginTransaction(session, table);
        session.setCurrentTransaction(transaction);
        connector.getWritePlanProvider().planWrite(session, writeHandle(metadata, session, table, false));
        Path tablePath = Path.of(java.net.URI.create(((DeltaTableHandle) table).getTablePath()));
        transaction.addCommitData(report(tablePath.resolve(fileName), rows));
        transaction.commit();
        transaction.close();
        metadata.close();
    }

    private static byte[] report(Path file, long rows) throws Exception {
        Files.write(file, new byte[] {1, 2, 3});
        TConnectorFileCommitData data = new TConnectorFileCommitData(file.toUri().toString(), rows,
                Files.size(file), 1234L);
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(data);
    }

    private static ConnectorWriteHandle writeHandle(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle table, boolean overwrite) {
        List<ConnectorColumn> columns = metadata.getTableSchema(session, table).getColumns();
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return table;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return columns;
            }

            @Override
            public boolean isOverwrite() {
                return overwrite;
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return Map.of();
            }

            @Override
            public WriteOperation getWriteOperation() {
                return overwrite ? WriteOperation.OVERWRITE : WriteOperation.INSERT;
            }
        };
    }

    private static final class TestSession implements ConnectorSession {
        private final String queryId;
        private ConnectorTransaction transaction;
        private final ConnectorStatementScope scope = new ConnectorStatementScope() {
            private final Map<String, Object> values = new HashMap<>();

            @Override
            @SuppressWarnings("unchecked")
            public <T> T computeIfAbsent(String key, Supplier<T> loader) {
                return (T) values.computeIfAbsent(key, ignored -> loader.get());
            }
        };

        private TestSession(String queryId) {
            this.queryId = queryId;
        }

        @Override
        public String getQueryId() {
            return queryId;
        }

        @Override
        public String getUser() {
            return "root";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1;
        }

        @Override
        public String getCatalogName() {
            return "delta_spi_test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Map.of();
        }

        @Override
        public long allocateTransactionId() {
            return 1000L;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }

        @Override
        public Optional<ConnectorTransaction> getCurrentTransaction() {
            return Optional.ofNullable(transaction);
        }

        @Override
        public void setCurrentTransaction(ConnectorTransaction transaction) {
            this.transaction = transaction;
        }
    }
}
