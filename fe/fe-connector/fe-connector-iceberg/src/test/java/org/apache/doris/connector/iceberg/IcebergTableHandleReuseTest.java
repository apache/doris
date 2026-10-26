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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergTableHandleReuseTest {
    private static final TableIdentifier ID = TableIdentifier.of("db1", "t1");
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static ConnectorSession session(String query, ConnectorStatementScope scope) {
        return new IcebergStatementScopeTest.ScopeSession(7, query, scope);
    }

    private static RecordingIcebergCatalogOps ops() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable("t1", SCHEMA, PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());
        ops.tableExists = true;
        return ops;
    }

    private static IcebergConnectorMetadata metadata(RecordingIcebergCatalogOps ops, IcebergTableCache cache) {
        return new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext(), new IcebergLatestSnapshotCache(0L, 1), cache);
    }

    private static long existenceCalls(RecordingIcebergCatalogOps ops) {
        return ops.log.stream().filter(s -> s.startsWith("tableExists:")).count();
    }

    @Test
    void ddlHandleDoesNotRequireLocalScanPlanning() {
        RecordingIcebergCatalogOps ops = ops();
        Table serverPlanned = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
                new Class<?>[] {Table.class, SupportsDistributedScanPlanning.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allowDistributedPlanning")) {
                        return false;
                    }
                    throw new AssertionError("DDL must not read scan metadata: " + method.getName());
                });
        ops.table = serverPlanned;
        IcebergConnectorMetadata metadata = metadata(ops, null);
        ConnectorSession session = session("ddl", new TestStatementScope());

        RuntimeException scanFailure = Assertions.assertThrows(RuntimeException.class,
                () -> metadata.getTableHandle(session, "db1", "t1"));
        Assertions.assertTrue(scanFailure.getMessage().contains("server-side scan planning"));
        Assertions.assertTrue(metadata.tableExists(session, "db1", "t1"));
        int loads = (int) ops.log.stream().filter(s -> s.startsWith("loadTable:")).count();
        ConnectorTableHandle handle = metadata.getTableHandleForDdl(session, "db1", "t1").orElseThrow();
        Assertions.assertEquals(loads, ops.log.stream().filter(s -> s.startsWith("loadTable:")).count());
        metadata.dropTable(session, handle);
        Assertions.assertTrue(ops.log.stream().anyMatch(s -> s.startsWith("dropTable:")));
    }

    @Test
    void loadedStatementTableProvesExistenceWithoutSharedCache() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergConnectorMetadata metadata = metadata(ops, null);
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = session("q1", scope);
        ConnectorTableHandle handle = metadata.getTableHandle(session, "db1", "t1").orElseThrow();
        metadata.getTableSchema(session, handle);
        ops.tableExistsFailure = new IllegalStateException("must not contact remote after pinning");
        ConnectorTableHandle again = metadata.getTableHandle(session, "db1", "t1").orElseThrow();
        Assertions.assertNotSame(handle, again, "scan handles remain independent of snapshot/pushdown refinements");
        Assertions.assertEquals(0, existenceCalls(ops));
        ops.loadTableFailure = new IllegalStateException("new statement must load independently");
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata.getTableHandle(session("q2", scope), "db1", "t1"));
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata.getTableHandle(new IcebergStatementScopeTest.ScopeSession(8, "q1", scope),
                        "db1", "t1"));
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata.getTableHandle(session, "db1", "other"));
        scope.closeAll();
    }

    @Test
    void cacheHitAvoidsRemoteAcrossStatementsAndInvalidationKeepsOnlyExistingReader() {
        AtomicInteger closes = new AtomicInteger();
        IcebergTableCache cache = new IcebergTableCache(3600, 10, table -> closes::incrementAndGet);
        RecordingIcebergCatalogOps ops = ops();
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope first = new TestStatementScope();
        ConnectorSession reader = session("q1", first);
        metadata.getTableSchema(reader, new IcebergTableHandle("db1", "t1"));
        Assertions.assertTrue(cache.contains(ID));
        Assertions.assertTrue(metadata.getTableHandle(session("q2", new TestStatementScope()),
                "db1", "t1").isPresent());
        Assertions.assertEquals(0, existenceCalls(ops));
        cache.invalidate(ID);
        ops.tableExists = false;
        ops.throwNoSuchTableOnLoadTable = true;
        Assertions.assertFalse(cache.contains(ID));
        Assertions.assertTrue(metadata.getTableHandle(reader, "db1", "t1").isPresent());
        Assertions.assertTrue(metadata.getTableHandle(session("q3", new TestStatementScope()),
                "db1", "t1").isEmpty());
        Assertions.assertEquals(0, closes.get(), "REFRESH cannot close an active reader's metadata/FileIO");
        first.closeAll();
        Assertions.assertEquals(1, closes.get());
        Assertions.assertTrue(metadata.getTableHandle(reader, "db1", "t1").isEmpty());
        cache.close();
    }

    @Test
    void existenceChecksDoNotLoadTablesOrRetainPositiveNegativeOrFailedChecks() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        ConnectorSession session = session("ddl", new TestStatementScope());
        Assertions.assertTrue(metadata.tableExists(session, "db1", "t1"));
        ops.tableExists = false;
        ops.throwNoSuchTableOnLoadTable = true;
        Assertions.assertFalse(metadata.tableExists(session, "db1", "t1"));
        ops.tableExists = true; // e.g. CTAS resolves the same target after creation.
        Assertions.assertTrue(metadata.tableExists(session, "db1", "t1"));
        ops.tableExistsFailure = new IllegalStateException("permission denied");
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata.tableExists(session, "db1", "t1"));
        ops.tableExistsFailure = null;
        Assertions.assertTrue(metadata.tableExists(session, "db1", "t1"));
        Assertions.assertEquals(5, existenceCalls(ops));
        Assertions.assertFalse(ops.log.stream().anyMatch(s -> s.startsWith("loadTable:")));
        Assertions.assertEquals(0, cache.size());
        cache.close();
    }

    @Test
    void ddlExistenceRemainsLiveWithCachedAndPinnedReadMetadata() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession reader = session("reader", scope);
        metadata.getTableSchema(reader, new IcebergTableHandle("db1", "t1"));
        Assertions.assertTrue(cache.contains(ID));
        ops.tableExists = false;
        ops.throwNoSuchTableOnLoadTable = true;
        Assertions.assertFalse(metadata.tableExists(reader, "db1", "t1"));
        Assertions.assertFalse(metadata.tableExists(session("ddl", new TestStatementScope()), "db1", "t1"));
        Assertions.assertFalse(metadata.tableExists(null, "db1", "t1"));
        Assertions.assertTrue(metadata.getTableHandle(reader, "db1", "t1").isPresent(),
                "the existing reader keeps its frozen metadata independently of live DDL existence");
        Assertions.assertEquals(3, existenceCalls(ops));
        scope.closeAll();
        cache.close();
    }

    @Test
    void disabledCacheSharesOnlyTheCurrentStatementAndNoContextNeverRetainsProof() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergTableCache cache = new IcebergTableCache(0, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = session("q1", scope);
        metadata.getTableSchema(session, new IcebergTableHandle("db1", "t1"));
        Assertions.assertFalse(cache.contains(ID));
        ops.tableExists = false;
        ops.throwNoSuchTableOnLoadTable = true;
        Assertions.assertTrue(metadata.getTableHandle(session, "db1", "t1").isPresent());
        Assertions.assertTrue(metadata.getTableHandle(session("q2", new TestStatementScope()),
                "db1", "t1").isEmpty());
        Assertions.assertTrue(metadata.getTableHandle(null, "db1", "t1").isEmpty());
        Assertions.assertTrue(metadata.getTableHandle(session("offline", ConnectorStatementScope.NONE),
                "db1", "t1").isEmpty());
        scope.closeAll();
        cache.close();
    }

    @Test
    void refreshRetainsOldSchemaAndRefsWhileNewStatementSeesSchemaAndBranchMovement() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("handles", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(ID, SCHEMA);
        append(table, "first.parquet");
        long firstSnapshot = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("audit", firstSnapshot).createTag("old", firstSnapshot).commit();
        RecordingIcebergCatalogOps ops = ops();
        ops.table = table;
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope first = new TestStatementScope();
        ConnectorSession old = session("q1", first);
        ConnectorTableHandle oldHandle = metadata.getTableHandle(old, "db1", "t1").orElseThrow();
        // The cold handle must pin metadata before schema binding, not merely after getTableSchema.
        table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        append(table, "second.parquet");
        long secondSnapshot = table.currentSnapshot().snapshotId();
        table.manageSnapshots().replaceBranch("audit", secondSnapshot).commit();
        cache.invalidate(ID);
        Assertions.assertTrue(metadata.getTableHandle(old, "db1", "t1").isPresent());
        Assertions.assertEquals("INT", metadata.getTableSchema(old, oldHandle).getColumns().get(0)
                .getType().getTypeName());
        Assertions.assertEquals(firstSnapshot, metadata.resolveTimeTravel(old, oldHandle,
                ConnectorTimeTravelSpec.branch("audit")).orElseThrow().getSnapshotId());
        TestStatementScope next = new TestStatementScope();
        ConnectorSession fresh = session("q2", next);
        ConnectorTableHandle freshHandle = metadata.getTableHandle(fresh, "db1", "t1").orElseThrow();
        Assertions.assertEquals("BIGINT", metadata.getTableSchema(fresh, freshHandle).getColumns().get(0)
                .getType().getTypeName());
        Assertions.assertEquals(secondSnapshot, metadata.resolveTimeTravel(fresh, freshHandle,
                ConnectorTimeTravelSpec.branch("audit")).orElseThrow().getSnapshotId());
        Assertions.assertEquals(firstSnapshot, metadata.resolveTimeTravel(fresh,
                metadata.getTableHandle(fresh, "db1", "t1").orElseThrow(),
                ConnectorTimeTravelSpec.tag("old")).orElseThrow().getSnapshotId());
        Assertions.assertEquals(firstSnapshot, metadata.resolveTimeTravel(fresh, freshHandle,
                ConnectorTimeTravelSpec.snapshotId(Long.toString(firstSnapshot))).orElseThrow().getSnapshotId());
        Assertions.assertEquals(0, existenceCalls(ops), "the pinned load proves existence without a HEAD request");
        first.closeAll();
        next.closeAll();
        cache.close();
    }

    @Test
    void coldHandleLoadsOnceWithoutExistenceCheckAndMissingTableCanBeCreatedLater() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = session("cold", scope);
        ops.throwNoSuchTableOnLoadTable = true;
        Assertions.assertTrue(metadata.getTableHandle(session, "db1", "t1").isEmpty());
        ops.throwNoSuchTableOnLoadTable = false;
        ConnectorTableHandle handle = metadata.getTableHandle(session, "db1", "t1").orElseThrow();
        metadata.getTableSchema(session, handle);
        metadata.getTableHandle(session, "db1", "t1").orElseThrow();
        Assertions.assertEquals(0, existenceCalls(ops));
        Assertions.assertEquals(2, ops.log.stream().filter(call -> call.startsWith("loadTable:")).count(),
                "one failed load and one successful load; no negative entry or schema reload");
        scope.closeAll();
        cache.close();
    }

    @Test
    void coldHandleDoesNotHidePermissionFailureOrCacheItAsMissing() {
        RecordingIcebergCatalogOps ops = ops();
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergConnectorMetadata metadata = metadata(ops, cache);
        TestStatementScope scope = new TestStatementScope();
        ConnectorSession session = session("denied", scope);
        ops.loadTableFailure = new org.apache.iceberg.exceptions.ForbiddenException("denied");
        Assertions.assertThrows(RuntimeException.class, () -> metadata.getTableHandle(session, "db1", "t1"));
        ops.loadTableFailure = null;
        Assertions.assertTrue(metadata.getTableHandle(session, "db1", "t1").isPresent());
        Assertions.assertEquals(0, existenceCalls(ops));
        scope.closeAll();
        cache.close();
    }

    @Test
    void refreshedSharedSnapshotPinCannotOverrideAnAlreadyFrozenColdHandle() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("cold-pin-race", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(ID, SCHEMA);
        append(table, "first.parquet");
        long oldSnapshot = table.currentSnapshot().snapshotId();
        RecordingIcebergCatalogOps ops = ops();
        ops.table = table;
        IcebergTableCache cache = new IcebergTableCache(3600, 10);
        IcebergLatestSnapshotCache snapshots = new IcebergLatestSnapshotCache(3600, 10);
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(ops,
                IcebergCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext(), snapshots, cache);
        TestStatementScope oldScope = new TestStatementScope();
        TestStatementScope freshScope = new TestStatementScope();
        ConnectorSession old = session("old", oldScope);
        ConnectorSession fresh = session("fresh", freshScope);
        try {
            ConnectorTableHandle oldHandle = metadata.getTableHandle(old, "db1", "t1").orElseThrow();
            table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
            append(table, "second.parquet");
            long newSnapshot = table.currentSnapshot().snapshotId();
            cache.invalidate(ID);
            snapshots.invalidate(ID);
            ConnectorTableHandle freshHandle = metadata.getTableHandle(fresh, "db1", "t1").orElseThrow();
            Assertions.assertEquals(newSnapshot,
                    metadata.beginQuerySnapshot(fresh, freshHandle).orElseThrow().getSnapshotId());
            snapshots.getOrLoad(ID, () -> new IcebergLatestSnapshotCache.CachedSnapshot(
                    newSnapshot, table.schema().schemaId()));
            org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot oldPin =
                    metadata.beginQuerySnapshot(old, oldHandle).orElseThrow();
            Assertions.assertEquals(oldSnapshot, oldPin.getSnapshotId(),
                    "another query's refreshed pin cannot refer to a snapshot missing from the frozen old table");
            Assertions.assertEquals("INT", metadata.getTableSchema(old, oldHandle, oldPin)
                    .getColumns().get(0).getType().getTypeName());
            Assertions.assertEquals(newSnapshot, snapshots.getOrLoad(ID,
                    () -> {
                        throw new AssertionError("the refreshed shared pin must remain cached");
                    }).snapshotId);
        } finally {
            oldScope.closeAll();
            freshScope.closeAll();
            cache.close();
        }
    }

    private static void append(Table table, String path) {
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(path).withFormat("PARQUET")
                .withFileSizeInBytes(10).withRecordCount(1).build()).commit();
    }
}
