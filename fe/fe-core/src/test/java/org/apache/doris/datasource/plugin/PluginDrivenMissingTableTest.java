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

package org.apache.doris.datasource.plugin;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableNotFoundException;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.metacache.ExternalMetaCache;
import org.apache.doris.datasource.metacache.FeMetaCacheEntry;
import org.apache.doris.datasource.metacache.IdNameIndex;
import org.apache.doris.datasource.metacache.NameCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Tests missing-table repair through real FE lookup/schema caches and a controllable connector. */
public class PluginDrivenMissingTableTest extends TestWithFeService {
    private static final AtomicLong IDS = new AtomicLong(910000);
    private final Map<String, String> remoteTables = new ConcurrentHashMap<>();
    private final Set<String> remoteViews = ConcurrentHashMap.newKeySet();
    private Connector connector;
    private ConnectorMetadata metadata;
    private TestCatalog catalog;
    private TestDatabase database;
    private final Map<String, String> remoteCache = new ConcurrentHashMap<>();
    private ExecutorService worker;
    private CountDownLatch missingStarted;
    private CountDownLatch releaseMissing;
    private AtomicBoolean blockMissing;
    private boolean mvccTables;
    private boolean failLookup;
    private boolean failInvalidation;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @BeforeEach
    void setup() {
        mvccTables = false;
        failLookup = false;
        failInvalidation = false;
        remoteTables.put("a", "old_column");
        remoteTables.put("b", "sibling_column");
        missingStarted = new CountDownLatch(1);
        releaseMissing = new CountDownLatch(1);
        blockMissing = new AtomicBoolean();
        worker = Executors.newFixedThreadPool(2);
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.doAnswer(call -> {
            if (failInvalidation) {
                throw new IllegalStateException("connector cleanup failed");
            }
            remoteCache.remove(call.getArgument(1));
            return null;
        }).when(connector).invalidateTable(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(call -> {
            remoteCache.clear();
            return null;
        }).when(connector).invalidateDb(Mockito.anyString());
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> {
                    if (failLookup) {
                        throw new IllegalStateException("metastore timeout");
                    }
                    String name = call.getArgument(2);
                    if (remoteViews.contains(name)) {
                        return Optional.empty();
                    }
                    String column = remoteTables.get(name);
                    if (column == null) {
                        if (blockMissing.compareAndSet(true, false)) {
                            missingStarted.countDown();
                            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
                        }
                        return Optional.empty();
                    }
                    return Optional.of(new Handle(name, column));
                });
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> remoteViews.contains(call.getArgument(2)));
        Mockito.when(metadata.getTableSchema(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            Handle handle = call.getArgument(1);
            return new ConnectorTableSchema(handle.name, Collections.singletonList(new ConnectorColumn(
                    handle.column, ConnectorType.of("INT", -1, -1), "", true, null, true)),
                    null, Collections.emptyMap());
        });
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).thenAnswer(call -> call.getArgument(3));
        Mockito.when(metadata.fromRemoteTableName(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> call.getArgument(2));
        Mockito.when(metadata.fromRemoteDatabaseName(Mockito.any(), Mockito.anyString()))
                .thenAnswer(call -> call.getArgument(1));
        catalog = new TestCatalog(IDS.incrementAndGet());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().put(catalog.getId(), catalog);
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.put(catalog.getName(), catalog);
        installDatabase();
    }

    @AfterEach
    void cleanup() throws Exception {
        releaseMissing.countDown();
        worker.shutdownNow();
        Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().remove(catalog.getId());
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.remove(catalog.getName());
        remoteTables.clear();
        remoteViews.clear();
        remoteCache.clear();
    }

    private void installDatabase() {
        catalog.setInitializedForTest(true);
        database = new TestDatabase();
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
    }

    private org.apache.doris.connector.cache.MetaCache<SchemaCacheKey, SchemaCacheValue> schemas() {
        ExternalMetaCache cache = Deencapsulation.invoke(Env.getCurrentEnv().getExtMetaCacheMgr(), "engine", "default");
        return cache.entry(catalog.getId(), "schema", SchemaCacheKey.class, SchemaCacheValue.class);
    }

    private PluginDrivenExternalTable warmTarget() {
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        PluginDrivenExternalTable table = database.getTableNullable("a");
        table.getFullSchema();
        database.getTableNullable("b").getFullSchema();
        remoteCache.put("a", "old");
        remoteCache.put("b", "sibling");
        // Keep membership/object entries hot while forcing the next schema read to consult the source.
        schemas().invalidateKey(new SchemaCacheKey(table.getOrBuildNameMapping()));
        return table;
    }

    @Test
    void showCreateFailureAfterHandleResolutionRepairsLookup() {
        PluginDrivenExternalTable old = warmTarget();
        Mockito.when(metadata.renderShowCreateTableDdl(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Assertions.assertThrows(ConnectorTableNotFoundException.class, old::getShowCreateTableDdl);
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void commentFailureRepairsLookupAndKeepsEmptyDisplayValue() {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        Mockito.when(metadata.getTableComment(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new ConnectorTableNotFoundException("db", "a", null));
        Assertions.assertEquals("", old.getComment());
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
    }

    @Test
    void wideMissingRepairRetainsSiblingNumericRoute() {
        catalog.lowerCaseMode = 2;
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenExternalTable sibling = database.getTableNullable("b");
        remoteTables.remove("a");
        readMissingHandle(old);
        PluginDrivenExternalTable reloaded = database.getTableNullable(sibling.getId());
        Assertions.assertNotNull(reloaded);
        Assertions.assertNotSame(sibling, reloaded);
        Assertions.assertEquals("sibling_column", reloaded.getFullSchema().get(0).getName());
        Assertions.assertNull(database.getTableNullable(old.getId()));
    }

    @Test
    void delayedCommentMissCannotClearSameNameReplacement() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        Mockito.when(metadata.getTableComment(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> {
                    missingStarted.countDown();
                    Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
                    throw new ConnectorTableNotFoundException("db", "a", null);
                });
        Future<String> result = worker.submit(() -> old.getComment());
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        remoteTables.put("a", "replacement_column");
        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "replacement");
        int loads = catalog.nameLoads.get();
        releaseMissing.countDown();
        Assertions.assertEquals("", result.get(10, TimeUnit.SECONDS));
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void liveViewAndOtherResourceFailuresDoNotRepairLookup() {
        PluginDrivenExternalTable old = warmTarget();
        Mockito.doThrow(new ConnectorTableNotFoundException("db", "other", null)).when(metadata)
                .getTableComment(Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Assertions.assertEquals("", old.getComment());
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
        Mockito.doThrow(new IllegalStateException("permission denied or missing metadata file")).when(metadata)
                .getTableComment(Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Assertions.assertEquals("", old.getComment());
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
        remoteViews.add("a");
        Mockito.doThrow(new ConnectorTableNotFoundException("db", "a", null)).when(metadata)
                .getTableComment(Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Assertions.assertEquals("", old.getComment());
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
        Assertions.assertEquals("old", remoteCache.get("a"));
        Assertions.assertEquals(1, catalog.nameLoads.get());
    }

    @Test
    void failedViewCheckDoesNotAuthorizeCleanupOrReplaceOriginalFailure() {
        PluginDrivenExternalTable old = warmTarget();
        ConnectorTableNotFoundException failure = new ConnectorTableNotFoundException("db", "a", null);
        Mockito.when(metadata.renderShowCreateTableDdl(Mockito.any(), Mockito.any())).thenThrow(failure);
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new IllegalStateException("metastore timeout"));
        Assertions.assertSame(failure, Assertions.assertThrows(
                ConnectorTableNotFoundException.class, old::getShowCreateTableDdl));
        Assertions.assertEquals(1, failure.getSuppressed().length);
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
    }

    @Test
    void wideRefreshProtectsReplacementPublishedBeforeItCompletes() throws Exception {
        verifyLookupRefreshProtectsReplacement(false);
    }

    @Test
    void coldTableRefreshProtectsReplacementPublishedBeforeItCompletes() throws Exception {
        verifyLookupRefreshProtectsReplacement(true);
    }

    private void verifyLookupRefreshProtectsReplacement(boolean coldTable) throws Exception {
        catalog.lowerCaseMode = 2;
        PluginDrivenExternalTable old = warmTarget();
        if (coldTable) {
            database.evictTableObjectForTest("a");
        }
        database.lookupRefreshPaused = new CountDownLatch(1);
        database.finishLookupRefresh = new CountDownLatch(1);
        Mockito.when(metadata.renderShowCreateTableDdl(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            missingStarted.countDown();
            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Future<?> refresh = worker.submit(() -> {
            new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
            return null;
        });
        try {
            Assertions.assertTrue(database.lookupRefreshPaused.await(10, TimeUnit.SECONDS));
            PluginDrivenExternalTable replacement = database.getTableNullable("a");
            Assertions.assertNotSame(old, replacement);
            Future<?> missing = worker.submit(() -> Assertions.assertThrows(ConnectorTableNotFoundException.class,
                    replacement::getShowCreateTableDdl));
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            remoteTables.put("a", "recreated_column");
            database.finishLookupRefresh.countDown();
            refresh.get(10, TimeUnit.SECONDS);
            List<Column> freshSchema = replacement.getFullSchema();
            Assertions.assertEquals("recreated_column", freshSchema.get(0).getName());
            remoteCache.put("a", "recreated metadata");
            int loads = catalog.nameLoads.get();
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
            Assertions.assertSame(freshSchema, replacement.getFullSchema());
            Assertions.assertEquals("recreated metadata", remoteCache.get("a"));
            Assertions.assertEquals(loads, catalog.nameLoads.get());
        } finally {
            database.finishLookupRefresh.countDown();
            releaseMissing.countDown();
        }
    }

    @Test
    void overlappingLookupRefreshesKeepReplacementUnobservable() {
        PluginDrivenExternalTable table = warmTarget();
        database.beginTableLookupRefresh();
        database.beginTableLookupRefresh();
        Assertions.assertFalse(database.observeTableLookup(table, () -> { }).isPresent());
        database.endTableLookupRefresh();
        Assertions.assertFalse(database.observeTableLookup(table, () -> { }).isPresent());
        database.endTableLookupRefresh();
        Assertions.assertTrue(database.observeTableLookup(table, () -> { }).isPresent());
    }

    @Test
    void failedLookupRefreshReleasesTheDatabaseFence() {
        warmTarget();
        failInvalidation = true;
        Assertions.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a"));
        failInvalidation = false;
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        database.getTableNamesWithLock();
        Assertions.assertTrue(database.observeTableLookup(replacement, () -> { }).isPresent());
        remoteTables.remove("a");
        readMissingHandle(replacement);
        Assertions.assertNull(database.getTableNullable(replacement.getId()));
    }

    @Test
    void pinnedSysSchemaMissingRepairsSourceAndHonorsSessionLifecycle() throws Exception {
        verifyPinnedSysSchemaMissing(false);
    }

    @Test
    void delayedPinnedSysSchemaMissingPreservesRecreatedSource() throws Exception {
        verifyPinnedSysSchemaMissing(true);
    }

    private void verifyPinnedSysSchemaMissing(boolean recreate) throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(1L).build();
        PluginDrivenMvccSnapshot pin = new PluginDrivenMvccSnapshot(snapshot,
                Collections.emptyMap(), Collections.emptyMap());
        AtomicInteger schemaSessionFinished = new AtomicInteger();
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(old, "snapshots") {
            @Override
            public Optional<MvccSnapshot> resolveScanPin(Optional<TableSnapshot> tableSnapshot,
                    Optional<TableScanParams> scanParams) {
                return Optional.of(pin);
            }

            @Override
            protected void closeSchemaSession(ConnectorSession session) {
                schemaSessionFinished.incrementAndGet();
                super.closeSchemaSession(session);
            }
        };
        Mockito.when(metadata.getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString()))
                .thenReturn(Optional.of(new Handle("a$snapshots", "snapshot_id")));
        Mockito.when(metadata.getTableSchema(Mockito.any(), Mockito.any(), Mockito.eq(snapshot))).thenAnswer(call -> {
            remoteTables.remove("a");
            missingStarted.countDown();
            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Future<?> missing = worker.submit(() -> Assertions.assertThrows(ConnectorTableNotFoundException.class,
                () -> sys.getFullSchemaAt(Optional.empty(), Optional.empty())));
        try {
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            PluginDrivenExternalTable replacement = null;
            List<Column> freshSchema = null;
            if (recreate) {
                remoteTables.put("a", "recreated_column");
                new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
                replacement = database.getTableNullable("a");
                freshSchema = replacement.getFullSchema();
                remoteCache.put("a", "recreated metadata");
            }
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(1, schemaSessionFinished.get());
            if (recreate) {
                Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
                Assertions.assertSame(freshSchema, replacement.getFullSchema());
                Assertions.assertEquals("recreated metadata", remoteCache.get("a"));
            } else {
                Assertions.assertNull(database.getTableNullable(old.getId()));
                Assertions.assertNull(remoteCache.get("a"));
                Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
            }
        } finally {
            releaseMissing.countDown();
        }
    }

    @Test
    void droppedMappedRemoteDoesNotHideBehindDistinctFoldedRemote() {
        catalog.lowerCaseMode = 1;
        Mockito.when(metadata.fromRemoteTableName(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(call -> {
                    String name = call.getArgument(2);
                    return "Foo".equals(name) ? "foo_old" : "FOO".equals(name) ? "foo_new" : name;
                });
        remoteTables.remove("a");
        remoteTables.put("Foo", "old_column");
        remoteTables.put("FOO", "new_column");
        Assertions.assertEquals(Set.of("foo_old", "foo_new", "b"), database.getTableNamesWithLock());
        PluginDrivenExternalTable old = database.getTableNullable("foo_old");
        PluginDrivenExternalTable sibling = database.getTableNullable("foo_new");
        remoteTables.remove("Foo");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        Assertions.assertEquals(Set.of("foo_new", "b"), database.getTableNamesWithLock());
        readMissingHandle(old);
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNotNull(database.getTableNullable(sibling.getId()));
        Assertions.assertEquals("FOO", database.getTableNullable("foo_new").getRemoteName());
    }

    @Test
    void sysSplitPlanningMissingRepairsSource() throws Exception {
        verifySysSplitPlanningMissing(false);
    }

    @Test
    void delayedSysSplitPlanningMissingPreservesRecreatedSource() throws Exception {
        verifySysSplitPlanningMissing(true);
    }

    private void verifySysSplitPlanningMissing(boolean recreate) throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(old, "labels");
        Mockito.when(metadata.getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString()))
                .thenReturn(Optional.of(new Handle("a$labels", "label")));
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(sys);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(connector.getScanPlanProvider(Mockito.any())).thenReturn(provider);
        PluginDrivenScanNode scan = PluginDrivenScanNode.create(new PlanNodeId(0), tuple, false,
                new SessionVariable(), ScanContext.EMPTY, catalog, sys);
        Mockito.when(provider.planScan(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            missingStarted.countDown();
            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Future<?> missing = worker.submit(() -> Assertions.assertThrows(ConnectorTableNotFoundException.class,
                () -> scan.getSplits(1)));
        try {
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            PluginDrivenExternalTable replacement = null;
            if (recreate) {
                remoteTables.put("a", "recreated_column");
                new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
                replacement = database.getTableNullable("a");
                replacement.getFullSchema();
                remoteCache.put("a", "recreated metadata");
            }
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            if (recreate) {
                Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
                Assertions.assertEquals("recreated metadata", remoteCache.get("a"));
            } else {
                Assertions.assertNull(database.getTableNullable(old.getId()));
                Assertions.assertNull(remoteCache.get("a"));
            }
        } finally {
            releaseMissing.countDown();
        }
    }

    @Test
    void explicitWideRefreshRetainsSiblingNumericRoute() throws Exception {
        catalog.lowerCaseMode = 2;
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenExternalTable sibling = database.getTableNullable("b");
        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
        Assertions.assertNotNull(database.getTableNullable(sibling.getId()));
        Assertions.assertNotSame(old, database.getTableNullable("a"));
    }

    @Test
    void retainedObjectOmittedByNamesRefreshCanStillRepair() {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        readMissingHandle(old);
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void omittedOldObjectCannotClearCurrentFoldedAlias() {
        catalog.lowerCaseMode = 2;
        remoteTables.put("Foo", "old_column");
        PluginDrivenExternalTable old = database.getTableNullable("foo");
        old.getFullSchema();
        remoteTables.remove("Foo");
        remoteTables.put("FOO", "replacement_column");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        PluginDrivenExternalTable replacement = database.getTableNullable("foo");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("FOO", "replacement");
        readMissingHandle(old);
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("FOO"));
    }

    @Test
    void missingViewTextRepairsItsLookupIdentity() {
        warmTarget();
        remoteViews.add("v");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        Assertions.assertTrue(database.getTableNamesWithLock().contains("v"));
        PluginDrivenExternalTable view = database.getTableNullable("v");
        remoteCache.put("v", "view metadata");
        remoteViews.remove("v");
        Mockito.when(metadata.getViewDefinition(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new ConnectorTableNotFoundException("db", "v", null));
        Assertions.assertThrows(ConnectorTableNotFoundException.class, view::getViewText);
        Assertions.assertNull(database.getTableNullable(view.getId()));
        Assertions.assertNull(remoteCache.get("v"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
    }

    @Test
    void sysTableResolutionRepairsDroppedSource() {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(old, "snapshots");
        remoteTables.remove("a");
        Assertions.assertFalse(sys.resolveConnectorTableHandle(catalog.buildConnectorSession(), metadata).isPresent());
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void sysHandleReadAfterBaseHandleRepairsSourceIdentity() {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(old, "snapshots");
        Mockito.when(metadata.getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString()))
                .thenAnswer(call -> {
                    remoteTables.remove("a");
                    throw new ConnectorTableNotFoundException("db", "a", null);
                });
        Assertions.assertThrows(ConnectorTableNotFoundException.class,
                () -> sys.resolveConnectorTableHandle(catalog.buildConnectorSession(), metadata));
        Assertions.assertNull(database.getTableNullable(old.getId()));
    }

    @Test
    void mvccSnapshotReadAfterHandleRepairsDroppedSource() {
        mvccTables = true;
        PluginDrivenMvccExternalTable old = (PluginDrivenMvccExternalTable) warmTarget();
        Mockito.when(metadata.beginQuerySnapshot(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Assertions.assertThrows(ConnectorTableNotFoundException.class, old::loadLatestSnapshotFence);
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void scanColumnsAfterHandleRepairDroppedSource() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(old);
        PluginDrivenScanNode scan = PluginDrivenScanNode.create(new PlanNodeId(0), tuple, false,
                new SessionVariable(), ScanContext.EMPTY, catalog, old);
        Mockito.when(metadata.getColumnHandles(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Assertions.assertThrows(RuntimeException.class, () -> Deencapsulation.invoke(scan, "buildColumnHandles"));
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void delayedMvccMissCannotRetireSameNameReplacement() throws Exception {
        mvccTables = true;
        PluginDrivenMvccExternalTable old = (PluginDrivenMvccExternalTable) warmTarget();
        Mockito.when(metadata.beginQuerySnapshot(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            missingStarted.countDown();
            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Future<?> failure = worker.submit(() -> Assertions.assertThrows(
                ConnectorTableNotFoundException.class, old::loadLatestSnapshotFence));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        remoteCache.put("a", "replacement");
        releaseMissing.countDown();
        failure.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertEquals("replacement", remoteCache.get("a"));
    }

    @Test
    void metadataRefreshBetweenHandleAndSnapshotReadIsProtected() {
        mvccTables = true;
        PluginDrivenMvccExternalTable old = (PluginDrivenMvccExternalTable) warmTarget();
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.anyString(), Mockito.eq("a")))
                .thenAnswer(call -> {
                    new RefreshManager().refreshTableInternal(database, old, 123L);
                    remoteCache.put("a", "refreshed metadata");
                    return Optional.of(new Handle("a", "old_column"));
                });
        Mockito.when(metadata.beginQuerySnapshot(Mockito.any(), Mockito.any()))
                .thenThrow(new ConnectorTableNotFoundException("db", "a", null));
        Assertions.assertThrows(ConnectorTableNotFoundException.class, old::loadLatestSnapshotFence);
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
        Assertions.assertEquals("refreshed metadata", remoteCache.get("a"));
    }

    @Test
    void metadataRefreshAfterScanHandleProtectsTheNewState() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(old);
        PluginDrivenScanNode scan = PluginDrivenScanNode.create(new PlanNodeId(0), tuple, false,
                new SessionVariable(), ScanContext.EMPTY, catalog, old);
        new RefreshManager().refreshTableInternal(database, old, 123L);
        remoteCache.put("a", "refreshed metadata");
        Mockito.when(metadata.getColumnHandles(Mockito.any(), Mockito.any()))
                .thenThrow(new ConnectorTableNotFoundException("db", "a", null));
        Assertions.assertThrows(RuntimeException.class, () -> Deencapsulation.invoke(scan, "buildColumnHandles"));
        Assertions.assertSame(old, database.getTableNullable(old.getId()));
        Assertions.assertEquals("refreshed metadata", remoteCache.get("a"));
    }

    @Test
    void sysSchemaFailureRepairsTheSourceInsteadOfTheSyntheticName() {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(old, "snapshots");
        Mockito.when(metadata.getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString()))
                .thenReturn(Optional.of(new Handle("a$snapshots", "metadata")));
        Mockito.doAnswer(call -> {
            remoteTables.remove("a");
            throw new ConnectorTableNotFoundException("db", "a", null);
        }).when(metadata).getTableSchema(Mockito.any(), Mockito.any());
        Assertions.assertThrows(ConnectorTableNotFoundException.class, sys::initSchema);
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void showCreateStartingDuringRefreshCannotClearTheCompletedRefresh() throws Exception {
        PluginDrivenExternalTable table = warmTarget();
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch finishRefresh = new CountDownLatch(1);
        Mockito.doAnswer(call -> {
            refreshStarted.countDown();
            Assertions.assertTrue(finishRefresh.await(10, TimeUnit.SECONDS));
            remoteCache.remove("a");
            return null;
        }).when(connector).invalidateTable(Mockito.anyString(), Mockito.anyString());
        Mockito.when(metadata.renderShowCreateTableDdl(Mockito.any(), Mockito.any())).thenAnswer(call -> {
            remoteTables.remove("a");
            missingStarted.countDown();
            Assertions.assertTrue(releaseMissing.await(10, TimeUnit.SECONDS));
            throw new ConnectorTableNotFoundException("db", "a", null);
        });
        Future<?> refresh = worker.submit(() -> new RefreshManager().refreshTableInternal(database, table, 123L));
        try {
            Assertions.assertTrue(refreshStarted.await(10, TimeUnit.SECONDS));
            Future<?> missing = worker.submit(() -> Assertions.assertThrows(ConnectorTableNotFoundException.class,
                    table::getShowCreateTableDdl));
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            remoteTables.put("a", "recreated_column");
            finishRefresh.countDown();
            refresh.get(10, TimeUnit.SECONDS);
            List<Column> freshSchema = table.getFullSchema();
            remoteCache.put("a", "recreated metadata");
            int loads = catalog.nameLoads.get();
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(table, database.getTableNullable(table.getId()));
            Assertions.assertSame(freshSchema, table.getFullSchema());
            Assertions.assertEquals("recreated metadata", remoteCache.get("a"));
            Assertions.assertEquals(loads, catalog.nameLoads.get());
        } finally {
            finishRefresh.countDown();
            releaseMissing.countDown();
        }
    }

    @Test
    void overlappingRefreshesRemainUnobservableUntilBothFinish() throws Exception {
        PluginDrivenExternalTable table = warmTarget();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch firstFinish = new CountDownLatch(1);
        CountDownLatch secondFinish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Mockito.doAnswer(call -> {
            boolean first = calls.getAndIncrement() == 0;
            (first ? firstStarted : secondStarted).countDown();
            Assertions.assertTrue((first ? firstFinish : secondFinish).await(10, TimeUnit.SECONDS));
            return null;
        }).when(connector).invalidateTable(Mockito.anyString(), Mockito.anyString());
        Future<?> first = worker.submit(() -> new RefreshManager().refreshTableInternal(database, table, 123L));
        try {
            Assertions.assertTrue(firstStarted.await(10, TimeUnit.SECONDS));
            Future<?> second = worker.submit(() -> new RefreshManager().refreshTableInternal(database, table, 124L));
            Assertions.assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(database.observeTableLookup(table, () -> { }).isPresent());
            firstFinish.countDown();
            first.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(database.observeTableLookup(table, () -> { }).isPresent());
            secondFinish.countDown();
            second.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(database.observeTableLookup(table, () -> { }).isPresent());
        } finally {
            firstFinish.countDown();
            secondFinish.countDown();
        }
    }

    @Test
    void failedRefreshReleasesTheObservationFence() {
        PluginDrivenExternalTable table = warmTarget();
        failInvalidation = true;
        Assertions.assertThrows(IllegalStateException.class,
                () -> new RefreshManager().refreshTableInternal(database, table, 123L));
        Assertions.assertTrue(database.observeTableLookup(table, () -> { }).isPresent());
        failInvalidation = false;
        remoteTables.remove("a");
        readMissingHandle(table);
        Assertions.assertNull(database.getTableNullable(table.getId()));
    }

    @Test
    void missingTableRepairsNamesObjectsIdsAndConnectorMetadata() {
        PluginDrivenExternalTable old = warmTarget();
        PluginDrivenExternalTable sibling = database.getTableNullable("b");
        List<Column> siblingSchema = sibling.getFullSchema();
        remoteTables.remove("a");
        Assertions.assertThrows(CacheException.class, old::getFullSchema);
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(old.getId()));
        Assertions.assertNull(remoteCache.get("a"));
        Assertions.assertEquals("sibling", remoteCache.get("b"));
        Assertions.assertSame(sibling, database.getTableNullable("b"));
        Assertions.assertSame(siblingSchema, sibling.getFullSchema());
    }

    @Test
    void missingHandleRepairsNamesBeforeSchemaLoading() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(table.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(table.getId()));
        Assertions.assertNull(remoteCache.get("a"));
    }

    @Test
    void existingViewWithoutTableHandleKeepsLookupCaches() {
        remoteViews.add("v");
        PluginDrivenExternalTable view = database.getTableNullable("v");
        Assertions.assertNotNull(view);
        remoteCache.put("v", "view metadata");
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(view.resolveConnectorTableHandle(session, metadata).isPresent());
            Assertions.assertFalse(view.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
        Assertions.assertSame(view, database.getTableNullable("v"));
        Assertions.assertSame(view, database.getTableNullable(view.getId()));
        Assertions.assertTrue(database.getTableNamesWithLock().contains("v"));
        Assertions.assertEquals("view metadata", remoteCache.get("v"));

        remoteViews.remove("v");
        readMissingHandle(view);
        Assertions.assertNull(database.getTableNullable(view.getId()));
        Assertions.assertFalse(database.getTableNamesWithLock().contains("v"));
    }

    @Test
    void delayedMissingDoesNotInvalidateRecreatedTable() throws Exception {
        verifyDelayedMissing(false);
    }

    @Test
    void delayedMissingDoesNotInvalidateExactKeyReplacement() throws Exception {
        verifyDelayedMissing(true);
    }

    @Test
    void delayedMissingDoesNotInvalidateMetadataOnlyRefresh() throws Exception {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> missing = worker.submit(() -> readMissingHandle(table));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));

        remoteTables.put("a", "new_column");
        new RefreshManager().refreshTableInternal(database, table, 123L);
        List<Column> refreshedSchema = table.getFullSchema();
        remoteCache.put("a", "refreshed");
        int nameLoads = catalog.nameLoads.get();

        releaseMissing.countDown();
        missing.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(table, database.getTableNullable("a"));
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertSame(refreshedSchema, table.getFullSchema());
        Assertions.assertEquals("new_column", refreshedSchema.get(0).getName());
        Assertions.assertEquals("refreshed", remoteCache.get("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    private void verifyDelayedMissing(boolean replaceWithoutScopeInvalidation) throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<CacheException> result = worker.submit(() -> Assertions.assertThrows(CacheException.class,
                old::getFullSchema));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        remoteTables.put("a", "new_column");
        PluginDrivenExternalTable replacement;
        if (replaceWithoutScopeInvalidation) {
            replacement = database.buildTableInternal("a", "a", old.getId(), catalog, database);
            database.addTableForTest(replacement);
            schemas().put(new SchemaCacheKey(replacement.getOrBuildNameMapping()), replacement.initSchema().get());
        } else {
            new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
            replacement = database.getTableNullable("a");
        }
        List<Column> newSchema = replacement.getFullSchema();
        remoteCache.put("a", "new");
        database.getTableNamesWithLock();
        int nameLoads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(replacement, database.getTableNullable(replacement.getId()));
        Assertions.assertSame(newSchema, replacement.getFullSchema());
        Assertions.assertTrue(replacement.isObjectCreated());
        Assertions.assertEquals("new_column", newSchema.get(0).getName());
        Assertions.assertEquals("new", remoteCache.get("a"));
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertEquals(nameLoads, catalog.nameLoads.get());
    }

    @Test
    void delayedMissingDoesNotInvalidateReplacementDatabase() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> result = worker.submit(() -> readMissingHandle(old));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        remoteTables.put("a", "replacement_column");
        database = new TestDatabase();
        database.setInitializedForTest(true);
        catalog.addDatabaseForTest(database);
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "replacement");
        database.getTableNamesWithLock();
        int loads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void delayedMissingDoesNotInvalidateReplacementCatalog() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        blockMissing.set(true);
        Future<?> result = worker.submit(() -> readMissingHandle(old));
        Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
        long catalogId = catalog.getId();
        catalog = new TestCatalog(catalogId);
        CatalogMgr manager = Env.getCurrentEnv().getCatalogMgr();
        manager.getIdToCatalog().put(catalogId, catalog);
        Map<String, CatalogIf<?>> byName = Deencapsulation.getField(manager, "nameToCatalog");
        byName.put(catalog.getName(), catalog);
        remoteTables.put("a", "replacement_column");
        installDatabase();
        PluginDrivenExternalTable replacement = database.getTableNullable("a");
        List<Column> schema = replacement.getFullSchema();
        remoteCache.put("a", "replacement");
        database.getTableNamesWithLock();
        int loads = catalog.nameLoads.get();
        releaseMissing.countDown();
        result.get(10, TimeUnit.SECONDS);
        Assertions.assertSame(replacement, database.getTableNullable("a"));
        Assertions.assertSame(schema, replacement.getFullSchema());
        Assertions.assertEquals("replacement", remoteCache.get("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void failedConnectorCleanupStillRemovesObservedLookup() {
        PluginDrivenExternalTable old = warmTarget();
        remoteTables.remove("a");
        failInvalidation = true;
        Assertions.assertThrows(IllegalStateException.class, () -> readMissingHandle(old));
        Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        Assertions.assertNull(database.getTableNullable(old.getId()));
    }

    private void readMissingHandle(PluginDrivenExternalTable table) {
        ConnectorSession session = catalog.buildCrossStatementSession();
        try {
            Assertions.assertFalse(table.resolveConnectorTableHandle(session, metadata).isPresent());
        } finally {
            session.getStatementScope().closeAll();
        }
    }

    @Test
    void changedCanonicalSpellingFencesBothExistingAndNewOldObjectReads() throws Exception {
        catalog.lowerCaseMode = 2;
        try {
            remoteTables.remove("a");
            remoteTables.put("Foo", "old_column");
            PluginDrivenExternalTable old = database.getTableNullable("foo");
            old.getFullSchema();
            schemas().invalidateKey(new SchemaCacheKey(old.getOrBuildNameMapping()));
            remoteTables.remove("Foo");
            blockMissing.set(true);
            Future<CacheException> missing = worker.submit(() -> Assertions.assertThrows(CacheException.class,
                    old::getFullSchema));
            Assertions.assertTrue(missingStarted.await(10, TimeUnit.SECONDS));
            remoteTables.put("FOO", "new_column");
            FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
            names.invalidateKey("");
            PluginDrivenExternalTable replacement = database.getTableNullable("foo");
            Assertions.assertEquals("FOO", replacement.getName());
            List<Column> schema = replacement.getFullSchema();
            remoteCache.put("FOO", "new");
            int loads = catalog.nameLoads.get();
            releaseMissing.countDown();
            missing.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(replacement, database.getTableNullable("foo"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals("new", remoteCache.get("FOO"));
            Assertions.assertEquals(loads, catalog.nameLoads.get());
            // A fresh request holding the old object must not combine it with the new names snapshot.
            ConnectorSession session = catalog.buildCrossStatementSession();
            try {
                Assertions.assertFalse(old.resolveConnectorTableHandle(session, metadata).isPresent());
            } finally {
                session.getStatementScope().closeAll();
            }
            Assertions.assertSame(replacement, database.getTableNullable("foo"));
            Assertions.assertSame(schema, replacement.getFullSchema());
            Assertions.assertEquals("new", remoteCache.get("FOO"));
        } finally {
            catalog.lowerCaseMode = 0;
        }
    }

    @Test
    void lookupFailureDoesNotRepairNamesOrEvictObjects() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        failLookup = true;
        Assertions.assertThrows(IllegalStateException.class, table::getFullSchema);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertEquals("old", remoteCache.get("a"));
        Assertions.assertEquals(1, catalog.nameLoads.get());
    }

    @Test
    void numericLookupCapturedBeforeRefreshRepublishesItsIdentity() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        IdNameIndex original = Deencapsulation.getField(database, "tableIdNameIndex");
        IdNameIndex index = Mockito.spy(original);
        Deencapsulation.setField(database, "tableIdNameIndex", index);
        CountDownLatch captured = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        Mockito.doAnswer(call -> {
            String name = original.getName(old.getId());
            if (pause.compareAndSet(true, false)) {
                captured.countDown();
                Assertions.assertTrue(proceed.await(10, TimeUnit.SECONDS));
            }
            return name;
        }).when(index).getName(old.getId());
        Future<PluginDrivenExternalTable> lookup = worker.submit(() -> database.getTableNullable(old.getId()));
        try {
            Assertions.assertTrue(captured.await(10, TimeUnit.SECONDS));
            remoteTables.put("a", "new_column");
            new RefreshManager().handleRefreshTable(catalog.getName(), "db", "a");
            Assertions.assertNull(original.getName(old.getId()));
            proceed.countDown();
            PluginDrivenExternalTable replacement = lookup.get(10, TimeUnit.SECONDS);
            Assertions.assertNotSame(old, replacement);
            Assertions.assertEquals("a", index.getName(old.getId()));
            Assertions.assertSame(replacement, database.getTableNullable(old.getId()));
        } finally {
            proceed.countDown();
        }
    }

    @Test
    void foldedNamesRefreshClearsHistoricalRemoteSpellings() throws Exception {
        catalog.lowerCaseMode = 1;
        remoteTables.remove("a");
        remoteTables.put("Foo", "first");
        Assertions.assertNotNull(database.getTableNullable("foo"));
        remoteCache.put("Foo", "first");
        remoteTables.remove("Foo");
        remoteTables.put("FOO", "second");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        database.evictTableObjectForTest("foo");
        PluginDrivenExternalTable second = database.getTableNullable("foo");
        Assertions.assertEquals("FOO", second.getRemoteName());
        remoteCache.put("FOO", "second");
        remoteTables.remove("FOO");
        remoteTables.put("Foo", "third");
        new RefreshManager().handleRefreshTable(catalog.getName(), "db", "foo");
        Assertions.assertTrue(remoteCache.isEmpty());
        PluginDrivenExternalTable replacement = database.getTableNullable("foo");
        Assertions.assertEquals("Foo", replacement.getRemoteName());
        Assertions.assertEquals("third", replacement.getFullSchema().get(0).getName());
    }

    @Test
    void automaticRepairClearsHistoricalCaseInsensitiveSpellings() {
        verifyAutomaticRepairClearsHistoricalSpellings(2);
    }

    @Test
    void automaticRepairClearsHistoricalFoldedSpellings() {
        verifyAutomaticRepairClearsHistoricalSpellings(1);
    }

    private void verifyAutomaticRepairClearsHistoricalSpellings(int mode) {
        catalog.lowerCaseMode = mode;
        remoteTables.remove("a");
        remoteTables.put("Foo", "first_column");
        PluginDrivenExternalTable first = database.getTableNullable("foo");
        first.getFullSchema();
        remoteCache.put("Foo", "first");
        remoteTables.remove("Foo");
        remoteTables.put("FOO", "second_column");
        FeMetaCacheEntry<String, NameCacheValue> names = Deencapsulation.getField(database, "tableNames");
        names.invalidateKey("");
        if (mode == 1) {
            database.evictTableObjectForTest("foo");
        }
        PluginDrivenExternalTable second = database.getTableNullable("foo");
        second.getFullSchema();
        remoteCache.put("FOO", "second");
        remoteTables.remove("FOO");
        remoteTables.put("Foo", "third_column");
        readMissingHandle(second);
        Assertions.assertTrue(remoteCache.isEmpty());
        PluginDrivenExternalTable third = database.getTableNullable("foo");
        Assertions.assertNotSame(first, third);
        Assertions.assertEquals("Foo", third.getRemoteName());
        Assertions.assertEquals("third_column", third.getFullSchema().get(0).getName());
    }

    @Test
    void numericLookupCannotRefillBetweenObjectAndNamesRetirement() throws Exception {
        PluginDrivenExternalTable old = warmTarget();
        IdNameIndex original = Deencapsulation.getField(database, "tableIdNameIndex");
        IdNameIndex index = Mockito.spy(original);
        Deencapsulation.setField(database, "tableIdNameIndex", index);
        CountDownLatch captured = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch progress = new CountDownLatch(1);
        CountDownLatch finishBuild = new CountDownLatch(1);
        AtomicBoolean capture = new AtomicBoolean(true);
        database.numericBuildProgress = progress;
        database.finishNumericBuild = finishBuild;
        Mockito.doAnswer(call -> {
            String name = original.getName(old.getId());
            if (capture.compareAndSet(true, false)) {
                captured.countDown();
                Assertions.assertTrue(resume.await(10, TimeUnit.SECONDS));
            }
            return name;
        }).when(index).getName(old.getId());
        FeMetaCacheEntry<String, PluginDrivenExternalTable> objects =
                Mockito.spy(Deencapsulation.<FeMetaCacheEntry<String, PluginDrivenExternalTable>>getField(
                        database, "tables"));
        Deencapsulation.setField(database, "tables", objects);
        Mockito.doAnswer(call -> {
            Optional<FeMetaCacheEntry.Observation> observed = (Optional<FeMetaCacheEntry.Observation>) call.callRealMethod();
            FeMetaCacheEntry.Observation observation = Mockito.spy(observed.get());
            org.mockito.stubbing.Answer<Boolean> afterObjectGuard = invocation -> {
                boolean current = (boolean) invocation.callRealMethod();
                if (current) {
                    // Pause the missing repair after its object guard returns but before names retire.
                    resume.countDown();
                    Assertions.assertTrue(progress.await(10, TimeUnit.SECONDS));
                }
                return current;
            };
            Mockito.doAnswer(afterObjectGuard).when(observation).runIfCurrent(Mockito.any(BooleanSupplier.class));
            Mockito.doAnswer(afterObjectGuard).when(observation).invalidateIfCurrent(Mockito.any(BooleanSupplier.class));
            return Optional.of(observation);
        }).when(objects).observe("a", old);
        Future<PluginDrivenExternalTable> numeric = worker.submit(() -> {
            PluginDrivenExternalTable result = database.getTableNullable(old.getId());
            progress.countDown();
            return result;
        });
        try {
            Assertions.assertTrue(captured.await(10, TimeUnit.SECONDS));
            remoteTables.remove("a");
            readMissingHandle(old);
            finishBuild.countDown();
            numeric.get(10, TimeUnit.SECONDS);
            Assertions.assertNull(objects.getIfPresent("a"));
            Assertions.assertNull(original.getName(old.getId()));
            Assertions.assertEquals(Set.of("b"), database.getTableNamesWithLock());
        } finally {
            resume.countDown();
            finishBuild.countDown();
        }
    }

    @Test
    void ordinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(0);
    }

    @Test
    void foldedNameOrdinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(1);
    }

    @Test
    void caseInsensitiveOrdinaryWritesDoNotReloadNames() {
        verifyOrdinaryWrite(2);
    }

    private void verifyOrdinaryWrite(int mode) {
        catalog.lowerCaseMode = mode;
        PluginDrivenExternalTable table = warmTarget();
        int loads = catalog.nameLoads.get();
        new RefreshManager().refreshTableInternal(database, table, 123L);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable("a"));
        Assertions.assertEquals(loads, catalog.nameLoads.get());
    }

    @Test
    void sessionBypassDoesNotInvalidateSharedNames() {
        PluginDrivenExternalTable table = warmTarget();
        remoteTables.remove("a");
        catalog.bypassSchema = true;
        readMissingHandle(table);
        Assertions.assertEquals(Set.of("a", "b"), database.getTableNamesWithLock());
        Assertions.assertSame(table, database.getTableNullable(table.getId()));
        Assertions.assertEquals("old", remoteCache.get("a"));
    }

    private static class Handle implements ConnectorTableHandle {
        private final String name;
        private final String column;

        Handle(String name, String column) {
            this.name = name;
            this.column = column;
        }
    }

    private class TestCatalog extends PluginDrivenExternalCatalog {
        private final AtomicInteger nameLoads = new AtomicInteger();
        private boolean bypassSchema;
        private int lowerCaseMode;

        TestCatalog(long id) {
            super(id, "missing_test_" + id, "", new HashMap<>(), "", connector);
        }

        @Override
        protected void initLocalObjectsImpl() {
            // The fixture injects the connector directly instead of loading a production plugin.
        }

        @Override
        public int getLowerCaseTableNames() {
            return lowerCaseMode;
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.singletonList("db");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String dbName) {
            nameLoads.incrementAndGet();
            List<String> names = new ArrayList<>(remoteTables.keySet());
            names.addAll(remoteViews);
            return names;
        }

        @Override
        public boolean shouldBypassSchemaCache(SessionContext context) {
            return bypassSchema;
        }
    }

    private class TestDatabase extends ExternalDatabase<PluginDrivenExternalTable> {
        private CountDownLatch lookupRefreshPaused;
        private CountDownLatch finishLookupRefresh;

        @Override
        public void invalidateTableLookup(String tableName) {
            super.invalidateTableLookup(tableName);
            if (lookupRefreshPaused != null) {
                lookupRefreshPaused.countDown();
                try {
                    Assertions.assertTrue(finishLookupRefresh.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        }

        private CountDownLatch numericBuildProgress;
        private CountDownLatch finishNumericBuild;

        TestDatabase() {
            super(catalog, Util.genIdByName(catalog.getName(), "db"), "db", "db");
        }

        @Override
        protected PluginDrivenExternalTable buildTableInternal(String remoteName, String localName, long id,
                ExternalCatalog owner, ExternalDatabase db) {
            if (numericBuildProgress != null) {
                numericBuildProgress.countDown();
                try {
                    Assertions.assertTrue(finishNumericBuild.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            if (mvccTables) {
                return new PluginDrivenMvccExternalTable(id, localName, remoteName, owner, db) {
                    @Override
                    protected synchronized void makeSureInitialized() {
                        objectCreated = true;
                    }
                };
            }
            return new PluginDrivenExternalTable(id, localName, remoteName, owner, db) {
                @Override
                protected synchronized void makeSureInitialized() {
                    // The fixture registers this database explicitly; connector schema loading remains real.
                    objectCreated = true;
                }
            };
        }
    }
}
