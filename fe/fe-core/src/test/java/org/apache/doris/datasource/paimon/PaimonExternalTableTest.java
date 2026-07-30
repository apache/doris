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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalRowCountCache;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonExternalTableTest {

    @Test
    public void testSelectorFreeOptionsPreserveStatementSnapshot() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Optional<MvccSnapshot> statementSnapshot = Optional.of(snapshot);
        Table statementTable = Mockito.mock(Table.class);
        Table statementCopy = Mockito.mock(Table.class);
        Table baseTable = Mockito.mock(Table.class);
        Table baseCopy = Mockito.mock(Table.class);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.plan-sort-partition", "true"),
                Collections.emptyList());

        Mockito.doReturn(statementTable).when(externalTable).getPaimonTable(statementSnapshot);
        Mockito.when(statementTable.copy(scanParams.getMapParams())).thenReturn(statementCopy);
        Mockito.when(statementCopy.options()).thenReturn(scanParams.getMapParams());
        Mockito.when(baseTable.copy(scanParams.getMapParams())).thenReturn(baseCopy);

        try (MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class);
                MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable)).thenReturn(statementSnapshot);
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(baseTable);

            Assert.assertSame(statementCopy, externalTable.getPaimonTable(scanParams));
        }
    }

    @Test
    public void testModeOnlyLatestUsesStatementSnapshotAcrossPhases() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        MvccSnapshot snapshot = Mockito.mock(MvccSnapshot.class);
        Optional<MvccSnapshot> statementSnapshot = Optional.of(snapshot);
        Table statementTable = Mockito.mock(Table.class);
        Table pinnedCopy = Mockito.mock(Table.class);
        Table baseTable = Mockito.mock(Table.class);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.mode", "latest"),
                Collections.emptyList());

        Mockito.doReturn(statementTable).when(externalTable).getPaimonTable(statementSnapshot);
        Mockito.when(statementTable.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "7"));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(pinnedCopy);
        Mockito.when(pinnedCopy.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "7"));

        try (MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class);
                MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable)).thenReturn(statementSnapshot);
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(baseTable);

            Assert.assertSame(pinnedCopy, externalTable.getPaimonTable(scanParams));
            Assert.assertSame(pinnedCopy, externalTable.getPaimonTable(scanParams));
        }

        Mockito.verify(baseTable, Mockito.times(2)).copy(ArgumentMatchers.argThat(options ->
                "7".equals(options.get("scan.snapshot-id"))
                        && options.containsKey("scan.mode")
                        && options.get("scan.mode") == null));
    }

    @Test
    public void testBranchSnapshotUsesEffectiveTableSchema() {
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable externalTable = new PaimonExternalTable(
                1L, "local_table", "remote_table", catalog, database);
        FileStoreTable branchTable = Mockito.mock(FileStoreTable.class);
        TableSchema branchSchema = new TableSchema(3L,
                Collections.singletonList(new DataField(1, "branch_column", DataTypes.INT())),
                1, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), "");
        Mockito.when(branchTable.schema()).thenReturn(branchSchema);
        Mockito.when(branchTable.schemaManager()).thenThrow(
                new AssertionError("branch schema must not be looked up through the base namespace"));
        PaimonSnapshotCacheValue cacheValue = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(7L, 3L, branchTable), true);

        List<Column> schema = externalTable.getFullSchema(
                Optional.of(new PaimonMvccSnapshot(cacheValue)));

        Assert.assertEquals(1, schema.size());
        Assert.assertEquals("branch_column", schema.get(0).getName());
    }

    @Test
    public void testRelationOptionsLoadSnapshotProjectionFromEffectiveTable() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(externalTable).makeSureInitialized();
        FileStoreTable unsafeDataTable = newFileStoreTable(
                "snapshot_projection", ImmutableMap.of("scan.manifest.parallelism", "0"), null);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.manifest.parallelism", "1"),
                Collections.emptyList());
        PaimonSnapshotCacheValue projection = Mockito.mock(PaimonSnapshotCacheValue.class);

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(unsafeDataTable);
            paimonUtils.when(() -> PaimonUtils.loadSnapshotProjection(
                    Mockito.eq(externalTable), Mockito.any(Table.class))).thenReturn(projection);

            PaimonMvccSnapshot snapshot = (PaimonMvccSnapshot) externalTable.loadSnapshot(
                    Optional.empty(), Optional.of(scanParams));

            Assert.assertSame(projection, snapshot.getSnapshotCacheValue());
            paimonUtils.verify(() -> PaimonUtils.loadSnapshotProjection(
                    Mockito.eq(externalTable), Mockito.argThat(table ->
                            "1".equals(table.options().get("scan.manifest.parallelism")))));
        }
    }

    @Test
    public void testReaderOnlyOptionsReusePartitionedMemoizedProjection() {
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(externalTable).makeSureInitialized();
        FileStoreTable partitionedTable = newPartitionedFileStoreTable("reader_only_projection");
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("read.batch-size", "4096"),
                Collections.emptyList());
        PaimonSnapshotCacheValue memoizedProjection = Mockito.mock(PaimonSnapshotCacheValue.class);
        PaimonSnapshotCacheValue directProjection = Mockito.mock(PaimonSnapshotCacheValue.class);
        AtomicInteger directProjectionLoads = new AtomicInteger();

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(partitionedTable);
            paimonUtils.when(() -> PaimonUtils.getLatestSnapshotCacheValue(externalTable))
                    .thenReturn(memoizedProjection);
            paimonUtils.when(() -> PaimonUtils.loadSnapshotProjection(
                    Mockito.eq(externalTable), Mockito.any(Table.class))).thenAnswer(invocation -> {
                        directProjectionLoads.incrementAndGet();
                        return directProjection;
                    });

            PaimonMvccSnapshot snapshot = (PaimonMvccSnapshot) externalTable.loadSnapshot(
                    Optional.empty(), Optional.of(scanParams));

            Assert.assertSame(memoizedProjection, snapshot.getSnapshotCacheValue());
            Assert.assertEquals(0, directProjectionLoads.get());
        }
    }

    @Test
    public void testSystemTableIsBuiltFromTheValidatedSourceHandle() {
        FileStoreTable safeSource = newFileStoreTable(
                "safe_cached_source", ImmutableMap.of("scan.manifest.parallelism", "1"), null);
        FileStoreTable unsafeReload = newFileStoreTable(
                "unsafe_catalog_reload", ImmutableMap.of("scan.manifest.parallelism", "0"), null);
        Table divergentWrapper = new PartitionsTable(unsafeReload);
        PaimonExternalTable sourceTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getRemoteName()).thenReturn("db");
        Mockito.when(sourceTable.getId()).thenReturn(10L);
        Mockito.when(sourceTable.getName()).thenReturn("source");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("source");
        Mockito.when(sourceTable.getCatalog()).thenReturn(catalog);
        Mockito.when(sourceTable.getDatabase()).thenReturn(database);
        Mockito.doReturn(safeSource).when(sourceTable).getBasePaimonTable();
        Mockito.when(catalog.getPaimonTable(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(divergentWrapper);
        PaimonSysExternalTable systemTable = new PaimonSysExternalTable(sourceTable, "partitions");

        Table actual = systemTable.getSysPaimonTable();

        Assert.assertNotSame(divergentWrapper, actual);
        Mockito.verify(catalog, Mockito.never()).getPaimonTable(
                Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testPartitionsTableValidatesHiddenDataTableWithOverridePrecedence() throws Exception {
        FileStoreTable unsafeDataTable = newFileStoreTable(
                "partitions", ImmutableMap.of("scan.manifest.parallelism", "0"), null);
        PaimonSysExternalTable systemTable = newSystemTable(unsafeDataTable);

        try {
            systemTable.getSysPaimonTable();
            Assert.fail("$partitions must reject an unsafe hidden data table without an override");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("scan.manifest.parallelism"));
        }

        TableScanParams safeOverride = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.manifest.parallelism", "1"),
                Collections.emptyList());
        Assert.assertTrue(systemTable.getSysPaimonTable(safeOverride) instanceof PartitionsTable);
        Assert.assertFalse(systemTable.isDataTable());
        // Descriptor serialization happens after relation binding and must not fall back to
        // validating the unsafe physical handle that the relation override already replaced.
        Assert.assertNotNull(systemTable.toThrift());
    }

    @Test
    public void testFetchRowCountValidatesBeforePlanning() {
        AtomicBoolean planningStarted = new AtomicBoolean(false);
        FileStoreTable unsafeDataTable = newFileStoreTable(
                "row_count", ImmutableMap.of("scan.manifest.parallelism", "0"), planningStarted);
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(externalTable).makeSureInitialized();

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(unsafeDataTable);
            try {
                externalTable.fetchRowCount();
                Assert.fail("row-count planning must reject unsafe manifest parallelism");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("scan.manifest.parallelism"));
            }
        }
        Assert.assertFalse(planningStarted.get());
    }

    @Test
    public void testExternalRowCountCacheUsesGuardedPaimonFetch() {
        AtomicBoolean planningStarted = new AtomicBoolean(false);
        FileStoreTable unsafeDataTable = newFileStoreTable(
                "row_count_cache", ImmutableMap.of("scan.manifest.parallelism", "0"), planningStarted);
        PaimonExternalTable externalTable = Mockito.mock(
                PaimonExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(externalTable).makeSureInitialized();

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class);
                MockedStatic<StatisticsUtil> statisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            paimonUtils.when(() -> PaimonUtils.getPaimonTable(externalTable)).thenReturn(unsafeDataTable);
            statisticsUtil.when(() -> StatisticsUtil.findTable(1, 2, 3)).thenReturn(externalTable);
            ExternalRowCountCache cache = new ExternalRowCountCache(MoreExecutors.newDirectExecutorService());

            Assert.assertEquals(TableIf.UNKNOWN_ROW_COUNT, cache.getCachedRowCount(1, 2, 3, false));
        }
        Assert.assertFalse(planningStarted.get());
    }

    private PaimonSysExternalTable newSystemTable(FileStoreTable dataTable) {
        PaimonExternalTable sourceTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getRemoteName()).thenReturn("db");
        Mockito.when(sourceTable.getId()).thenReturn(10L);
        Mockito.when(sourceTable.getName()).thenReturn("source");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("source");
        Mockito.when(sourceTable.getCatalog()).thenReturn(catalog);
        Mockito.when(sourceTable.getDatabase()).thenReturn(database);
        Mockito.when(sourceTable.getPaimonCatalogType())
                .thenReturn(PaimonExternalCatalog.PAIMON_FILESYSTEM);
        Mockito.doReturn(dataTable).when(sourceTable).getBasePaimonTable();
        return new PaimonSysExternalTable(sourceTable, "partitions");
    }

    private FileStoreTable newFileStoreTable(
            String name, Map<String, String> options, AtomicBoolean planningStarted) {
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                options,
                null);
        return new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class), new Path("memory://" + name), schema, CatalogEnvironment.empty()) {
            @Override
            public ReadBuilder newReadBuilder() {
                if (planningStarted != null) {
                    planningStarted.set(true);
                }
                return super.newReadBuilder();
            }
        };
    }

    private FileStoreTable newPartitionedFileStoreTable(String name) {
        TableSchema schema = new TableSchema(
                0,
                Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new IntType())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        return new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class), new Path("memory://" + name), schema, CatalogEnvironment.empty());
    }
}
