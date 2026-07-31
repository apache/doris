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

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.AllGrantedPrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class PaimonSysExternalTableTest {

    @Test
    public void testReadOptimizedSysTableKeepsTheFallbackPairVisible() throws Exception {
        // With file based privileges enabled PrivilegedCatalog#getTable hands the meta cache a
        // Privileged(FallbackRead(main, fallback)) - a shape Paimon's own catalog never lets a
        // system table see, because CatalogUtils#loadTable builds one over the raw
        // FileStoreTableFactory result and only wraps what it returns.
        FileStoreTable pair = newFallbackBranchPair();
        FileStoreTable cached = PrivilegedFileStoreTable.wrap(pair, new AllGrantedPrivilegeChecker(),
                Identifier.create("db", "tbl"));

        PaimonSysExternalTable sysTable = newSysTable(cached, "ro");
        Table feWrapper = sysTable.getSysPaimonTable();

        // The FE must plan tbl$ro over both branches.
        Assert.assertTrue(((ReadOptimizedTable) feWrapper).newScan()
                instanceof FallbackReadFileStoreTable.FallbackReadScan);
        // And so must the BE, which rebuilds its table from this one.
        Assert.assertSame(pair, sysTable.getSysBaseTable());

        // Why the decorator has to come off rather than stay: ReadOptimizedTable#newScan dispatches
        // on a direct instanceof, so with the privilege wrapper in between it plans the main branch
        // alone - through the pair's inherited newSnapshotReader() - and every fallback-only
        // partition silently produces no split at all.
        Assert.assertFalse(((ReadOptimizedTable) SystemTableLoader.load("ro", cached)).newScan()
                instanceof FallbackReadFileStoreTable.FallbackReadScan);
    }

    @Test
    public void testReadOptimizedSysTablePlansTheFallbackOnlyPartition() throws Exception {
        // What the assertion above stands for, end to end. FallbackReadScan#plan takes the main
        // branch's splits, then adds the fallback branch's splits only for the partitions main does
        // not have - so a partition living on the fallback branch alone is exactly what a
        // single-branch plan loses, and it loses it silently: tbl$ro still succeeds, it just
        // returns fewer rows.
        FileStoreTable[] branches = newFallbackBranchPairWithData();
        FileStoreTable cached = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(branches[0], branches[1]),
                new AllGrantedPrivilegeChecker(), Identifier.create("db", "tbl"));

        PaimonSysExternalTable sysTable = newSysTable(cached, "ro");

        Assert.assertEquals(new TreeSet<>(Arrays.asList(1, 2)),
                plannedPartitions((ReadOptimizedTable) sysTable.getSysPaimonTable()));
        // Leaving the privilege decorator in place takes ReadOptimizedTable#newScan down its
        // single-branch path, and pt=2 then produces no split at all.
        Assert.assertEquals(new TreeSet<>(Collections.singletonList(1)),
                plannedPartitions((ReadOptimizedTable) SystemTableLoader.load("ro", cached)));
    }

    @Test
    public void testSystemTableOptionsResolveOnTheCapturedBaseGeneration() throws Exception {
        // getSysPaimonTable() captures one base table and keeps the wrapper it built over it, but
        // the meta cache behind getBasePaimonTable() can be refreshed or invalidated in between.
        // Resolving the relation's OPTIONS there would freeze the newer generation's snapshot and
        // then apply it to the wrapper over the captured one - which is also the table
        // PaimonScanNode rebuilds from it for the BE.
        FileStoreTable captured = newTableWithSnapshots("doris_paimon_sys_captured_ut", 1);
        FileStoreTable refreshed = newTableWithSnapshots("doris_paimon_sys_refreshed_ut", 2);

        PaimonSysExternalTable sysTable = newSysTable(captured, "ro");
        Mockito.when(sysTable.getSourceTable().getBasePaimonTable()).thenReturn(refreshed);

        TableScanParams scanParams = new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of(CoreOptions.SCAN_MODE.key(), "latest"), Collections.emptyList());
        sysTable.getSysPaimonTable(scanParams);

        // Snapshot 1 is the captured generation's latest; the refreshed one is already at 2.
        Assert.assertEquals("1", scanParams.getResolvedMapParams().get()
                .get(CoreOptions.SCAN_SNAPSHOT_ID.key()));
    }

    /** A real table carrying {@code snapshots} committed generations. */
    private FileStoreTable newTableWithSnapshots(String prefix, int snapshots) throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory(prefix);
        Path tablePath = new Path("file://" + tempDir + "/db.db/tbl");
        LocalFileIO fileIO = LocalFileIO.create();
        new SchemaManager(fileIO, tablePath).createTable(new Schema(
                Collections.singletonList(new DataField(0, "c1", DataTypes.INT())),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), ""));
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath);
        for (int i = 0; i < snapshots; i++) {
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                write.write(GenericRow.of(i));
                commit.commit(write.prepareCommit());
            }
        }
        return table;
    }

    private Set<Integer> plannedPartitions(ReadOptimizedTable sysTable) {
        Set<Integer> partitions = new TreeSet<>();
        for (Split split : sysTable.newScan().plan().splits()) {
            partitions.add(((DataSplit) split).partition().getInt(0));
        }
        return partitions;
    }

    /**
     * A {@code {main, fallback}} pair holding real data: partition {@code pt=1} on the main branch
     * and {@code pt=2} on the fallback branch alone. The {@code scan.fallback-branch} option only
     * tells {@code FileStoreTableFactory} to build the pair - {@code FallbackReadScan} reads
     * nothing from it - so the two branches are wrapped directly here.
     */
    private FileStoreTable[] newFallbackBranchPairWithData() throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory("doris_paimon_sys_ro_data_ut");
        Path tablePath = new Path("file://" + tempDir + "/db.db/tbl");
        LocalFileIO fileIO = LocalFileIO.create();
        // No primary keys, so $ro reads every file of the snapshot and the fallback-only partition
        // shows up without depending on compaction having run.
        Schema schema = new Schema(
                Arrays.asList(new DataField(0, "pt", DataTypes.INT()), new DataField(1, "c1", DataTypes.INT())),
                Collections.singletonList("pt"), Collections.emptyList(), Collections.emptyMap(), "");
        new SchemaManager(fileIO, tablePath).createTable(schema);
        new SchemaManager(fileIO, tablePath, "fb").createTable(schema);

        FileStoreTable main = FileStoreTableFactory.create(fileIO, tablePath);
        commitPartition(main, 1);
        FileStoreTable fallback = main.switchToBranch("fb");
        commitPartition(fallback, 2);
        return new FileStoreTable[] {main, fallback};
    }

    private void commitPartition(FileStoreTable branch, int partition) throws Exception {
        BatchWriteBuilder writeBuilder = branch.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(partition, partition));
            commit.commit(write.prepareCommit());
        }
    }

    private PaimonSysExternalTable newSysTable(FileStoreTable cached, String sysTableType) {
        PaimonExternalDatabase db = Mockito.mock(PaimonExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        PaimonExternalTable sourceTable = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(sourceTable.getName()).thenReturn("tbl");
        Mockito.when(sourceTable.getRemoteName()).thenReturn("tbl");
        Mockito.when(sourceTable.getCatalog()).thenReturn(Mockito.mock(PaimonExternalCatalog.class));
        Mockito.when(sourceTable.getDatabase()).thenReturn(db);
        Mockito.when(sourceTable.getPaimonTable(Optional.empty())).thenReturn(cached);
        return new PaimonSysExternalTable(sourceTable, sysTableType);
    }

    /**
     * A {@code scan.fallback-branch} pair, built the way Paimon builds it: each branch without
     * re-expanding the fallback branch, then the two wrapped into a
     * {@link FallbackReadFileStoreTable}.
     */
    private FileStoreTable newFallbackBranchPair() throws Exception {
        java.nio.file.Path tempDir = Files.createTempDirectory("doris_paimon_sys_ro_ut");
        Path tablePath = new Path("file://" + tempDir + "/db.db/tbl");
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("scan.fallback-branch", "fb");
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        return new FallbackReadFileStoreTable(newBranchTable(tablePath, mainOptions),
                newBranchTable(tablePath, fallbackOptions));
    }

    private FileStoreTable newBranchTable(Path tablePath, Map<String, String> options) {
        List<DataField> fields = Collections.singletonList(new DataField(0, "c1", DataTypes.INT()));
        TableSchema schema = new TableSchema(0L, fields, 0, Collections.emptyList(),
                Collections.emptyList(), options, "");
        return FileStoreTableFactory.createWithoutFallbackBranch(LocalFileIO.create(), tablePath, schema,
                new Options(), CatalogEnvironment.empty());
    }
}
