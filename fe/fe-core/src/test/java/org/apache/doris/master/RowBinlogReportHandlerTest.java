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

package org.apache.doris.master;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;

public class RowBinlogReportHandlerTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 3L;
    private static final long BASE_INDEX_ID = 4L;
    private static final long ROW_BINLOG_INDEX_ID = 5L;
    private static final long ROLLUP_INDEX_ID = 6L;
    private static final long ORDINARY_TABLE_ID = 7L;
    private static final long ORDINARY_PARTITION_ID = 8L;
    private static final long ORDINARY_BASE_INDEX_ID = 9L;

    private MockedStatic<Env> mockedEnvStatic;
    private TabletInvertedIndex invertedIndex;

    @Before
    public void setUp() {
        Database db = new Database(DB_ID, "test_db");
        createRowBinlogTable(db);
        createOrdinaryTable(db);

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(db);
        invertedIndex = Mockito.mock(TabletInvertedIndex.class);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void storageMediumMigrationKeepsRollupAndOrdinaryTabletsOnly() {
        long baseTabletId = 100L;
        long rowBinlogTabletId = 101L;
        long rollupTabletId = 102L;
        long ordinaryTabletId = 103L;
        long missingTabletId = 104L;
        Mockito.when(invertedIndex.getTabletMeta(baseTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                BASE_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(rowBinlogTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                ROW_BINLOG_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(rollupTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                ROLLUP_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(ordinaryTabletId)).thenReturn(tabletMeta(ORDINARY_TABLE_ID,
                ORDINARY_PARTITION_ID, ORDINARY_BASE_INDEX_ID));

        ListMultimap<TStorageMedium, Long> migrationMap = LinkedListMultimap.create();
        migrationMap.put(TStorageMedium.SSD, baseTabletId);
        migrationMap.put(TStorageMedium.SSD, rowBinlogTabletId);
        migrationMap.put(TStorageMedium.SSD, rollupTabletId);
        migrationMap.put(TStorageMedium.SSD, ordinaryTabletId);
        migrationMap.put(TStorageMedium.SSD, missingTabletId);

        ReportHandler.filterRowBinlogPairedTabletMigration(migrationMap, 10001L);

        Assert.assertEquals(2, migrationMap.size());
        Assert.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, rollupTabletId));
        Assert.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, ordinaryTabletId));
    }

    private void createRowBinlogTable(Database db) {
        MaterializedIndex baseIndex = new MaterializedIndex(BASE_INDEX_ID, IndexState.NORMAL);
        Partition partition = new Partition(PARTITION_ID, "p0", baseIndex, new HashDistributionInfo());
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(ROW_BINLOG_INDEX_ID, IndexState.NORMAL);
        rowBinlogIndex.setIsRowBinlog(true);
        partition.createRollupIndex(rowBinlogIndex);
        partition.createRollupIndex(new MaterializedIndex(ROLLUP_INDEX_ID, IndexState.NORMAL));

        OlapTable table = new OlapTable(TABLE_ID, "row_binlog_table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        table.addPartition(partition);
        db.registerTable(table);
    }

    private void createOrdinaryTable(Database db) {
        MaterializedIndex baseIndex = new MaterializedIndex(ORDINARY_BASE_INDEX_ID, IndexState.NORMAL);
        Partition partition = new Partition(ORDINARY_PARTITION_ID, "p0", baseIndex, new HashDistributionInfo());
        OlapTable table = new OlapTable(ORDINARY_TABLE_ID, "ordinary_table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        table.addPartition(partition);
        db.registerTable(table);
    }

    private TabletMeta tabletMeta(long tableId, long partitionId, long indexId) {
        return new TabletMeta(DB_ID, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
    }
}
