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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
        invertedIndex = Mockito.mock(TabletInvertedIndex.class);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void storageMediumMigrationKeepsPairedBaseRollupAndOrdinaryTablets() {
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
        Mockito.when(invertedIndex.getTabletMeta(missingTabletId))
                .thenReturn(TabletInvertedIndex.NOT_EXIST_TABLET_META);

        ListMultimap<TStorageMedium, Long> migrationMap = LinkedListMultimap.create();
        migrationMap.put(TStorageMedium.SSD, baseTabletId);
        migrationMap.put(TStorageMedium.SSD, rowBinlogTabletId);
        migrationMap.put(TStorageMedium.SSD, rollupTabletId);
        migrationMap.put(TStorageMedium.SSD, ordinaryTabletId);
        migrationMap.put(TStorageMedium.SSD, missingTabletId);

        ReportHandler.filterRowBinlogTabletMigration(migrationMap, 10001L);

        Assert.assertEquals(3, migrationMap.size());
        Assert.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, baseTabletId));
        Assert.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, rollupTabletId));
        Assert.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, ordinaryTabletId));
        Assert.assertFalse(migrationMap.containsEntry(TStorageMedium.SSD, rowBinlogTabletId));
        Assert.assertFalse(migrationMap.containsEntry(TStorageMedium.SSD, missingTabletId));
    }

    private TabletMeta tabletMeta(long tableId, long partitionId, long indexId) {
        return new TabletMeta(DB_ID, tableId, partitionId, indexId, 0, TStorageMedium.HDD,
                indexId == ROW_BINLOG_INDEX_ID);
    }
}
