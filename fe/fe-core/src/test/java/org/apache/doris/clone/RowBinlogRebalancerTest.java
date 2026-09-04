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

package org.apache.doris.clone;

import org.apache.doris.catalog.ColocateTableIndex;
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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class RowBinlogRebalancerTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 3L;
    private static final long BASE_INDEX_ID = 4L;
    private static final long ROW_BINLOG_INDEX_ID = 5L;
    private static final long ROLLUP_INDEX_ID = 6L;
    private static final long ORDINARY_TABLE_ID = 7L;
    private static final long ORDINARY_PARTITION_ID = 8L;
    private static final long ORDINARY_BASE_INDEX_ID = 9L;

    private boolean previousRunningUnitTest;
    private MockedStatic<Env> mockedEnvStatic;

    @Before
    public void setUp() {
        previousRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;

        Database db = new Database(DB_ID, "test_db");
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(db);

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);
        Mockito.when(colocateTableIndex.isColocateTable(Mockito.anyLong())).thenReturn(false);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnvStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);

        MaterializedIndex baseIndex = new MaterializedIndex(BASE_INDEX_ID, IndexState.NORMAL);
        Partition partition = new Partition(PARTITION_ID, "p0", baseIndex, new HashDistributionInfo());
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(ROW_BINLOG_INDEX_ID, IndexState.NORMAL);
        rowBinlogIndex.setIsRowBinlog(true);
        partition.createRollupIndex(rowBinlogIndex);
        partition.createRollupIndex(new MaterializedIndex(ROLLUP_INDEX_ID, IndexState.NORMAL));

        OlapTable table = new OlapTable(TABLE_ID, "test_table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        table.addPartition(partition);
        db.registerTable(table);

        MaterializedIndex ordinaryBaseIndex = new MaterializedIndex(ORDINARY_BASE_INDEX_ID, IndexState.NORMAL);
        Partition ordinaryPartition = new Partition(ORDINARY_PARTITION_ID, "p0", ordinaryBaseIndex,
                new HashDistributionInfo());
        OlapTable ordinaryTable = new OlapTable(ORDINARY_TABLE_ID, "ordinary_table", new ArrayList<>(),
                KeysType.DUP_KEYS, new RangePartitionInfo(), new HashDistributionInfo());
        ordinaryTable.addPartition(ordinaryPartition);
        db.registerTable(ordinaryTable);
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
        FeConstants.runningUnitTest = previousRunningUnitTest;
    }

    @Test
    public void allLocalRebalancersSkipOnlyPairMembers() {
        SystemInfoService infoService = new SystemInfoService();
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        List<Rebalancer> rebalancers = Lists.newArrayList(
                new BeLoadRebalancer(infoService, invertedIndex, Maps.newHashMap()),
                new PartitionRebalancer(infoService, invertedIndex, Maps.newHashMap()),
                new DiskRebalancer(infoService, invertedIndex, Maps.newHashMap()));

        TabletMeta baseMeta = tabletMeta(BASE_INDEX_ID, false);
        TabletMeta rowBinlogMeta = tabletMeta(ROW_BINLOG_INDEX_ID, true);
        TabletMeta rollupMeta = tabletMeta(ROLLUP_INDEX_ID, false);
        // The same rollup is dynamically movable, so this isolates the TabletMeta fast-path filter.
        TabletMeta fastFilteredMeta = tabletMeta(ROLLUP_INDEX_ID, true);
        TabletMeta ordinaryBaseMeta = new TabletMeta(DB_ID, ORDINARY_TABLE_ID, ORDINARY_PARTITION_ID,
                ORDINARY_BASE_INDEX_ID, 0, TStorageMedium.HDD, false /* isRowBinlog */);
        for (Rebalancer rebalancer : rebalancers) {
            Assert.assertFalse(rebalancer.getClass().getSimpleName(), rebalancer.canBalanceTablet(baseMeta));
            Assert.assertFalse(rebalancer.getClass().getSimpleName(), rebalancer.canBalanceTablet(rowBinlogMeta));
            Assert.assertFalse(rebalancer.getClass().getSimpleName(), rebalancer.canBalanceTablet(fastFilteredMeta));
            Assert.assertTrue(rebalancer.getClass().getSimpleName(), rebalancer.canBalanceTablet(rollupMeta));
            Assert.assertTrue(rebalancer.getClass().getSimpleName(), rebalancer.canBalanceTablet(ordinaryBaseMeta));
        }
    }

    private TabletMeta tabletMeta(long indexId, boolean isRowBinlog) {
        return new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, indexId, 0, TStorageMedium.HDD, isRowBinlog);
    }
}
