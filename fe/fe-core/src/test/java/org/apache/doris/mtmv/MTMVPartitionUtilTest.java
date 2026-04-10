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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MTMVPartitionUtilTest {
    private MTMV mtmv = Mockito.mock(MTMV.class);
    private Partition p1 = Mockito.mock(Partition.class);
    private MTMVRelation relation = Mockito.mock(MTMVRelation.class);
    private BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
    private MTMVPartitionInfo mtmvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
    private OlapTable baseOlapTable = Mockito.mock(OlapTable.class);
    private DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
    private CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
    private MTMVSnapshotIf baseSnapshotIf = Mockito.mock(MTMVSnapshotIf.class);
    private MTMVRefreshSnapshot refreshSnapshot = Mockito.mock(MTMVRefreshSnapshot.class);
    private MockedStatic<MTMVUtil> mtmvUtilStatic;
    private MockedStatic<MTMVRefreshContext> refreshContextStatic;
    private MTMVRefreshContext context = Mockito.mock(MTMVRefreshContext.class);
    private MTMVBaseVersions versions = Mockito.mock(MTMVBaseVersions.class);

    private Set<BaseTableInfo> baseTables = Sets.newHashSet();

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        baseTables.add(baseTableInfo);

        mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
        refreshContextStatic = Mockito.mockStatic(MTMVRefreshContext.class);
        refreshContextStatic.when(() -> MTMVRefreshContext.buildContext(Mockito.any(MTMV.class))).thenReturn(context);

        Mockito.when(mtmv.getRelation()).thenReturn(relation);

        Mockito.when(context.getMtmv()).thenReturn(mtmv);

        Mockito.when(context.getPartitionMappings()).thenReturn(Maps.newHashMap());

        Mockito.when(context.getBaseVersions()).thenReturn(versions);

        Mockito.when(context.getBaseTableSnapshotCache()).thenReturn(Maps.newHashMap());

        Mockito.when(mtmv.getPartitions()).thenReturn(Lists.newArrayList(p1));

        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet("name1"));

        Mockito.when(p1.getName()).thenReturn("name1");

        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mtmvPartitionInfo);

        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.SELF_MANAGE);

        mtmvUtilStatic.when(() -> MTMVUtil.getTable(Mockito.any(BaseTableInfo.class)))
                .thenReturn(baseOlapTable);

        Mockito.when(baseOlapTable.needAutoRefresh()).thenReturn(true);

        Mockito.when(baseOlapTable.getTableSnapshot(Mockito.any(MTMVRefreshContext.class), Mockito.any(Optional.class)))
                .thenReturn(baseSnapshotIf);

        Mockito.when(mtmv.getRefreshSnapshot()).thenReturn(refreshSnapshot);

        Mockito.when(refreshSnapshot.equalsWithBaseTable(Mockito.anyString(), Mockito.any(BaseTableInfo.class), Mockito.any(MTMVSnapshotIf.class)))
                .thenReturn(true);

        Mockito.when(relation.getBaseTablesOneLevelAndFromView()).thenReturn(baseTables);

        Mockito.when(baseOlapTable.getPartitionSnapshot(Mockito.anyString(), Mockito.any(MTMVRefreshContext.class), Mockito.any(Optional.class)))
                .thenReturn(baseSnapshotIf);

        Mockito.when(refreshSnapshot.equalsWithPct(Mockito.anyString(), Mockito.anyString(), Mockito.any(MTMVSnapshotIf.class),
                Mockito.any(BaseTableInfo.class)))
                .thenReturn(true);

        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.anyString(), Mockito.any(BaseTableInfo.class)))
                .thenReturn(Sets.newHashSet("name2"));

        Mockito.when(baseOlapTable.getName()).thenReturn("t1");

        Mockito.when(baseOlapTable.getDatabase()).thenReturn(databaseIf);

        Mockito.when(databaseIf.getFullName()).thenReturn("db1");

        Mockito.when(databaseIf.getCatalog()).thenReturn(catalogIf);

        Mockito.when(catalogIf.getName()).thenReturn("ctl1");
    }

    @After
    public void tearDown() {
        mtmvUtilStatic.close();
        refreshContextStatic.close();
    }

    @Test
    public void testIsMTMVSyncNormal() {
        boolean mtmvSync = MTMVPartitionUtil.isMTMVSync(mtmv);
        Assert.assertTrue(mtmvSync);
    }

    @Test
    public void testIsMTMVSyncNotSync() {
        Mockito.when(refreshSnapshot.equalsWithBaseTable(Mockito.anyString(), Mockito.any(BaseTableInfo.class), Mockito.any(MTMVSnapshotIf.class)))
                .thenReturn(false);
        boolean mtmvSync = MTMVPartitionUtil.isMTMVSync(mtmv);
        Assert.assertFalse(mtmvSync);
    }

    @Test
    public void testIsSyncWithPartition() throws AnalysisException {
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(context, "name1", Sets.newHashSet("name2"), baseOlapTable);
        Assert.assertTrue(isSyncWithPartition);
    }

    @Test
    public void testIsSyncWithPartitionNotEqual() throws AnalysisException {
        Mockito.when(refreshSnapshot.getPctSnapshots(Mockito.anyString(), Mockito.any(BaseTableInfo.class)))
                .thenReturn(Sets.newHashSet("name2", "name3"));
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(context, "name1", Sets.newHashSet("name2"), baseOlapTable);
        Assert.assertFalse(isSyncWithPartition);
    }

    @Test
    public void testIsSyncWithPartitionNotSync() throws AnalysisException {
        Mockito.when(refreshSnapshot.equalsWithPct(Mockito.anyString(), Mockito.anyString(), Mockito.any(MTMVSnapshotIf.class),
                Mockito.any(BaseTableInfo.class)))
                .thenReturn(false);
        boolean isSyncWithPartition = MTMVPartitionUtil
                .isSyncWithPartitions(context, "name1", Sets.newHashSet("name2"), baseOlapTable);
        Assert.assertFalse(isSyncWithPartition);
    }

    @Test
    public void testGeneratePartitionName() {
        List<List<PartitionValue>> inValues = Lists.newArrayList();
        inValues.add(Lists.newArrayList(new PartitionValue("20201010 01:01:01"), new PartitionValue("value12")));
        inValues.add(Lists.newArrayList(new PartitionValue("value21"), new PartitionValue("value22")));
        PartitionKeyDesc inDesc = PartitionKeyDesc.createIn(inValues);
        String inName = MTMVPartitionUtil.generatePartitionName(inDesc);
        Assert.assertEquals("p_20201010010101_value12_value21_value22", inName);

        PartitionKeyDesc rangeDesc = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue(1L)),
                Lists.newArrayList(new PartitionValue(2L))
        );
        String rangeName = MTMVPartitionUtil.generatePartitionName(rangeDesc);
        Assert.assertEquals("p_1_2", rangeName);
    }

    @Test
    public void testIsTableExcluded() {
        Set<TableNameInfo> excludedTriggerTables = Sets.newHashSet(new TableNameInfo("table1"));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table1")));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db2", "table1")));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl2", "db1", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table2")));

        excludedTriggerTables = Sets.newHashSet(new TableNameInfo("db1.table1"));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db2", "table1")));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl2", "db1", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table2")));

        excludedTriggerTables = Sets.newHashSet(new TableNameInfo("ctl1.db1.table1"));
        Assert.assertTrue(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db2", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl2", "db1", "table1")));
        Assert.assertFalse(
                MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, new TableNameInfo("ctl1", "db1", "table2")));
    }

    @Test
    public void testIsTableNamelike() {
        TableNameInfo tableNameToCheck = new TableNameInfo("ctl1", "db1", "table1");
        Assert.assertTrue(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("table1"), tableNameToCheck));
        Assert.assertTrue(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("db1.table1"), tableNameToCheck));
        Assert.assertTrue(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl1.db1.table1"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl1.table1"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl1.db2.table1"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl1.db1.table2"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl2.db1.table1"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("db1"), tableNameToCheck));
        Assert.assertFalse(MTMVPartitionUtil.isTableNamelike(new TableNameInfo("ctl1"), tableNameToCheck));
    }

    @Test
    public void testGetTableSnapshotFromContext() throws AnalysisException {
        Map<BaseTableInfo, MTMVSnapshotIf> cache = Maps.newHashMap();
        Mockito.when(context.getBaseTableSnapshotCache()).thenReturn(cache);
        Assert.assertTrue(cache.isEmpty());
        MTMVPartitionUtil.getTableSnapshotFromContext(baseOlapTable, context);
        Assert.assertEquals(1, cache.size());
        Assert.assertEquals(baseSnapshotIf, cache.values().iterator().next());
    }
}
