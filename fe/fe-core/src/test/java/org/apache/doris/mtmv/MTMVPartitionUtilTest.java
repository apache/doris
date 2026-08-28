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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.ImmutableSet;
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
        refreshContextStatic.when(() -> MTMVRefreshContext.buildContext(Mockito.any(MTMV.class), Mockito.anyMap()))
                .thenReturn(context);
        refreshContextStatic.when(() -> MTMVRefreshContext.buildContextForDisplay(Mockito.any(MTMV.class)))
                .thenReturn(context);

        Mockito.when(mtmv.getRelation()).thenReturn(relation);

        Mockito.when(context.getMtmv()).thenReturn(mtmv);

        Mockito.when(context.getPartitionMappings()).thenReturn(Maps.newHashMap());

        Mockito.when(context.persistedPartitionSetsMatch(Mockito.anySet())).thenReturn(true);

        Mockito.when(context.hasPersistedPartitionSet(
                Mockito.anyString(), Mockito.any(MTMVRelatedTableIf.class), Mockito.anySet())).thenReturn(true);

        Mockito.when(context.getBaseVersions()).thenReturn(versions);

        Mockito.when(context.getBaseTableSnapshotCache()).thenReturn(Maps.newHashMap());

        Mockito.when(context.getBaseTable(Mockito.any(BaseTableInfo.class))).thenReturn(baseOlapTable);

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

        Mockito.when(context.getPartitionSnapshots(Mockito.eq(baseOlapTable), Mockito.anySet(),
                Mockito.any(Optional.class))).thenAnswer(invocation -> {
                    Map<String, MTMVSnapshotIf> result = Maps.newHashMap();
                    for (String partitionName : invocation.<Set<String>>getArgument(1)) {
                        result.put(partitionName, baseSnapshotIf);
                    }
                    return result;
                });

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
    public void testIsMTMVSyncForDisplay() {
        Assert.assertTrue(MTMVPartitionUtil.isMTMVSyncForDisplay(mtmv));
        refreshContextStatic.verify(() -> MTMVRefreshContext.buildContextForDisplay(mtmv));
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
        Mockito.when(context.hasPersistedPartitionSet(
                Mockito.anyString(), Mockito.any(MTMVRelatedTableIf.class), Mockito.anySet())).thenReturn(false);
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
    public void testIsMTMVPartitionSyncWithImmutableExcludedTriggerTables() throws AnalysisException {
        Map<MTMVRelatedTableIf, Set<String>> partitionMappings = Maps.newHashMap();
        partitionMappings.put(baseOlapTable, Sets.newHashSet("name2"));
        Mockito.when(context.getByPartitionName("name1")).thenReturn(partitionMappings);
        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(mtmvPartitionInfo.getPctTables()).thenReturn(Sets.newHashSet(baseOlapTable));

        Set<TableNameInfo> excludedTriggerTables = ImmutableSet.of();
        boolean isMTMVPartitionSync = MTMVPartitionUtil.isMTMVPartitionSync(context, "name1", baseTables,
                excludedTriggerTables);

        Assert.assertTrue(isMTMVPartitionSync);
        Assert.assertTrue(excludedTriggerTables.isEmpty());
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
        Assert.assertTrue(MTMVPartitionUtil.isTableExcluded(excludedTriggerTables,
                new BaseTableInfo(new TableNameInfo("ctl1", "db1", "table1"))));
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
    public void testGetBaseVersionsUsesMappedPartitions() throws AnalysisException {
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings = Maps.newHashMap();
        partitionMappings.put("mv1", pctMapping("p1"));

        assertFetchedPartitionNames(partitionMappings, Sets.newHashSet("p1", "p2", "p3"),
                Sets.newHashSet("p1"));
    }

    @Test
    public void testGetBaseVersionsDeduplicatesMappedPartitions() throws AnalysisException {
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings = Maps.newHashMap();
        partitionMappings.put("mv1", pctMapping("p1", "p2"));
        partitionMappings.put("mv2", pctMapping("p2", "p3"));

        assertFetchedPartitionNames(partitionMappings, Sets.newHashSet("p1", "p2", "p3", "p4"),
                Sets.newHashSet("p1", "p2", "p3"));
    }

    @Test
    public void testGetBaseVersionsUsesAllFullyMappedPartitions() throws AnalysisException {
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings = Maps.newHashMap();
        partitionMappings.put("mv1", pctMapping("p1", "p2", "p3"));

        assertFetchedPartitionNames(partitionMappings, Sets.newHashSet("p1", "p2", "p3"),
                Sets.newHashSet("p1", "p2", "p3"));
    }

    @Test
    public void testCachedBaseVersionsNeverUseCloudVersionRpcApis() throws AnalysisException {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);
            Mockito.when(mtmvPartitionInfo.getPctTables()).thenReturn(Sets.newHashSet(baseOlapTable));
            Mockito.when(baseTableInfo.isInternalTable()).thenReturn(true);
            Mockito.when(baseOlapTable.getId()).thenReturn(10L);
            Mockito.when(baseOlapTable.getCachedTableVersion()).thenReturn(11L);
            Mockito.when(baseOlapTable.getPartitionOrAnalysisException("p1")).thenReturn(p1);
            Mockito.when(p1.getName()).thenReturn("p1");
            Mockito.when(p1.getCachedVisibleVersion()).thenReturn(12L);
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> mappings = Maps.newHashMap();
            mappings.put("mv1", pctMapping("p1"));

            try (MockedStatic<Partition> partitionStatic = Mockito.mockStatic(Partition.class);
                    MockedStatic<OlapTable> olapTableStatic = Mockito.mockStatic(OlapTable.class)) {
                MTMVBaseVersions cached = MTMVPartitionUtil.getCachedBaseVersions(
                        mtmv, mappings, baseTables);

                Assert.assertEquals(11L, cached.getTableVersions().get(10L).longValue());
                Assert.assertEquals(12L,
                        cached.getPartitionVersions(baseOlapTable).get("p1").longValue());
                partitionStatic.verify(
                        () -> Partition.getVisibleVersions(Mockito.anyList()), Mockito.never());
                olapTableStatic.verify(
                        () -> OlapTable.getVisibleVersionInBatch(Mockito.anyList()), Mockito.never());
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
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

    private Map<MTMVRelatedTableIf, Set<String>> pctMapping(String... partitionNames) {
        Map<MTMVRelatedTableIf, Set<String>> mapping = Maps.newHashMap();
        mapping.put(baseOlapTable, Sets.newHashSet(partitionNames));
        return mapping;
    }

    private void assertFetchedPartitionNames(
            Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings,
            Set<String> allPartitionNames, Set<String> expectedPartitionNames) throws AnalysisException {
        Mockito.when(mtmv.getRelation()).thenReturn(null);
        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(mtmvPartitionInfo.getPctTables()).thenReturn(Sets.newHashSet(baseOlapTable));

        Map<String, Partition> partitions = Maps.newHashMap();
        long visibleVersion = 1;
        for (String partitionName : allPartitionNames) {
            Partition partition = Mockito.mock(Partition.class);
            Mockito.when(partition.getName()).thenReturn(partitionName);
            Mockito.when(partition.getVisibleVersion()).thenReturn(visibleVersion++);
            Mockito.when(baseOlapTable.getPartitionOrAnalysisException(partitionName)).thenReturn(partition);
            partitions.put(partitionName, partition);
        }
        Mockito.when(baseOlapTable.getPartitions()).thenReturn(partitions.values());

        List<Set<String>> versionRequests = Lists.newArrayList();
        try (MockedStatic<Partition> partitionStatic = Mockito.mockStatic(Partition.class, Mockito.CALLS_REAL_METHODS)) {
            partitionStatic.when(() -> Partition.getVisibleVersions(Mockito.anyList())).thenAnswer(invocation -> {
                List<? extends Partition> requestedPartitions = invocation.getArgument(0);
                Set<String> requestedPartitionNames = Sets.newHashSet();
                List<Long> visibleVersions = Lists.newArrayList();
                for (Partition partition : requestedPartitions) {
                    requestedPartitionNames.add(partition.getName());
                    visibleVersions.add(partition.getVisibleVersion());
                }
                versionRequests.add(requestedPartitionNames);
                return visibleVersions;
            });

            Assert.assertEquals(expectedPartitionNames,
                    MTMVPartitionUtil.getBaseVersions(mtmv, partitionMappings)
                            .getPartitionVersions(baseOlapTable).keySet());
        }
        Assert.assertEquals(1, versionRequests.size());
        Assert.assertEquals(expectedPartitionNames, versionRequests.get(0));
        Mockito.verify(baseOlapTable, Mockito.never()).getPartitions();
    }
}
