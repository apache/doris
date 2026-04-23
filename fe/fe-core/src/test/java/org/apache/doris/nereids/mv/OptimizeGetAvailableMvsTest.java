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

package org.apache.doris.nereids.mv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test get available mvs after rewrite by rules
 */
public class OptimizeGetAvailableMvsTest extends SqlTestBase {

    @Test
    void testWhenNotPartitionPrune() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        Partition mockPartition = Mockito.mock(Partition.class);
        Mockito.when(mockPartition.getId()).thenReturn(1L);
        Mockito.when(mockPartition.getName()).thenReturn("mock_partition");
        Mockito.when(mockPartition.getState()).thenReturn(Partition.PartitionState.NORMAL);
        MaterializedIndex mockIndex = new MaterializedIndex(1L, IndexState.NORMAL);
        Mockito.when(mockPartition.getIndex(Mockito.anyLong())).thenReturn(mockIndex);
        DistributionInfo mockDistInfo = Mockito.mock(DistributionInfo.class);
        Mockito.when(mockDistInfo.getType()).thenReturn(DistributionInfo.DistributionInfoType.RANDOM);
        Mockito.when(mockPartition.getDistributionInfo()).thenReturn(mockDistInfo);

        Database db = (Database) Env.getCurrentEnv().getInternalCatalog().getDbNullable("test");
        for (String tableName : new String[] {"T2", "T3", "T4"}) {
            OlapTable table = (OlapTable) db.getTableNullable(tableName);
            OlapTable spyTable = Mockito.spy(table);
            Mockito.doReturn(mockPartition).when(spyTable).getPartition(Mockito.anyLong());
            // Check the real field, not the possibly-stubbed method (spy-of-spy scenario)
            Map<Long, Partition> realIdToPartition = Deencapsulation.getField(table, "idToPartition");
            if (realIdToPartition.isEmpty()) {
                Mockito.doReturn(Lists.newArrayList(1L)).when(spyTable).getPartitionIds();
            }
            Map<Long, org.apache.doris.catalog.Table> idToTable = Deencapsulation.getField(db, "idToTable");
            java.util.concurrent.ConcurrentMap<String, org.apache.doris.catalog.Table> nameToTable
                    = Deencapsulation.getField(db, "nameToTable");
            idToTable.put(spyTable.getId(), spyTable);
            nameToTable.put(spyTable.getName(), spyTable);
        }

        installValidRelationManager();
        connectContext.getState().setIsQuery(true);

        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 "
                + "        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        PARTITION BY (id)\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as "
                + "        select T4.id from T4 inner join T2 "
                + "        on T4.id = T2.id;");
        mockCandidateMtmv("mv1");
        CascadesContext c1 = createCascadesContext(
                "select T4.id "
                        + "from T4 "
                        + "inner join T2 on T4.id = T2.id "
                        + "inner join T3 on T4.id = T3.id",
                connectContext
        );
        PlanChecker.from(c1)
                .setIsQuery()
                .analyze()
                .rewrite()
                .preMvRewrite()
                .optimize()
                .printlnBestPlanTree();
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = c1.getStatementContext()
                .getTableUsedPartitionNameMap();
        Assertions.assertFalse(tableUsedPartitionNameMap.isEmpty());

        for (Map.Entry<List<String>, Pair<RelationId, Set<String>>> tableInfoEntry
                : tableUsedPartitionNameMap.entries()) {
            if (tableInfoEntry.getKey().contains("T2")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T3")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            } else if (tableInfoEntry.getKey().contains("T4")) {
                Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
            }
        }

        Map<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap = c1.getStatementContext()
                .getMvCanRewritePartitionsMap();
        Assertions.assertEquals(1, mvCanRewritePartitionsMap.size());
        Assertions.assertTrue(mvCanRewritePartitionsMap.keySet().iterator().next().getTableName()
                .equalsIgnoreCase("mv1"));

        dropMvByNereids("drop materialized view mv1");
    }

    @Test
    void testWhenPartitionPrune() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        Partition mockPartition2 = Mockito.mock(Partition.class);
        Mockito.when(mockPartition2.getId()).thenReturn(1L);
        Mockito.when(mockPartition2.getName()).thenReturn("mock_partition");
        Mockito.when(mockPartition2.getState()).thenReturn(Partition.PartitionState.NORMAL);
        MaterializedIndex mockIndex2 = new MaterializedIndex(1L, IndexState.NORMAL);
        Mockito.when(mockPartition2.getIndex(Mockito.anyLong())).thenReturn(mockIndex2);
        DistributionInfo mockDistInfo2 = Mockito.mock(DistributionInfo.class);
        Mockito.when(mockDistInfo2.getType()).thenReturn(DistributionInfo.DistributionInfoType.RANDOM);
        Mockito.when(mockPartition2.getDistributionInfo()).thenReturn(mockDistInfo2);

        Database db2 = (Database) Env.getCurrentEnv().getInternalCatalog().getDbNullable("test");
        for (String tableName : new String[] {"T2", "T3", "T4"}) {
            OlapTable table = (OlapTable) db2.getTableNullable(tableName);
            OlapTable spyTable = Mockito.spy(table);
            Mockito.doReturn(mockPartition2).when(spyTable).getPartition(Mockito.anyLong());
            // Check the real field, not the possibly-stubbed method (spy-of-spy scenario)
            Map<Long, Partition> realIdToPartition = Deencapsulation.getField(table, "idToPartition");
            if (realIdToPartition.isEmpty()) {
                Mockito.doReturn(Lists.newArrayList(1L)).when(spyTable).getPartitionIds();
            }
            Map<Long, org.apache.doris.catalog.Table> idToTable = Deencapsulation.getField(db2, "idToTable");
            java.util.concurrent.ConcurrentMap<String, org.apache.doris.catalog.Table> nameToTable
                    = Deencapsulation.getField(db2, "nameToTable");
            idToTable.put(spyTable.getId(), spyTable);
            nameToTable.put(spyTable.getName(), spyTable);
        }

        try (MockedStatic<PartitionPruner> mockedPruner = Mockito.mockStatic(PartitionPruner.class,
                Mockito.CALLS_REAL_METHODS)) {
            mockedPruner.when(() -> PartitionPruner.<Long>prune(
                    Mockito.<List<Slot>>any(), Mockito.any(Expression.class),
                    Mockito.<Map<Long, PartitionItem>>any(),
                    Mockito.any(CascadesContext.class), Mockito.any(PartitionTableType.class),
                    Mockito.<Optional<SortedPartitionRanges<Long>>>any()))
                    .thenReturn(Pair.of(Lists.newArrayList(1L), Optional.empty()));

            installValidRelationManager();
            connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
            connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
            createMvByNereids("create materialized view mv2 "
                    + "        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "        PARTITION BY (id)\n"
                    + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "        PROPERTIES ('replication_num' = '1') \n"
                    + "        as "
                    + "        select T4.id from T4 inner join T2 "
                    + "        on T4.id = T2.id;");
            mockCandidateMtmv("mv2");
            CascadesContext c1 = createCascadesContext(
                    "select T4.id "
                        + "from T4 "
                        + "inner join T2 on T4.id = T2.id "
                        + "inner join T3 on T4.id = T3.id "
                        + "where T4.id > 0",
                    connectContext
            );
            PlanChecker.from(c1)
                .setIsQuery()
                .analyze()
                .rewrite()
                .preMvRewrite()
                .optimize()
                    .printlnBestPlanTree();
            Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = c1.getStatementContext()
                    .getTableUsedPartitionNameMap();
            Map<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap = c1.getStatementContext()
                    .getMvCanRewritePartitionsMap();
            Assertions.assertFalse(tableUsedPartitionNameMap.isEmpty());

            for (Map.Entry<List<String>, Pair<RelationId, Set<String>>> tableInfoEntry
                    : tableUsedPartitionNameMap.entries()) {
                if (tableInfoEntry.getKey().contains("T2")) {
                    Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
                } else if (tableInfoEntry.getKey().contains("T3")) {
                    Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
                } else if (tableInfoEntry.getKey().contains("T4")) {
                    Assertions.assertEquals(tableInfoEntry.getValue().value(), Sets.newHashSet("mock_partition"));
                }
            }

            Assertions.assertEquals(1, mvCanRewritePartitionsMap.size());
            Assertions.assertTrue(mvCanRewritePartitionsMap.keySet().iterator().next().getTableName()
                    .equalsIgnoreCase("mv2"));

            dropMvByNereids("drop materialized view mv2");
        }
    }
}
