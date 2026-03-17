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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionCompensatorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("partition_compensate_test");
        useDatabase("partition_compensate_test");

        createTable("CREATE TABLE `lineitem_list_partition` (\n"
                + "      `l_orderkey` BIGINT not NULL,\n"
                + "      `l_linenumber` INT NULL,\n"
                + "      `l_partkey` INT NULL,\n"
                + "      `l_suppkey` INT NULL,\n"
                + "      `l_quantity` DECIMAL(15, 2) NULL,\n"
                + "      `l_extendedprice` DECIMAL(15, 2) NULL,\n"
                + "      `l_discount` DECIMAL(15, 2) NULL,\n"
                + "      `l_tax` DECIMAL(15, 2) NULL,\n"
                + "      `l_returnflag` VARCHAR(1) NULL,\n"
                + "      `l_linestatus` VARCHAR(1) NULL,\n"
                + "      `l_commitdate` DATE NULL,\n"
                + "      `l_receiptdate` DATE NULL,\n"
                + "      `l_shipinstruct` VARCHAR(25) NULL,\n"
                + "      `l_shipmode` VARCHAR(10) NULL,\n"
                + "      `l_comment` VARCHAR(44) NULL,\n"
                + "      `l_shipdate` DATE NULL\n"
                + "    ) ENGINE=OLAP\n"
                + "    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )\n"
                + "    COMMENT 'OLAP'\n"
                + "    PARTITION BY list(l_orderkey) (\n"
                + "    PARTITION p1 VALUES in ('1'),\n"
                + "    PARTITION p2 VALUES in ('2'),\n"
                + "    PARTITION p3 VALUES in ('3')\n"
                + "    )\n"
                + "    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n"
                + "    PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + "    )");

        createTable("CREATE TABLE `orders_list_partition` (\n"
                + "      `o_orderkey` BIGINT not NULL,\n"
                + "      `o_custkey` INT NULL,\n"
                + "      `o_orderstatus` VARCHAR(1) NULL,\n"
                + "      `o_totalprice` DECIMAL(15, 2)  NULL,\n"
                + "      `o_orderpriority` VARCHAR(15) NULL,\n"
                + "      `o_clerk` VARCHAR(15) NULL,\n"
                + "      `o_shippriority` INT NULL,\n"
                + "      `o_comment` VARCHAR(79) NULL,\n"
                + "      `o_orderdate` DATE NULL\n"
                + "    ) ENGINE=OLAP\n"
                + "    DUPLICATE KEY(`o_orderkey`, `o_custkey`)\n"
                + "    COMMENT 'OLAP'\n"
                + "    PARTITION BY list(o_orderkey) (\n"
                + "    PARTITION p1 VALUES in ('1'),\n"
                + "    PARTITION p2 VALUES in ('2'),\n"
                + "    PARTITION p3 VALUES in ('3'),\n"
                + "    PARTITION p4 VALUES in ('4')\n"
                + "    )\n"
                + "    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 3\n"
                + "    PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + "    )");

        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE,PRUNE_EMPTY_PARTITION,ELIMINATE_GROUP_BY_KEY_BY_UNIFORM,"
                        + "ELIMINATE_CONST_JOIN_CONDITION");
    }

    // Test when join both side are all partition table and partition column name is same
    @Test
    public void testGetQueryTableUsedPartition() {
        PlanChecker.from(connectContext)
                .checkExplain("select l1.*, O_CUSTKEY \n"
                                + "from lineitem_list_partition l1\n"
                                + "left outer join orders_list_partition\n"
                                + "on l1.l_shipdate = o_orderdate\n",
                        nereidsPlanner -> {
                            MaterializedViewUtils.collectTableUsedPartitions(nereidsPlanner.getRewrittenPlan(),
                                    nereidsPlanner.getCascadesContext());
                            Map<List<String>, Set<String>> queryUsedPartitions
                                    = PartitionCompensator.getQueryUsedPartitions(
                                            nereidsPlanner.getCascadesContext().getStatementContext(), new BitSet());

                            List<String> itmeQualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "lineitem_list_partition");
                            Set<String> queryTableUsedPartition = queryUsedPartitions.get(itmeQualifier);
                            Assertions.assertEquals(queryTableUsedPartition, ImmutableSet.of("p1", "p2", "p3"));

                            List<String> orderQualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "orders_list_partition");
                            Set<String> orderTableUsedPartition = queryUsedPartitions.get(orderQualifier);
                            Assertions.assertEquals(orderTableUsedPartition, ImmutableSet.of("p1", "p2", "p3", "p4"));
                        });
    }

    @Test
    public void testGetAllTableUsedPartition() {
        PlanChecker.from(connectContext)
                .checkExplain("select l1.*, O_CUSTKEY \n"
                                + "from lineitem_list_partition l1\n"
                                + "left outer join orders_list_partition\n"
                                + "on l1.l_shipdate = o_orderdate\n",
                        nereidsPlanner -> {
                            MaterializedViewUtils.collectTableUsedPartitions(nereidsPlanner.getRewrittenPlan(),
                                    nereidsPlanner.getCascadesContext());
                            List<String> qualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "lineitem_list_partition");

                            Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap
                                    = connectContext.getStatementContext().getTableUsedPartitionNameMap();
                            tableUsedPartitionNameMap.put(qualifier, PartitionCompensator.ALL_PARTITIONS);

                            Map<List<String>, Set<String>> queryUsedPartitions
                                    = PartitionCompensator.getQueryUsedPartitions(
                                    nereidsPlanner.getCascadesContext().getStatementContext(), new BitSet());
                            Set<String> queryTableUsedPartition = queryUsedPartitions.get(qualifier);
                            // if tableUsedPartitionNameMap contain any PartitionCompensator.ALL_PARTITIONS
                            // consider query all partitions from table
                            Assertions.assertNull(queryTableUsedPartition);

                            List<String> orderQualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "orders_list_partition");
                            Set<String> orderTableUsedPartition = queryUsedPartitions.get(orderQualifier);
                            Assertions.assertEquals(orderTableUsedPartition, ImmutableSet.of("p1", "p2", "p3", "p4"));
                        });
    }

    @Test
    public void testGetAllTableUsedPartitionList() {
        PlanChecker.from(connectContext)
                .checkExplain("select l1.*, O_CUSTKEY \n"
                                + "from lineitem_list_partition l1\n"
                                + "left outer join orders_list_partition\n"
                                + "on l1.l_shipdate = o_orderdate\n",
                        nereidsPlanner -> {
                            MaterializedViewUtils.collectTableUsedPartitions(nereidsPlanner.getRewrittenPlan(),
                                    nereidsPlanner.getCascadesContext());
                            List<String> qualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "lineitem_list_partition");

                            Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap
                                    = connectContext.getStatementContext().getTableUsedPartitionNameMap();
                            tableUsedPartitionNameMap.removeAll(qualifier);
                            tableUsedPartitionNameMap.put(qualifier, PartitionCompensator.ALL_PARTITIONS);

                            Map<List<String>, Set<String>> queryUsedPartitions
                                    = PartitionCompensator.getQueryUsedPartitions(
                                            nereidsPlanner.getCascadesContext().getStatementContext(), new BitSet());
                            Set<String> queryTableUsedPartition = queryUsedPartitions.get(qualifier);
                            // if tableUsedPartitionNameMap contain only PartitionCompensator.ALL_PARTITIONS
                            // consider query all partitions from table
                            Assertions.assertNull(queryTableUsedPartition);

                            List<String> orderQualifier = ImmutableList.of(
                                    "internal", "partition_compensate_test", "orders_list_partition");
                            Set<String> orderTableUsedPartition = queryUsedPartitions.get(orderQualifier);
                            Assertions.assertEquals(orderTableUsedPartition, ImmutableSet.of("p1", "p2", "p3", "p4"));
                        });
    }

    @Test
    public void testNeedUnionRewriteUnpartitionedOrNoPctInfos() throws Exception {
        MaterializationContext ctx1 = mockCtx(
                PartitionType.UNPARTITIONED,
                ImmutableList.of(new BaseColInfo("c", newBaseTableInfo())),
                ImmutableSet.of(),
                false);
        StatementContext sc1 = Mockito.mock(StatementContext.class);
        Mockito.when(sc1.getTableUsedPartitionNameMap()).thenReturn(ArrayListMultimap.create());
        Assertions.assertFalse(PartitionCompensator.needUnionRewrite(ctx1, sc1));

        MaterializationContext ctx2 = mockCtx(
                PartitionType.RANGE,
                Collections.emptyList(),
                ImmutableSet.of(Mockito.mock(MTMVRelatedTableIf.class)),
                false);
        StatementContext sc2 = Mockito.mock(StatementContext.class);
        Mockito.when(sc2.getTableUsedPartitionNameMap()).thenReturn(ArrayListMultimap.create());
        Assertions.assertFalse(PartitionCompensator.needUnionRewrite(ctx2, sc2));
    }

    @Test
    public void testNeedUnionRewriteEmptyPctTables() throws Exception {
        MaterializationContext ctx = mockCtx(
                PartitionType.RANGE,
                ImmutableList.of(),
                Collections.emptySet(),
                false);
        StatementContext sc = Mockito.mock(StatementContext.class);
        Mockito.when(sc.getTableUsedPartitionNameMap()).thenReturn(ArrayListMultimap.create());
        Assertions.assertFalse(PartitionCompensator.needUnionRewrite(ctx, sc));
    }

    @Test
    public void testNeedUnionRewriteExternalNoPrune() throws Exception {
        MaterializationContext ctx = mockCtx(
                PartitionType.LIST,
                ImmutableList.of(new BaseColInfo("c", newBaseTableInfo())),
                ImmutableSet.of(Mockito.mock(MTMVRelatedTableIf.class)),
                true);
        StatementContext sc = Mockito.mock(StatementContext.class);
        Mockito.when(sc.getTableUsedPartitionNameMap()).thenReturn(ArrayListMultimap.create());
        Assertions.assertFalse(PartitionCompensator.needUnionRewrite(ctx, sc));
    }

    @Test
    public void testNeedUnionRewritePositive() throws Exception {
        MaterializationContext ctx = mockCtx(
                PartitionType.LIST,
                ImmutableList.of(new BaseColInfo("c", newBaseTableInfo())),
                ImmutableSet.of(Mockito.mock(MTMVRelatedTableIf.class)),
                false);
        StatementContext sc = Mockito.mock(StatementContext.class);
        Mockito.when(sc.getTableUsedPartitionNameMap()).thenReturn(ArrayListMultimap.create());
        Assertions.assertTrue(PartitionCompensator.needUnionRewrite(ctx, sc));
    }

    @Test
    public void testNotNeedUnionRewriteWhenAllPartitions() throws Exception {
        BaseTableInfo tableInfo = newBaseTableInfo();
        MaterializationContext ctx = mockCtx(
                PartitionType.LIST,
                ImmutableList.of(new BaseColInfo("c", tableInfo)),
                ImmutableSet.of(Mockito.mock(MTMVRelatedTableIf.class)),
                false);
        StatementContext sc = Mockito.mock(StatementContext.class);

        ArrayListMultimap<List<String>, Pair<RelationId, Set<String>>> t = ArrayListMultimap.create();
        t.put(ImmutableList.of(), PartitionCompensator.ALL_PARTITIONS);
        Mockito.when(sc.getTableUsedPartitionNameMap()).thenReturn(t);
        Assertions.assertFalse(PartitionCompensator.needUnionRewrite(ctx, sc));
    }

    @Test
    public void testGetQueryUsedPartitionsAllAndPartial() {
        // Prepare qualifiers
        List<String> lineitemQualifier = ImmutableList.of(
                "internal", "partition_compensate_test", "lineitem_list_partition");
        List<String> ordersQualifier = ImmutableList.of(
                "internal", "partition_compensate_test", "orders_list_partition");

        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap
                = connectContext.getStatementContext().getTableUsedPartitionNameMap();
        tableUsedPartitionNameMap.clear();

        tableUsedPartitionNameMap.put(lineitemQualifier, PartitionCompensator.ALL_PARTITIONS);

        RelationId ridA = new RelationId(1);
        RelationId ridB = new RelationId(2);
        tableUsedPartitionNameMap.put(ordersQualifier, Pair.of(ridA, ImmutableSet.of("p1", "p2")));
        tableUsedPartitionNameMap.put(ordersQualifier, Pair.of(ridB, ImmutableSet.of("p3")));

        Map<List<String>, Set<String>> result = PartitionCompensator.getQueryUsedPartitions(
                connectContext.getStatementContext(), new BitSet());
        Assertions.assertNull(result.get(lineitemQualifier)); // all partitions
        Assertions.assertEquals(ImmutableSet.of("p1", "p2", "p3"), result.get(ordersQualifier));

        BitSet filterRidA = new BitSet();
        filterRidA.set(ridA.asInt());
        Map<List<String>, Set<String>> resultRidA = PartitionCompensator.getQueryUsedPartitions(
                connectContext.getStatementContext(), filterRidA);
        Assertions.assertNull(resultRidA.get(lineitemQualifier));
        Assertions.assertEquals(ImmutableSet.of("p1", "p2"), resultRidA.get(ordersQualifier));

        BitSet filterRidB = new BitSet();
        filterRidB.set(ridB.asInt());
        Map<List<String>, Set<String>> resultRidB = PartitionCompensator.getQueryUsedPartitions(
                connectContext.getStatementContext(), filterRidB);
        Assertions.assertNull(resultRidB.get(lineitemQualifier));
        Assertions.assertEquals(ImmutableSet.of("p3"), resultRidB.get(ordersQualifier));

        tableUsedPartitionNameMap.put(ordersQualifier, PartitionCompensator.ALL_PARTITIONS);
        Map<List<String>, Set<String>> resultAllOrders = PartitionCompensator.getQueryUsedPartitions(
                connectContext.getStatementContext(), new BitSet());
        Assertions.assertNull(resultAllOrders.get(ordersQualifier));
    }

    @Test
    public void testGetQueryUsedPartitionsEmptyCollectionMeansNoPartitions() {
        List<String> qualifier = ImmutableList.of(
                "internal", "partition_compensate_test", "lineitem_list_partition");
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap
                = connectContext.getStatementContext().getTableUsedPartitionNameMap();
        tableUsedPartitionNameMap.clear();
        // Put an empty set via a distinct relation id to simulate no partitions used
        RelationId rid = new RelationId(3);
        tableUsedPartitionNameMap.put(qualifier, Pair.of(rid, ImmutableSet.of()));

        Map<List<String>, Set<String>> result = PartitionCompensator.getQueryUsedPartitions(
                connectContext.getStatementContext(), new BitSet());
        Assertions.assertEquals(ImmutableSet.of(), result.get(qualifier));
    }

    private static MaterializationContext mockCtx(
            PartitionType type,
            List<BaseColInfo> pctInfos,
            Set<MTMVRelatedTableIf> pctTables,
            boolean externalNoPrune) throws AnalysisException {

        MTMV mtmv = Mockito.mock(MTMV.class);
        PartitionInfo pi = Mockito.mock(PartitionInfo.class);
        Mockito.when(mtmv.getPartitionInfo()).thenReturn(pi);
        Mockito.when(pi.getType()).thenReturn(type);

        MTMVPartitionInfo mpi = Mockito.mock(MTMVPartitionInfo.class);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mpi);
        Mockito.when(mpi.getPctInfos()).thenReturn(pctInfos);
        Mockito.when(mpi.getPctTables()).thenReturn(pctTables);

        if (externalNoPrune) {
            HMSExternalTable ext = Mockito.mock(HMSExternalTable.class);
            Mockito.when(ext.supportInternalPartitionPruned()).thenReturn(false);
            Set<TableIf> tbls = new HashSet<>(pctTables);
            tbls.add(ext);
            Mockito.when(mpi.getPctTables()).thenReturn(
                    tbls.stream().map(MTMVRelatedTableIf.class::cast).collect(Collectors.toSet()));
        }

        AsyncMaterializationContext ctx = Mockito.mock(AsyncMaterializationContext.class);
        Mockito.when(ctx.getMtmv()).thenReturn(mtmv);
        return ctx;
    }

    // Regression test for ConcurrentModificationException when merging partition maps
    // across multiple related tables. The bug was caused by calling replaceAll() inside
    // the for-loop after forEach() had already replaced map values with a shared Set
    // reference, causing a self-modification on the second iteration.
    // This test calls calcInvalidPartitions() directly with two related tables so that
    // the exact code path containing the bug is exercised end-to-end.
    @SuppressWarnings("unchecked")
    @Test
    public void testCalcInvalidPartitionsNoConcurrentModificationWithTwoRelatedTables()
            throws Exception {
        // Shared catalog/db for both related base tables
        CatalogIf<?> baseCatalog = Mockito.mock(CatalogIf.class);
        Mockito.when(baseCatalog.getName()).thenReturn("cat");
        Mockito.when(baseCatalog.getId()).thenReturn(1L);
        DatabaseIf<?> baseDb = Mockito.mock(DatabaseIf.class);
        Mockito.when(baseDb.getFullName()).thenReturn("db");
        Mockito.when(baseDb.getId()).thenReturn(2L);
        Mockito.when(baseDb.getCatalog()).thenReturn(baseCatalog);

        MTMVRelatedTableIf relatedTable1 = mockRelatedTableIf(
                "t1", 10L, ImmutableList.of("cat", "db", "t1"), baseDb);
        MTMVRelatedTableIf relatedTable2 = mockRelatedTableIf(
                "t2", 20L, ImmutableList.of("cat", "db", "t2"), baseDb);

        // Two MV valid partitions: mv_p1 maps to t1_p1, mv_p2 maps to t2_p2
        Partition mvP1 = Mockito.mock(Partition.class);
        Mockito.when(mvP1.getId()).thenReturn(101L);
        Mockito.when(mvP1.getName()).thenReturn("mv_p1");
        Partition mvP2 = Mockito.mock(Partition.class);
        Mockito.when(mvP2.getId()).thenReturn(102L);
        Mockito.when(mvP2.getName()).thenReturn("mv_p2");

        Map<String, Set<String>> mappingForTable1 = new HashMap<>();
        mappingForTable1.put("mv_p1", ImmutableSet.of("t1_p1"));
        Map<String, Set<String>> mappingForTable2 = new HashMap<>();
        mappingForTable2.put("mv_p2", ImmutableSet.of("t2_p2"));
        Map<MTMVRelatedTableIf, Map<String, Set<String>>> partitionMappings = new HashMap<>();
        partitionMappings.put(relatedTable1, mappingForTable1);
        partitionMappings.put(relatedTable2, mappingForTable2);

        BaseColInfo colInfo1 = new BaseColInfo("date_col", new BaseTableInfo(relatedTable1));
        BaseColInfo colInfo2 = new BaseColInfo("date_col", new BaseTableInfo(relatedTable2));

        // Separate catalog/db for the MV itself
        CatalogIf<?> mvCatalog = Mockito.mock(CatalogIf.class);
        Mockito.when(mvCatalog.getName()).thenReturn("internal");
        Mockito.when(mvCatalog.getId()).thenReturn(1L);
        DatabaseIf<?> mvDb = Mockito.mock(DatabaseIf.class);
        Mockito.when(mvDb.getFullName()).thenReturn("mv_db");
        Mockito.when(mvDb.getId()).thenReturn(3L);
        Mockito.when(mvDb.getCatalog()).thenReturn(mvCatalog);

        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getName()).thenReturn("mv1");
        Mockito.when(mtmv.getId()).thenReturn(100L);
        Mockito.when(mtmv.getDatabase()).thenReturn(mvDb);
        PartitionInfo mvPartitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(mtmv.getPartitionInfo()).thenReturn(mvPartitionInfo);
        Mockito.when(mvPartitionInfo.getType()).thenReturn(PartitionType.RANGE);
        MTMVPartitionInfo mvPctInfo = Mockito.mock(MTMVPartitionInfo.class);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mvPctInfo);
        Mockito.when(mvPctInfo.getPctTables()).thenReturn(ImmutableSet.of(relatedTable1, relatedTable2));
        Mockito.when(mvPctInfo.getPctInfos()).thenReturn(ImmutableList.of(colInfo1, colInfo2));
        // All MV partitions contain data
        Mockito.when(mtmv.selectNonEmptyPartitionIds(ArgumentMatchers.any())).thenReturn(ImmutableList.of(1L));

        AsyncMaterializationContext matCtx = Mockito.mock(AsyncMaterializationContext.class);
        Mockito.when(matCtx.getMtmv()).thenReturn(mtmv);
        Mockito.when(matCtx.calculatePartitionMappings()).thenReturn(partitionMappings);

        // StatementContext: the MV's two valid partitions are available for rewrite
        Map<BaseTableInfo, Collection<Partition>> canRewriteMap = new HashMap<>();
        canRewriteMap.put(new BaseTableInfo(mtmv), ImmutableList.of(mvP1, mvP2));
        StatementContext stmtCtx = Mockito.mock(StatementContext.class);
        Mockito.when(stmtCtx.getMvCanRewritePartitionsMap()).thenReturn(canRewriteMap);

        CascadesContext cascadesCtx = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesCtx.getStatementContext()).thenReturn(stmtCtx);

        // Rewritten plan has no MV scans, so mvNeedRemovePartitionNameSet stays empty
        Plan rewrittenPlan = Mockito.mock(Plan.class);
        Mockito.when(rewrittenPlan.collectToList(ArgumentMatchers.any())).thenReturn(Collections.emptyList());

        // Each table contributes one covered and one uncovered partition:
        //   t1 uses {t1_p1 (covered by mv_p1), t1_p2 (not covered)}
        //   t2 uses {t2_p1 (not covered), t2_p2 (covered by mv_p2)}
        Map<List<String>, Set<String>> queryUsedPartitions = new HashMap<>();
        queryUsedPartitions.put(ImmutableList.of("cat", "db", "t1"),
                ImmutableSet.of("t1_p1", "t1_p2"));
        queryUsedPartitions.put(ImmutableList.of("cat", "db", "t2"),
                ImmutableSet.of("t2_p1", "t2_p2"));

        // Must not throw ConcurrentModificationException when two related tables each
        // contribute entries that require the post-loop merge in calcInvalidPartitions()
        Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> result =
                Assertions.assertDoesNotThrow(() ->
                        PartitionCompensator.calcInvalidPartitions(
                                queryUsedPartitions, rewrittenPlan, matCtx, cascadesCtx));

        // The uncovered partitions from both tables should be merged into one unified set
        Assertions.assertNotNull(result);
        Set<String> expectedUnion = ImmutableSet.of("t1_p2", "t2_p1");
        result.value().values()
                .forEach(v -> Assertions.assertEquals(expectedUnion, v));
    }

    @SuppressWarnings("unchecked")
    private static MTMVRelatedTableIf mockRelatedTableIf(
            String tableName, long tableId, List<String> qualifiers, DatabaseIf<?> db) {
        MTMVRelatedTableIf table = Mockito.mock(MTMVRelatedTableIf.class);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getFullQualifiers()).thenReturn(qualifiers);
        return table;
    }

    private static BaseTableInfo newBaseTableInfo() {
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("internal");

        DatabaseIf<?> db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getId()).thenReturn(2L);
        Mockito.when(db.getFullName()).thenReturn("partition_compensate_test");
        Mockito.when(db.getCatalog()).thenReturn(catalog);

        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(3L);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getDatabase()).thenReturn(db);

        return new BaseTableInfo(table);
    }
}
