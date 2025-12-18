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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.Collections;
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
