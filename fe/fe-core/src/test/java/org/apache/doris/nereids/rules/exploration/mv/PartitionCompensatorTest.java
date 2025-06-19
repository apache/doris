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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
}
