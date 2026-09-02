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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test:
 * For short-circuit point query, we should not rewrite LogicalOlapScan to LogicalEmptyRelation
 * even if the table partitions are empty or partition pruning selects no partitions.
 * Current execution still needs the scan to initialize the point-query path; PreparedStatement
 * cacheability for empty selected partitions is handled separately.
 */
class ShortCircuitPointQueryTest extends TestWithFeService
        implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE `tbl_point_query` (\n"
                + "  `key` int(11) NULL,\n"
                + "  `v1` varchar(30) NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`key`)\n"
                + "DISTRIBUTED BY HASH(`key`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "  \"light_schema_change\" = \"true\",\n"
                + "  \"store_row_column\" = \"true\"\n"
                + ");");
        createTable("CREATE TABLE `tbl_partitioned_point_query` (\n"
                + "  `order_id` bigint NOT NULL,\n"
                + "  `pay_date` date NOT NULL,\n"
                + "  `v1` varchar(30) NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`order_id`, `pay_date`)\n"
                + "PARTITION BY RANGE(`pay_date`) (\n"
                + "  PARTITION `p20260805` VALUES [(\"2026-08-05\"), (\"2026-08-06\"))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`order_id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"enable_unique_key_merge_on_write\" = \"true\",\n"
                + "  \"light_schema_change\" = \"true\",\n"
                + "  \"store_row_column\" = \"true\"\n"
                + ");");
    }

    @Test
    void testShortCircuitPointQueryKeepOlapScanWhenTableEmpty() {
        Plan plan = rewrite("select * from tbl_point_query where `key` = 1");

        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalOlapScan));
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
    }

    @Test
    void testShortCircuitPointQueryKeepOlapScanWhenNoPartitionMatches() {
        Plan plan = rewrite("select * from tbl_partitioned_point_query "
                + "where order_id = 1 and pay_date = '2026-08-04'");

        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalOlapScan
                && ((LogicalOlapScan) p).isPartitionPruned()
                && ((LogicalOlapScan) p).getSelectedPartitionIds().isEmpty()
                && ((LogicalOlapScan) p).hasPartitionPredicate()));
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
    }

    @Test
    void testNonPointQueryWithNoMatchingPartitionPrunesToEmptyRelation() {
        Plan plan = rewrite("select * from tbl_partitioned_point_query "
                + "where pay_date = '2026-08-04'");

        Assertions.assertFalse(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalOlapScan));
    }

    @Test
    void testShortCircuitPointQueryWithMatchingPartitionKeepsSelectedPartition() {
        Plan plan = rewrite("select * from tbl_partitioned_point_query "
                + "where order_id = 1 and pay_date = '2026-08-05'");

        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalOlapScan
                && ((LogicalOlapScan) p).isPartitionPruned()
                && !((LogicalOlapScan) p).getSelectedPartitionIds().isEmpty()));
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
    }

    @Test
    void testPointQueryWithManualPartitionDoesNotUseShortCircuit() {
        rewrite("select * from tbl_partitioned_point_query partition(p20260805) "
                + "where order_id = 1 and pay_date = '2026-08-05'");

        Assertions.assertFalse(connectContext.getStatementContext().isShortCircuitQuery());
    }

    @Test
    void testPointQueryWithManualTabletDoesNotUseShortCircuit() throws Exception {
        long tabletId = getTabletId("p20260805");
        rewrite("select * from tbl_partitioned_point_query tablet(" + tabletId + ") "
                + "where order_id = 1 and pay_date = '2026-08-05'");

        Assertions.assertFalse(connectContext.getStatementContext().isShortCircuitQuery());
    }

    @Test
    void testRemoteOlapTableDoesNotUseShortCircuit() throws Exception {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) database.getTableOrMetaException("tbl_point_query");
        RemoteOlapTable remoteTable = RemoteOlapTable.fromOlapTable(table);
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), remoteTable);

        Assertions.assertTrue(connectContext.getSessionVariable().isEnableShortCircuitQuery());
        Assertions.assertTrue(remoteTable.getEnableLightSchemaChange());
        Assertions.assertTrue(remoteTable.getEnableUniqueKeyMergeOnWrite());
        Assertions.assertTrue(remoteTable.storeRowColumn());
        Assertions.assertFalse(new LogicalResultSinkToShortCircuitPointQuery()
                .scanMatchShortCircuitCondition(scan));
    }

    private long getTabletId(String partitionName) throws Exception {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) database.getTableOrMetaException("tbl_partitioned_point_query");
        Tablet tablet = table.getPartition(partitionName).getBaseIndex().getTablets().iterator().next();
        return tablet.getId();
    }

    private Plan rewrite(String sql) {
        boolean originRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            return PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .getPlan();
        } finally {
            FeConstants.runningUnitTest = originRunningUnitTest;
        }
    }
}
