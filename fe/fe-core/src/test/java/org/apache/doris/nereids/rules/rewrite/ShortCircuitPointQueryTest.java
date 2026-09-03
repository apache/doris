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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

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
        createTable("CREATE TABLE tbl_multi_point_query (k1 INT, k2 INT, v INT) "
                + "UNIQUE KEY(k1, k2) DISTRIBUTED BY HASH(k1, k2) BUCKETS 3 PROPERTIES ("
                + "\"replication_num\" = \"1\", \"enable_unique_key_merge_on_write\" = \"true\", "
                + "\"light_schema_change\" = \"true\", \"store_row_column\" = \"true\")");
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

    @Test
    void testArrowFlightSqlConnectionDoesNotUseShortCircuit() throws Exception {
        // The short circuit hands its rows back through PointQueryExecutor, which registers no
        // FlightSqlEndpointsLocation and leaves no Arrow result on the BE, so GetFlightInfo used to fail
        // with "fetch arrow flight schema failed, no FlightSqlEndpointsLocations" and drop the row.
        // An Arrow Flight SQL connection has to plan the normal execution path. See #67368.
        Field connectTypeField = ConnectContext.class.getDeclaredField("connectType");
        connectTypeField.setAccessible(true);
        ConnectType originConnectType = (ConnectType) connectTypeField.get(connectContext);
        try {
            connectTypeField.set(connectContext, ConnectType.ARROW_FLIGHT_SQL);
            Plan plan = rewrite("select * from tbl_point_query where `key` = 1");

            Assertions.assertFalse(connectContext.getStatementContext().isShortCircuitQuery());
            // And the plan really is the ordinary one: tbl_point_query is empty, so it prunes to a
            // LogicalEmptyRelation, which is exactly what the short circuit suppresses in
            // testShortCircuitPointQueryKeepOlapScanWhenTableEmpty above.
            Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalEmptyRelation));
            Assertions.assertFalse(plan.anyMatch(p -> p instanceof LogicalOlapScan));
        } finally {
            connectTypeField.set(connectContext, originConnectType);
        }

        // The very same statement still short circuits on a MySQL connection.
        rewrite("select * from tbl_point_query where `key` = 1");
        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
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

    @Test
    void testEqualityKeepsOriginalPointQueryPath() {
        rewrite("select * from tbl_point_query where `key` = 1");
        Assertions.assertTrue(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertFalse(connectContext.getStatementContext().isMultiKeyPointQuery());
    }

    @Test
    void testLiteralInDoesNotUseMultiKeyPointQueryPath() {
        rewrite("select * from tbl_point_query where `key` in (1, 2, 2, null)");
        Assertions.assertFalse(connectContext.getStatementContext().isShortCircuitQuery());
        Assertions.assertFalse(connectContext.getStatementContext().isMultiKeyPointQuery());
    }

    @Test
    void testPreparedMultiKeyEligibility() {
        boolean original = Config.enable_point_query_multi_get;
        Config.enable_point_query_multi_get = true;
        try {
            assertPreparedMultiKey(true, "k1 IN (?, ?, ?) AND k2 = ?", 1, 2, 2, 10);
            assertPreparedMultiKey(true, "? = k2 AND k1 IN (?, ?)", 10, 1, 2);
            assertPreparedMultiKey(false, "k1 IN (?, ?, ?) AND k2 = ?", 1, 1, 1, 10);
            // Either IN may fold to equality, but neither binding may be mistaken for k = ?.
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 IN (?, ?)", 1, 1, 10, 20);
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 IN (?, ?)", 1, 2, 10, 10);
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 IN (?, ?)", 1, 2, 10, 20);
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 = ? AND v = ?", 1, 2, 10, 3);
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 = ? AND k2 = ?", 1, 2, 10, 10);
            assertPreparedMultiKey(false, "CAST(k1 AS STRING) IN (?, ?) AND k2 = ?", 1, 2, 10);
        } finally {
            Config.enable_point_query_multi_get = original;
        }
    }

    @Test
    void testPreparedMultiKeyDisabledByConfig() {
        boolean original = Config.enable_point_query_multi_get;
        Config.enable_point_query_multi_get = false;
        try {
            assertPreparedMultiKey(false, "k1 IN (?, ?) AND k2 = ?", 1, 2, 10);
        } finally {
            Config.enable_point_query_multi_get = original;
        }
    }

    @Test
    void testPreparedMultiKeyWithTableSampleUsesNormalPath() {
        boolean original = Config.enable_point_query_multi_get;
        Config.enable_point_query_multi_get = true;
        try {
            assertPreparedMultiKey(false, "TABLESAMPLE(1 ROWS)",
                    "k1 IN (?, ?) AND k2 = ?", 1, 2, 10);
            assertPreparedMultiKey(false, "TABLESAMPLE(1 ROWS) REPEATABLE 7",
                    "k1 IN (?, ?) AND k2 = ?", 1, 2, 10);
        } finally {
            Config.enable_point_query_multi_get = original;
        }
    }

    private void assertPreparedMultiKey(boolean expected, String predicate, int... values) {
        assertPreparedMultiKey(expected, "", predicate, values);
    }

    private void assertPreparedMultiKey(boolean expected, String tableModifier,
            String predicate, int... values) {
        boolean originRunningUnitTest = FeConstants.runningUnitTest;
        MysqlCommand originCommand = connectContext.getCommand();
        FeConstants.runningUnitTest = false;
        connectContext.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        try {
            LogicalPlanAdapter adapter = (LogicalPlanAdapter) new NereidsParser().parseSQL(
                    "SELECT k1, v FROM tbl_multi_point_query " + tableModifier
                            + " WHERE " + predicate).get(0);
            StatementContext statementContext = adapter.getStatementContext();
            statementContext.setConnectContext(connectContext);
            connectContext.setStatementContext(statementContext);
            Assertions.assertEquals(values.length, statementContext.getPlaceholders().size());
            for (int i = 0; i < values.length; i++) {
                statementContext.getIdToPlaceholderRealExpr().put(
                        statementContext.getPlaceholders().get(i).getPlaceholderId(), new IntegerLiteral(values[i]));
            }
            PlanChecker.from(MemoTestUtils.createCascadesContext(statementContext, adapter.getLogicalPlan()))
                    .analyze().rewrite();
            Assertions.assertEquals(expected, statementContext.isMultiKeyPointQuery(), predicate);
            Assertions.assertEquals(expected, statementContext.isShortCircuitQuery(), predicate);
        } finally {
            connectContext.setCommand(originCommand);
            FeConstants.runningUnitTest = originRunningUnitTest;
        }
    }
}
