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

package org.apache.doris.planner;

import org.apache.doris.common.Config;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class StatisticDeriveTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_odbc_mysql_broker_table = true;
        // create database
        createDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createTable(
                "CREATE TABLE test.join1 (\n"
                        + "  `dt` int(11) COMMENT \"\",\n"
                        + "  `id` int(11) COMMENT \"\",\n"
                        + "  `value` bigint(8) COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`dt`, `id`)\n"
                        + "PARTITION BY RANGE(`dt`)\n"
                        + "(PARTITION p1 VALUES LESS THAN (\"10\"))\n"
                        + "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ");");

        createTable(
                "CREATE TABLE test.join2 (\n"
                        + "  `dt` int(11) COMMENT \"\",\n"
                        + "  `id` int(11) COMMENT \"\",\n"
                        + "  `value` varchar(8) COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`dt`, `id`)\n"
                        + "PARTITION BY RANGE(`dt`)\n"
                        + "(PARTITION p1 VALUES LESS THAN (\"10\"))\n"
                        + "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ");");

        createTable("create external table test.odbc_oracle\n"
                + "(k1 float, k2 int)\n"
                + "ENGINE=ODBC\n"
                + "PROPERTIES (\n"
                + "\"host\" = \"127.0.0.1\",\n"
                + "\"port\" = \"3306\",\n"
                + "\"user\" = \"root\",\n"
                + "\"password\" = \"123\",\n"
                + "\"database\" = \"db1\",\n"
                + "\"table\" = \"tbl1\",\n"
                + "\"driver\" = \"Oracle Driver\",\n"
                + "\"odbc_type\" = \"oracle\"\n"
                + ");");

        createTable(
                "create external table test.odbc_mysql\n"
                        + "(k1 int, k2 int)\n"
                        + "ENGINE=ODBC\n"
                        + "PROPERTIES (\n"
                        + "\"host\" = \"127.0.0.1\",\n"
                        + "\"port\" = \"3306\",\n"
                        + "\"user\" = \"root\",\n"
                        + "\"password\" = \"123\",\n"
                        + "\"database\" = \"db1\",\n"
                        + "\"table\" = \"tbl1\",\n"
                        + "\"driver\" = \"Oracle Driver\",\n"
                        + "\"odbc_type\" = \"mysql\"\n"
                        + ");");

    }

    @Test
    public void testAggStatsDerive() throws Exception {
        // contain AggNode/OlapScanNode
        String sql = "select dt, max(id), value from test.join1 group by dt, value;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        assertSQLPlanOrErrorMsgContains(sql, "AGGREGATE");
        assertSQLPlanOrErrorMsgContains(sql, "OlapScanNode");
    }

    @Test
    public void testAnalyticEvalStatsDerive() throws Exception {
        // contain SortNode/ExchangeNode/OlapScanNode
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ dt, min(id) OVER (PARTITION BY dt ORDER BY id) from test.join1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "ANALYTIC");
        assertSQLPlanOrErrorMsgContains(sql, "SORT");
        assertSQLPlanOrErrorMsgContains(sql, "EXCHANGE");
    }

    @Test
    public void testAssertNumberRowsStatsDerive() throws Exception {
        // contain CrossJoinNode/ExchangeNode/AssertNumberRowsNode/AggNode/OlapScanNode
        String sql = "SELECT CASE\n"
                + "WHEN (\n"
                + "SELECT COUNT(*) / 2\n"
                + "FROM test.join1\n"
                + ") > id THEN (\n"
                + "SELECT AVG(id)\n"
                + "FROM test.join1\n"
                + ")\n"
                + "ELSE (\n"
                + "SELECT SUM(id)\n"
                + "FROM test.join1\n"
                + ")\n"
                + "END AS kk4\n"
                + "FROM test.join1;";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "NESTED LOOP JOIN");
        assertSQLPlanOrErrorMsgContains(sql, "EXCHANGE");
        assertSQLPlanOrErrorMsgContains(sql, "AGGREGATE");
        assertSQLPlanOrErrorMsgContains(sql, "OlapScanNode");
    }

    @Test
    public void testEmptySetStatsDerive() throws Exception {
        String sql = "select * from test.join1 where 1 = 2";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "EMPTYSET");
    }

    @Test
    public void testRepeatStatsDerive() throws Exception {
        String sql = "select dt, id, sum(value) from test.join1 group by rollup (dt, id)";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "REPEAT_NODE");
    }

    @Test
    public void testHashJoinStatsDerive() throws Exception {
        // contain HashJoinNode/ExchangeNode/OlapScanNode
        String sql = "select * from test.join1 a inner join test.join2 b on a.dt = b.dt";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "HASH JOIN");
    }

    @Test
    public void testOdbcScanStatsDerive() throws Exception {
        String sql = "select * from test.odbc_mysql";
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "SCAN ODBC");
    }

    @Test
    public void testTableFunctionStatsDerive() throws Exception {
        String sql = "select * from test.join2 lateral view explode_split(value, \",\") tmp as e1";
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "TABLE FUNCTION NODE");
    }

    @Test
    public void testUnionStatsDerive() throws Exception {
        String sql = "select * from test.join1 union select * from test.join2";
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableJoinReorderBasedCost(true);
        sessionVariable.setDisableJoinReorder(false);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assert.assertNotNull(stmtExecutor.planner());
        Assert.assertNotNull(stmtExecutor.planner().getFragments());
        Assert.assertNotEquals(0, stmtExecutor.planner().getFragments().size());
        System.out.println(getSQLPlanOrErrorMsg("explain " + sql));
        assertSQLPlanOrErrorMsgContains(sql, "UNION");
    }
}
