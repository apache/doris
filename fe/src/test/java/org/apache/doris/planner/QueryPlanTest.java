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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class QueryPlanTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/QueryPlanTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        
        createTable("create table test.test1\n" + 
                "(\n" + 
                "    query_id varchar(48) comment \"Unique query id\",\n" + 
                "    time datetime not null comment \"Query start time\",\n" + 
                "    client_ip varchar(32) comment \"Client IP\",\n" + 
                "    user varchar(64) comment \"User name\",\n" + 
                "    db varchar(96) comment \"Database of this query\",\n" + 
                "    state varchar(8) comment \"Query result state. EOF, ERR, OK\",\n" + 
                "    query_time bigint comment \"Query execution time in millisecond\",\n" + 
                "    scan_bytes bigint comment \"Total scan bytes of this query\",\n" + 
                "    scan_rows bigint comment \"Total scan rows of this query\",\n" + 
                "    return_rows bigint comment \"Returned rows of this query\",\n" + 
                "    stmt_id int comment \"An incremental id of statement\",\n" + 
                "    is_query tinyint comment \"Is this statemt a query. 1 or 0\",\n" + 
                "    frontend_ip varchar(32) comment \"Frontend ip of executing this statement\",\n" + 
                "    stmt varchar(2048) comment \"The original statement, trimed if longer than 2048 bytes\"\n" + 
                ")\n" + 
                "partition by range(time) ()\n" + 
                "distributed by hash(query_id) buckets 1\n" + 
                "properties(\n" + 
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" + 
                "    \"dynamic_partition.start\" = \"-30\",\n" + 
                "    \"dynamic_partition.end\" = \"3\",\n" + 
                "    \"dynamic_partition.prefix\" = \"p\",\n" + 
                "    \"dynamic_partition.buckets\" = \"1\",\n" + 
                "    \"dynamic_partition.enable\" = \"true\",\n" + 
                "    \"replication_num\" = \"1\"\n" + 
                ");");

        createTable("CREATE TABLE test.bitmap_table (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` bitmap bitmap_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.bitmap_table_2 (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` bitmap bitmap_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.hll_table (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` hll hll_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");
        
        createTable("CREATE TABLE test.`bigtable` (\n" +
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" + 
                "  `k2` smallint(6) NULL COMMENT \"\",\n" + 
                "  `k3` int(11) NULL COMMENT \"\",\n" + 
                "  `k4` bigint(20) NULL COMMENT \"\",\n" + 
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" + 
                "  `k6` char(5) NULL COMMENT \"\",\n" + 
                "  `k10` date NULL COMMENT \"\",\n" + 
                "  `k11` datetime NULL COMMENT \"\",\n" + 
                "  `k7` varchar(20) NULL COMMENT \"\",\n" + 
                "  `k8` double MAX NULL COMMENT \"\",\n" + 
                "  `k9` float SUM NULL COMMENT \"\"\n" + 
                ") ENGINE=OLAP\n" + 
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" + 
                "COMMENT \"OLAP\"\n" + 
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" + 
                "PROPERTIES (\n" + 
                "\"replication_num\" = \"1\"\n" + 
                ");");
        
        createTable("CREATE TABLE test.`baseall` (\n" + 
                "  `k1` tinyint(4) NULL COMMENT \"\",\n" + 
                "  `k2` smallint(6) NULL COMMENT \"\",\n" + 
                "  `k3` int(11) NULL COMMENT \"\",\n" + 
                "  `k4` bigint(20) NULL COMMENT \"\",\n" + 
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" + 
                "  `k6` char(5) NULL COMMENT \"\",\n" + 
                "  `k10` date NULL COMMENT \"\",\n" + 
                "  `k11` datetime NULL COMMENT \"\",\n" + 
                "  `k7` varchar(20) NULL COMMENT \"\",\n" + 
                "  `k8` double MAX NULL COMMENT \"\",\n" + 
                "  `k9` float SUM NULL COMMENT \"\"\n" + 
                ") ENGINE=OLAP\n" + 
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" + 
                "COMMENT \"OLAP\"\n" + 
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" + 
                "PROPERTIES (\n" + 
                "\"replication_num\" = \"1\"\n" + 
                ");");

        createTable("CREATE TABLE test.`dynamic_partition` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                "  `k3` int(11) NULL COMMENT \"\",\n" +
                "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `k6` char(5) NULL COMMENT \"\",\n" +
                "  `k10` date NULL COMMENT \"\",\n" +
                "  `k11` datetime NULL COMMENT \"\",\n" +
                "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                "  `k8` double MAX NULL COMMENT \"\",\n" +
                "  `k9` float SUM NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Test
    public void testBitmapInsertInto() throws Exception {
        String queryStr = "explain INSERT INTO test.bitmap_table (id, id2) VALUES (1001, to_bitmap(1000)), (1001, to_bitmap(2000));";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));

        queryStr = "explain insert into test.bitmap_table select id, bitmap_union(id2) from test.bitmap_table_2 group by id;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("bitmap_union"));
        Assert.assertTrue(explainString.contains("1:AGGREGATE"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode"));

        queryStr = "explain insert into test.bitmap_table select id, id2 from test.bitmap_table_2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:`id` | `id2`"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode"));

        queryStr = "explain insert into test.bitmap_table select id, to_bitmap(id2) from test.bitmap_table_2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:`id` | to_bitmap(`id2`)"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode"));

        queryStr = "explain insert into test.bitmap_table select id, bitmap_hash(id2) from test.bitmap_table_2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:`id` | bitmap_hash(`id2`)"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode"));

        queryStr = "explain insert into test.bitmap_table select id, id from test.bitmap_table_2;";
        String errorMsg = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(errorMsg.contains("bitmap column id2 require the function return type is BITMAP"));
    }

    private static void testBitmapQueryPlan(String sql, String result) throws Exception {
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains(result));
    }

    @Test
    public void testBitmapQuery() throws Exception {
        testBitmapQueryPlan(
                "select * from test.bitmap_table;",
                "OUTPUT EXPRS:`default_cluster:test.bitmap_table`.`id` | `default_cluster:test.bitmap_table`.`id2`"
        );

        testBitmapQueryPlan(
                "select count(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testBitmapQueryPlan(
                "select group_concat(id2) from test.bitmap_table;",
                "group_concat requires first parameter to be of type STRING: group_concat(`id2`)"
        );

        testBitmapQueryPlan(
                "select sum(id2) from test.bitmap_table;",
                "sum requires a numeric parameter: sum(`id2`)"
        );

        testBitmapQueryPlan(
                "select avg(id2) from test.bitmap_table;",
                "avg requires a numeric parameter: avg(`id2`)"
        );

        testBitmapQueryPlan(
                "select max(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testBitmapQueryPlan(
                "select min(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testBitmapQueryPlan(
                "select count(*) from test.bitmap_table group by id2;",
                Type.OnlyMetricTypeErrorMsg
        );

        testBitmapQueryPlan(
                "select count(*) from test.bitmap_table where id2 = 1;",
                "type not match, originType=BITMAP, targeType=DOUBLE"
        );

    }

    private static void testHLLQueryPlan(String sql, String result) throws Exception {
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains(result));
    }

    @Test
    public void testHLLTypeQuery() throws Exception {
        testHLLQueryPlan(
                "select * from test.hll_table;",
                "OUTPUT EXPRS:`default_cluster:test.hll_table`.`id` | `default_cluster:test.hll_table`.`id2`"
        );

        testHLLQueryPlan(
                "select count(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testHLLQueryPlan(
                "select group_concat(id2) from test.hll_table;",
                "group_concat requires first parameter to be of type STRING: group_concat(`id2`)"
        );

        testHLLQueryPlan(
                "select sum(id2) from test.hll_table;",
                "sum requires a numeric parameter: sum(`id2`)"
        );

        testHLLQueryPlan(
                "select avg(id2) from test.hll_table;",
                "avg requires a numeric parameter: avg(`id2`)"
        );

        testHLLQueryPlan(
                "select max(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testHLLQueryPlan(
                "select min(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testHLLQueryPlan(
                "select min(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        testHLLQueryPlan(
                "select count(*) from test.hll_table group by id2;",
                Type.OnlyMetricTypeErrorMsg
        );

        testHLLQueryPlan(
                "select count(*) from test.hll_table where id2 = 1",
                "type not match, originType=HLL, targeType=DOUBLE"
        );
    }

    @Test
    public void testTypeCast() throws Exception {
        // cmy: this test may sometimes failed in our daily test env, so I add a case here.
        String sql = "select * from test.baseall a where k1 in (select k1 from test.bigtable b where k2 > 0 and k1 = 1);";
        UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertEquals(MysqlStateType.EOF, connectContext.getState().getStateType());
        
        sql = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'";
        List<StatementBase> stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(2, stmts.size());
    }

    @Test
    public void testMultiStmts() throws Exception {
        String sql = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'";
        List<StatementBase>stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(2, stmts.size());
        
        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(1, stmts.size());
        
        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;SHOW VARIABLES LIKE 'lower_case_%';";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(4, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%'";
        stmts = UtFrameUtils.parseAndAnalyzeStmts(sql, connectContext);
        Assert.assertEquals(1, stmts.size());
    }

    @Test
    public void testCountDistinctRewrite() throws Exception {
        String sql = "select count(distinct id) from test.bitmap_table";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("output: count"));

        sql = "select count(distinct id2) from test.bitmap_table";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.hll_table";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("hll_union_agg"));

        sql = "select sum(id) / count(distinct id2) from test.hll_table";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("hll_union_agg"));

        sql = "select count(distinct id2) from test.bitmap_table group by id order by count(distinct id2)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.bitmap_table order by count(distinct id2)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        ConnectContext.get().getSessionVariable().setRewriteCountDistinct(false);
        sql = "select count(distinct id2) from test.bitmap_table";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains(Type.OnlyMetricTypeErrorMsg));
    }

    @Test
    public void testCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String createSchemaSql = "create schema if not exists test";
        String createDbSql = "create database if not exists test";
        CreateDbStmt createSchemaStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createSchemaSql, connectContext);
        CreateDbStmt createDbStmt =  (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbSql, connectContext);
        Assert.assertEquals(createDbStmt.toSql(), createSchemaStmt.toSql());
    }

    @Test
    public void testDropDbQueryPlanWithSchemaSyntax() throws Exception {
        String dropSchemaSql = "drop schema if exists test";
        String dropDbSql = "drop database if exists test";
        DropDbStmt dropSchemaStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSchemaSql, connectContext);
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropDbSql, connectContext);
        Assert.assertEquals(dropDbStmt.toSql(), dropSchemaStmt.toSql());
    }

    @Test
    public void testShowCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String showCreateSchemaSql = "show create schema test";
        String showCreateDbSql = "show create database test";
        ShowCreateDbStmt showCreateSchemaStmt = (ShowCreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(showCreateSchemaSql, connectContext);
        ShowCreateDbStmt showCreateDbStmt = (ShowCreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(showCreateDbSql, connectContext);
        Assert.assertEquals(showCreateDbStmt.toSql(), showCreateSchemaStmt.toSql());
    }

    @Test
    public void testDateTypeCastSyntax() throws Exception {
        String castSql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        SelectStmt selectStmt =
                (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(castSql, connectContext);
        Expr rightExpr = selectStmt.getWhereClause().getChildren().get(1);
        Assert.assertTrue(rightExpr.getType().equals(Type.DATETIME));

        String castSql2 = "select str_to_date('11/09/2011', '%m/%d/%Y');";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + castSql2);
        Assert.assertTrue(explainString.contains("2011-11-09"));
        Assert.assertFalse(explainString.contains("2011-11-09 00:00:00"));
    }
}
