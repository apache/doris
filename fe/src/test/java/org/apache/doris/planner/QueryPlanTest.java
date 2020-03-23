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

}
