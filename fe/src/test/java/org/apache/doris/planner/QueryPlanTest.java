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
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.lang3.StringUtils;
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

        createTable("CREATE TABLE test.join1 (\n" +
                "  `dt` int(11) COMMENT \"\",\n" +
                "  `id` int(11) COMMENT \"\",\n" +
                "  `value` varchar(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.join2 (\n" +
                "  `dt` int(11) COMMENT \"\",\n" +
                "  `id` int(11) COMMENT \"\",\n" +
                "  `value` varchar(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
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

        createTable("CREATE TABLE test.`app_profile` (\n" +
                "  `event_date` date NOT NULL COMMENT \"\",\n" +
                "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `age` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `gender` varchar(32) NOT NULL COMMENT \"\",\n" +
                "  `level` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `city` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `model` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `brand` varchar(64) NOT NULL COMMENT \"\",\n" +
                "  `hours` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n" +
                "  `use_time` double SUM NOT NULL COMMENT \"\",\n" +
                "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`)\n"
                +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`event_date`)\n" +
                "(PARTITION p_20200301 VALUES [('2020-02-27'), ('2020-03-02')),\n" +
                "PARTITION p_20200306 VALUES [('2020-03-02'), ('2020-03-07')))\n" +
                "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, `city`, `model`, `brand`, `hours`) BUCKETS 1\n"
                +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        createTable("CREATE TABLE test.`pushdown_test` (\n" +
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
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n" +
                "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n" +
                "PARTITION p3 VALUES [(\"0\"), (\"64\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        createTable("create table test.jointest\n" +
                "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
                "properties(\"replication_num\" = \"1\");");

        createTable("create external table test.mysql_table\n" +
                "(k1 int, k2 int)\n" +
                "ENGINE=MYSQL\n" +
                "PROPERTIES (\n" +
                "\"host\" = \"127.0.0.1\",\n" +
                "\"port\" = \"3306\",\n" +
                "\"user\" = \"root\",\n" +
                "\"password\" = \"123\",\n" +
                "\"database\" = \"db1\",\n" +
                "\"table\" = \"tbl1\"\n" +
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

    @Test
    public void testDateTypeEquality() throws Exception {
        // related to Github issue #3309
        String loadStr = "load label test.app_profile_20200306\n" +
                "(DATA INFILE('filexxx')INTO TABLE app_profile partition (p_20200306)\n" +
                "COLUMNS TERMINATED BY '\\t'\n" +
                "(app_name,package_name,age,gender,level,city,model,brand,hours,use_num,use_time,start_times)\n" +
                "SET\n" +
                "(event_date = default_value('2020-03-06'))) \n" +
                "PROPERTIES ( 'max_filter_ratio'='0.0001' );\n" +
                "";
        LoadStmt loadStmt = (LoadStmt) UtFrameUtils.parseAndAnalyzeStmt(loadStr, connectContext);
        Catalog.getCurrentCatalog().getLoadManager().createLoadJobV1FromStmt(loadStmt, EtlJobType.HADOOP,
                System.currentTimeMillis());
    }

    @Test
    public void testJoinPredicateTransitivity() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // test left join : left table where binary predicate
        String sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test left join: left table where in predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id in (2);";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` IN (2)"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` IN (2)"));

        // test left join: left table where between predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id BETWEEN 1 AND 2;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` >= 1, `join1`.`id` <= 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` >= 1, `join2`.`id` <= 2"));

        // test left join: left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("other join predicates: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test left join: right table where predicate.
        // If we eliminate outer join, we could push predicate down to join1 and join2.
        // Currently, we push predicate to join1 and keep join predicate for join2
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("other join predicates: `join2`.`id` > 1"));

        // test left join: right table join predicate, only push down right table
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test inner join: left table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test inner join: left table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test inner join: right table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test inner join: right table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and 1 < join2.id;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test anti join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test semi join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test anti join, left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("other join predicates: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test semi join, left table join predicate, only push to left table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test anti join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test semi join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join2`.`id` > 1"));
    }

    @Test
    public void testConvertCaseWhenToConstant() throws Exception {
        // basic test
        String caseWhenSql = "select "
                + "case when date_format(now(),'%H%i')  < 123 then 1 else 0 end as col "
                + "from test.test1 "
                + "where time = case when date_format(now(),'%H%i')  < 123 then date_format(date_sub(now(),2),'%Y%m%d') else date_format(date_sub(now(),1),'%Y%m%d') end";
        Assert.assertTrue(!StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + caseWhenSql), "CASE WHEN"));

        // test 1: case when then
        // 1.1 multi when in on `case when` and can be converted to constants
        String sql11 = "select case when false then 2 when true then 3 else 0 end as col11;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql11), "constant exprs: \n         3"));

        // 1.2 multi `when expr` in on `case when` ,`when expr` can not be converted to constants
        String sql121 = "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql121),
                "OUTPUT EXPRS:CASE WHEN substr(`k7`, 2, 1) THEN 3 ELSE 0 END"));

        // 1.2.2 when expr which can not be converted to constants in the first
        String sql122 = "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql122),
                "OUTPUT EXPRS:CASE WHEN substr(`k7`, 2, 1) THEN 2 WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.2.3 test return `then expr` in the middle
        String sql124 = "select case when false then 1 when true then 2 when false then 3 else 'other' end as col124";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql124), "constant exprs: \n         '2'"));

        // 1.3 test return null
        String sql3 = "select case when false then 2 end as col3";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql3), "constant exprs: \n         NULL"));

        // 1.3.1 test return else expr
        String sql131 = "select case when false then 2 when false then 3 else 4 end as col131";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql131), "constant exprs: \n         4"));

        // 1.4 nest `case when` and can be converted to constants
        String sql14 = "select case when (case when true then true else false end) then 2 when false then 3 else 0 end as col";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql14), "constant exprs: \n         2"));

        // 1.5 nest `case when` and can not be converted to constants
        String sql15 = "select case when case when substr(k7,2,1) then true else false end then 2 when false then 3 else 0 end as col from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql15),
                "OUTPUT EXPRS:CASE WHEN CASE WHEN substr(`k7`, 2, 1) THEN TRUE ELSE FALSE END THEN 2 WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.6 test when expr is null
        String sql16 = "select case when null then 1 else 2 end as col16;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql16), "constant exprs: \n         2"));

        // test 2: case xxx when then
        // 2.1 test equal
        String sql2 = "select case 1 when 1 then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql2), "constant exprs: \n         'a'"));

        // 2.1.2 test not equal
        String sql212 = "select case 'a' when 1 then 'a' when 'a' then 'b' else 'other' end as col212;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql212), "constant exprs: \n         'b'"));

        // 2.2 test return null
        String sql22 = "select case 'a' when 1 then 'a' when 'b' then 'b' end as col22;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql22), "constant exprs: \n         NULL"));

        // 2.2.2 test return else
        String sql222 = "select case 1 when 2 then 'a' when 3 then 'b' else 'other' end as col222;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql222), "constant exprs: \n         'other'"));

        // 2.3 test can not convert to constant,middle when expr is not constant
        String sql23 = "select case 'a' when 'b' then 'a' when substr(k7,2,1) then 2 when false then 3 else 0 end as col23 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql23),
                "OUTPUT EXPRS:CASE'a' WHEN substr(`k7`, 2, 1) THEN '2' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.3.1  first when expr is not constant
        String sql231 = "select case 'a' when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col231 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql231),
                "OUTPUT EXPRS:CASE'a' WHEN substr(`k7`, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.3.2 case expr is not constant
        String sql232 = "select case k1 when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col232 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql232),
                "OUTPUT EXPRS:CASE`k1` WHEN substr(`k7`, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 3.1 test float,float in case expr
        String sql31 = "select case cast(100 as float) when 1 then 'a' when 2 then 'b' else 'other' end as col31;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql31),
                "constant exprs: \n         CASE100.0 WHEN 1.0 THEN 'a' WHEN 2.0 THEN 'b' ELSE 'other' END"));

        // 4.1 test null in case expr return else
        String sql41 = "select case null when 1 then 'a' when 2 then 'b' else 'other' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql41), "constant exprs: \n         'other'"));

        // 4.1.2 test null in case expr return null
        String sql412 = "select case null when 1 then 'a' when 2 then 'b' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql412), "constant exprs: \n         NULL"));

        // 4.2.1 test null in when expr
        String sql421 = "select case 'a' when null then 'a' else 'other' end as col421";
        Assert.assertTrue(StringUtils.containsIgnoreCase(UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql421), "constant exprs: \n         'other'"));
    }

    @Test
    public void testJoinPredicateTransitivityWithSubqueryInWhereClause() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "SELECT *\n" +
                "FROM test.pushdown_test\n" +
                "WHERE 0 < (\n" +
                "    SELECT MAX(k9)\n" +
                "    FROM test.pushdown_test);";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT"));
        Assert.assertTrue(explainString.contains("CROSS JOIN"));
        Assert.assertTrue(!explainString.contains("PREDICATES"));
    }

    @Test
    public void testConstInParitionPrune() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryStr = "explain select * from (select 'aa' as kk1, sum(id) from test.join1 where dt = 9 group by kk1)tt where kk1 in ('aa');";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        FeConstants.runningUnitTest = false;
        Assert.assertTrue(explainString.contains("partitions=1/1"));
    }

    @Test
    public void testOrCompoundPredicateFold() throws Exception {
        String queryStr = "explain select * from baseall where (k1 > 1) or (k1 > 1 and k2 < 1)";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: (`k1` > 1)\n"));

        queryStr = "explain select * from  baseall where (k1 > 1 and k2 < 1) or  (k1 > 1)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: `k1` > 1\n"));

        queryStr = "explain select * from  baseall where (k1 > 1) or (k1 > 1)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: (`k1` > 1)\n"));
    }

    @Test
    public void testJoinWithMysqlTable() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // set data size and row count for the olap table
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersionAndVersionHash(2, 0);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 0, 200000, 10000);
                    }
                }
            }
        }

        String queryStr = "explain select * from mysql_table t2, jointest t1 where t1.k1 = t2.k1";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BROADCAST)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));

        queryStr = "explain select * from jointest t1, mysql_table t2 where t1.k1 = t2.k1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BROADCAST)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));
    }
}
