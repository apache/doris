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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.ExplainTest;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.rewrite.RewriteDateLiteralRuleTest;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class QueryPlanTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        // create database
        createDatabase("test");
        connectContext.getSessionVariable().setEnableNereidsPlanner(false);

        createTable("create table test.test1\n"
                + "(\n"
                + "    query_id varchar(48) comment \"Unique query id\",\n"
                + "    time_col datetime not null comment \"Query start time\",\n"
                + "    client_ip varchar(32) comment \"Client IP\",\n"
                + "    user varchar(64) comment \"User name\",\n"
                + "    db varchar(96) comment \"Database of this query\",\n"
                + "    state varchar(8) comment \"Query result state. EOF, ERR, OK\",\n"
                + "    query_time bigint comment \"Query execution time in millisecond\",\n"
                + "    scan_bytes bigint comment \"Total scan bytes of this query\",\n"
                + "    scan_rows bigint comment \"Total scan rows of this query\",\n"
                + "    return_rows bigint comment \"Returned rows of this query\",\n"
                + "    stmt_id int comment \"An incremental id of statement\",\n"
                + "    is_query tinyint comment \"Is this statemt a query. 1 or 0\",\n"
                + "    frontend_ip varchar(32) comment \"Frontend ip of executing this statement\",\n"
                + "    stmt varchar(2048) comment \"The original statement, trimed if longer than 2048 bytes\"\n"
                + ")\n"
                + "partition by range(time_col) ()\n"
                + "distributed by hash(query_id) buckets 1\n"
                + "properties(\n"
                + "    \"dynamic_partition.time_unit\" = \"DAY\",\n"
                + "    \"dynamic_partition.start\" = \"-30\",\n"
                + "    \"dynamic_partition.end\" = \"3\",\n"
                + "    \"dynamic_partition.prefix\" = \"p\",\n"
                + "    \"dynamic_partition.buckets\" = \"1\",\n"
                + "    \"dynamic_partition.enable\" = \"true\",\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.bitmap_table (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` bitmap bitmap_union NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.join1 (\n"
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

        createTable("CREATE TABLE test.join2 (\n"
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

        createTable("CREATE TABLE test.bitmap_table_2 (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` bitmap bitmap_union NULL,\n"
                + "  `id3` bitmap bitmap_union NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.hll_table (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` hll hll_union NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`bigtable` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `k4` bigint(20) NULL COMMENT \"\",\n"
                + "  `k5` decimal(9, 3) NULL COMMENT \"\",\n"
                + "  `k6` char(5) NULL COMMENT \"\",\n"
                + "  `k10` date NULL COMMENT \"\",\n"
                + "  `k11` datetime NULL COMMENT \"\",\n"
                + "  `k7` varchar(20) NULL COMMENT \"\",\n"
                + "  `k8` double MAX NULL COMMENT \"\",\n"
                + "  `k9` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`baseall` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `k4` bigint(20) NULL COMMENT \"\",\n"
                + "  `k5` decimal(9, 3) NULL COMMENT \"\",\n"
                + "  `k6` char(5) NULL COMMENT \"\",\n"
                + "  `k10` date NULL COMMENT \"\",\n"
                + "  `k11` datetime NULL COMMENT \"\",\n"
                + "  `k7` varchar(20) NULL COMMENT \"\",\n"
                + "  `k8` double MAX NULL COMMENT \"\",\n"
                + "  `k9` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`dynamic_partition` (\n"
                + "  `k1` date NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `k4` bigint(20) NULL COMMENT \"\",\n"
                + "  `k5` decimal(9, 3) NULL COMMENT \"\",\n"
                + "  `k6` char(5) NULL COMMENT \"\",\n"
                + "  `k10` date NULL COMMENT \"\",\n"
                + "  `k11` datetime NULL COMMENT \"\",\n"
                + "  `k7` varchar(20) NULL COMMENT \"\",\n"
                + "  `k8` double MAX NULL COMMENT \"\",\n"
                + "  `k9` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"dynamic_partition.enable\" = \"true\",\n"
                + "\"dynamic_partition.start\" = \"-3\",\n"
                + "\"dynamic_partition.end\" = \"3\",\n"
                + "\"dynamic_partition.time_unit\" = \"day\",\n"
                + "\"dynamic_partition.prefix\" = \"p\",\n"
                + "\"dynamic_partition.buckets\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`app_profile` (\n"
                + "  `event_date` date NOT NULL COMMENT \"\",\n"
                + "  `app_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `package_name` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `age` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `gender` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `level` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `city` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `model` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `brand` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `hours` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `use_num` int(11) SUM NOT NULL COMMENT \"\",\n"
                + "  `use_time` double SUM NOT NULL COMMENT \"\",\n"
                + "  `start_times` bigint(20) SUM NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, "
                + "`city`, `model`, `brand`, `hours`) COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`event_date`)\n"
                + "(PARTITION p_20200301 VALUES [('2020-02-27'), ('2020-03-02')),\n"
                + "PARTITION p_20200306 VALUES [('2020-03-02'), ('2020-03-07')))\n"
                + "DISTRIBUTED BY HASH(`event_date`, `app_name`, `package_name`, `age`, `gender`, `level`, "
                + "`city`, `model`, `brand`, `hours`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`pushdown_test` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `k4` bigint(20) NULL COMMENT \"\",\n"
                + "  `k5` decimal(9, 3) NULL COMMENT \"\",\n"
                + "  `k6` char(5) NULL COMMENT \"\",\n"
                + "  `k10` date NULL COMMENT \"\",\n"
                + "  `k11` datetime NULL COMMENT \"\",\n"
                + "  `k7` varchar(20) NULL COMMENT \"\",\n"
                + "  `k8` double MAX NULL COMMENT \"\",\n"
                + "  `k9` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n"
                + "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n"
                + "PARTITION p3 VALUES [(\"0\"), (\"64\")))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ");");

        createTable("create table test.jointest\n"
                + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        createTable("create table test.bucket_shuffle1\n"
                + "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 5\n"
                + "properties(\"replication_num\" = \"1\""
                + ");");

        createTable("CREATE TABLE test.`bucket_shuffle2` (\n"
                + "  `k1` int NULL COMMENT \"\",\n"
                + "  `k2` int(6) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`k1`)\n"
                + "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n"
                + "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n"
                + "PARTITION p3 VALUES [(\"0\"), (\"64\")))\n"
                + "DISTRIBUTED BY HASH(k1, k2) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ");");

        createTable("create table test.colocate1\n"
                + "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n"
                + "properties(\"replication_num\" = \"1\","
                + "\"colocate_with\" = \"group1\");");

        createTable("create table test.colocate2\n"
                + "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n"
                + "properties(\"replication_num\" = \"1\","
                + "\"colocate_with\" = \"group1\");");

        createTable("create external table test.mysql_table\n"
                + "(k1 int, k2 int)\n"
                + "ENGINE=MYSQL\n"
                + "PROPERTIES (\n"
                + "\"host\" = \"127.0.0.1\",\n"
                + "\"port\" = \"3306\",\n"
                + "\"user\" = \"root\",\n"
                + "\"password\" = \"123\",\n"
                + "\"database\" = \"db1\",\n"
                + "\"table\" = \"tbl1\"\n"
                + ");");

        createTable("CREATE TABLE test.`table_partitioned` (\n"
                + "  `dt` int(11) NOT NULL COMMENT \"\",\n"
                + "  `dis_key` varchar(20) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`dt`, `dis_key`)\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(PARTITION p20200101 VALUES [(\"-1\"), (\"20200101\")),\n"
                + "PARTITION p20200201 VALUES [(\"20200101\"), (\"20200201\")))\n"
                + "DISTRIBUTED BY HASH(`dt`, `dis_key`) BUCKETS 2\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        createTable("CREATE TABLE test.`table_unpartitioned` (\n"
                + "  `dt` int(11) NOT NULL COMMENT \"\",\n"
                + "  `dis_key` varchar(20) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`dt`, `dis_key`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`dt`, `dis_key`) BUCKETS 2\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
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

        createTable("create external table test.odbc_mysql\n"
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

        createTable("create table test.tbl_int_date ("
                + "`date` datetime NULL,"
                + "`day` date NULL,"
                + "`site_id` int(11) NULL )"
                + " ENGINE=OLAP "
                + "DUPLICATE KEY(`date`, `day`, `site_id`)"
                + "DISTRIBUTED BY HASH(`site_id`) BUCKETS 10 "
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");");

        createView("create view test.tbl_null_column_view AS SELECT *,NULL as add_column  FROM test.test1;");

        createView("create view test.function_view AS SELECT query_id, client_ip, concat(user, db) as"
                + " concat FROM test.test1;");

        createTable("create table test.tbl_using_a\n"
                + "(\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int sum\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 "
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\""
                + ");");

        createTable("create table test.tbl_using_b\n"
                + "(\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    k3 int \n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 "
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\""
                + ");");
    }

    @Test
    public void testFunctionViewGroupingSet() throws Exception {
        String queryStr = "select /*+ SET_VAR(enable_nereids_planner=false) */ query_id, client_ip, concat from test.function_view group by rollup("
                + "query_id, client_ip, concat);";
        assertSQLPlanOrErrorMsgContains(queryStr, "repeat: repeat 3 lines [[], [8], [8, 9], [8, 9, 10]]");
    }

    @Test
    public void testBitmapInsertInto() throws Exception {
        String sql = "INSERT INTO test.bitmap_table (id, id2) VALUES (1001, to_bitmap(1000)), (1001, to_bitmap(2000));";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));

        sql = "insert into test.bitmap_table select id, bitmap_union(id2) from test.bitmap_table_2 group by id;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("bitmap_union"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 1, "AGGREGATE"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 0, "OlapScanNode"));

        sql = "insert into test.bitmap_table select /*+ SET_VAR(enable_nereids_planner=false) */ id, id2 from test.bitmap_table_2;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("OLAP TABLE SINK"));
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:\n    `id`\n    `id2`"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 0, "OlapScanNode"));

        assertSQLPlanOrErrorMsgContains("insert into test.bitmap_table select id, id from test.bitmap_table_2;",
                "bitmap column require the function return type is BITMAP");
    }

    @Test
    public void testBitmapQuery() throws Exception {
        assertSQLPlanOrErrorMsgContains(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.bitmap_table;",
                "OUTPUT EXPRS:\n    `default_cluster:test`.`bitmap_table`.`id`\n    `default_cluster:test`.`bitmap_table`.`id2`"
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select group_concat(id2) from test.bitmap_table;",
                "group_concat requires first parameter to be of type STRING: group_concat(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select sum(id2) from test.bitmap_table;",
                "sum requires a numeric parameter: sum(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select avg(id2) from test.bitmap_table;",
                "avg requires a numeric parameter: avg(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select max(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select min(id2) from test.bitmap_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(*) from test.bitmap_table group by id2;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(*) from test.bitmap_table where id2 = 1;",
                "Unsupported bitmap type in expression: `id2` = 1"
        );

    }

    @Test
    public void testHLLTypeQuery() throws Exception {
        assertSQLPlanOrErrorMsgContains(
                "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.hll_table;",
                "OUTPUT EXPRS:\n    `default_cluster:test`.`hll_table`.`id`\n    `default_cluster:test`.`hll_table`.`id2`"
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select group_concat(id2) from test.hll_table;",
                "group_concat requires first parameter to be of type STRING: group_concat(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select sum(id2) from test.hll_table;",
                "sum requires a numeric parameter: sum(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select avg(id2) from test.hll_table;",
                "avg requires a numeric parameter: avg(`id2`)"
        );

        assertSQLPlanOrErrorMsgContains(
                "select max(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select min(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select min(id2) from test.hll_table;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(*) from test.hll_table group by id2;",
                Type.OnlyMetricTypeErrorMsg
        );

        assertSQLPlanOrErrorMsgContains(
                "select count(*) from test.hll_table where id2 = 1",
                "Hll type dose not support operand: `id2` = 1"
        );
    }

    @Test
    public void testTypeCast() throws Exception {
        // cmy: this test may sometimes failed in our daily test env, so I add a case here.
        String sql = "select * from test.baseall a where k1 in (select k1 from test.bigtable b where k2 > 0 and k1 = 1);";
        getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertEquals(MysqlStateType.EOF, connectContext.getState().getStateType());

        sql = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'";
        List<StatementBase> stmts = parseAndAnalyzeStmts(sql);
        Assert.assertEquals(2, stmts.size());

        // disable cast hll/bitmap to string
        assertSQLPlanOrErrorMsgContains(
                "select cast(id2 as varchar) from test.hll_table;",
                "Invalid type cast of `id2` from HLL to VARCHAR(*)"
        );
        assertSQLPlanOrErrorMsgContains(
                "select cast(id2 as varchar) from test.bitmap_table;",
                "Invalid type cast of `id2` from BITMAP to VARCHAR(*)"
        );
        // disable implicit cast hll/bitmap to string
        assertSQLPlanOrErrorMsgContains(
                "select length(id2) from test.hll_table;",
                "No matching function with signature: length(HLL)"
        );
        assertSQLPlanOrErrorMsgContains(
                "select length(id2) from test.bitmap_table;",
                "No matching function with signature: length(BITMAP)"
        );
    }

    @Test
    public void testMultiStmts() throws Exception {
        String sql = "SHOW VARIABLES LIKE 'lower_case_%'; SHOW VARIABLES LIKE 'sql_mode'";
        List<StatementBase> stmts = parseAndAnalyzeStmts(sql);
        Assert.assertEquals(2, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;";
        stmts = parseAndAnalyzeStmts(sql);
        Assert.assertEquals(1, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%';;;SHOW VARIABLES LIKE 'lower_case_%';";
        stmts = parseAndAnalyzeStmts(sql);
        Assert.assertEquals(4, stmts.size());

        sql = "SHOW VARIABLES LIKE 'lower_case_%'";
        stmts = parseAndAnalyzeStmts(sql);
        Assert.assertEquals(1, stmts.size());
    }

    @Test
    public void testCountDistinctRewrite() throws Exception {
        String sql = "select count(distinct id) from test.bitmap_table";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("output: count"));

        sql = "select count(distinct id2) from test.bitmap_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.hll_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("hll_union_agg"));

        sql = "select sum(id) / count(distinct id2) from test.hll_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("hll_union_agg"));

        sql = "select count(distinct id2) from test.bitmap_table group by id order by count(distinct id2)";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct id2) from test.bitmap_table order by count(distinct id2)";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct if(id = 1, id2, null)) from test.bitmap_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct ifnull(id2, id3)) from test.bitmap_table_2";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        sql = "select count(distinct coalesce(id2, id3)) from test.bitmap_table_2";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("bitmap_union_count"));

        ConnectContext.get().getSessionVariable().setRewriteCountDistinct(false);
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ count(distinct id2) from test.bitmap_table";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains(Type.OnlyMetricTypeErrorMsg));
    }

    @Test
    public void testCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String createSchemaSql = "create schema if not exists test";
        String createDbSql = "create database if not exists test";
        CreateDbStmt createSchemaStmt = (CreateDbStmt) parseAndAnalyzeStmt(createSchemaSql);
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbSql);
        Assert.assertEquals(createDbStmt.toSql(), createSchemaStmt.toSql());
    }

    @Test
    public void testDropDbQueryPlanWithSchemaSyntax() throws Exception {
        String dropSchemaSql = "drop schema if exists test";
        String dropDbSql = "drop database if exists test";
        DropDbStmt dropSchemaStmt = (DropDbStmt) parseAndAnalyzeStmt(dropSchemaSql);
        DropDbStmt dropDbStmt = (DropDbStmt) parseAndAnalyzeStmt(dropDbSql);
        Assert.assertEquals(dropDbStmt.toSql(), dropSchemaStmt.toSql());
    }

    @Test
    public void testShowCreateDbQueryPlanWithSchemaSyntax() throws Exception {
        String showCreateSchemaSql = "show create schema test";
        String showCreateDbSql = "show create database test";
        ShowCreateDbStmt showCreateSchemaStmt = (ShowCreateDbStmt) parseAndAnalyzeStmt(showCreateSchemaSql);
        ShowCreateDbStmt showCreateDbStmt = (ShowCreateDbStmt) parseAndAnalyzeStmt(showCreateDbSql);
        Assert.assertEquals(showCreateDbStmt.toSql(), showCreateSchemaStmt.toSql());
    }

    @Test
    public void testDateTypeCastSyntax() throws Exception {
        String castSql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        SelectStmt selectStmt = (SelectStmt) parseAndAnalyzeStmt(castSql);
        Expr rightExpr = selectStmt.getWhereClause().getChildren().get(1);
        Assert.assertEquals(rightExpr.getType(), ScalarType.getDefaultDateType(Type.DATETIME));

        String castSql2 = "select /*+ SET_VAR(enable_nereids_planner=false) */ str_to_date('11/09/2011', '%m/%d/%Y');";
        String explainString = getSQLPlanOrErrorMsg("explain " + castSql2);
        Assert.assertTrue(explainString.contains("2011-11-09"));
        Assert.assertFalse(explainString.contains("2011-11-09 00:00:00"));
    }

    @Test
    public void testJoinPredicateTransitivity() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        ConnectContext.get().getSessionVariable().setEnableInferPredicate(true);
        /*  TODO: commit on_clause and where_clause Cross-identification
        // test left join : left table where binary predicate
        String sql = "select join1.id\n"
                + "from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "where join1.id > 1;";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test left join: left table where in predicate
        sql = "select join1.id\n"
                + "from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "where join1.id in (2);";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` IN (2)"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` IN (2)"));

        // test left join: left table where between predicate
        sql = "select join1.id\n"
                + "from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "where join1.id BETWEEN 1 AND 2;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` >= 1, `join1`.`id` <= 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` >= 1, `join2`.`id` <= 2"));

        */
        // test left join: left table join predicate, left table couldn't push down
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "and join1.id > 1;";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("other join predicates: <slot 12> <slot 0> > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        /*
        // test left join: right table where predicate.
        // If we eliminate outer join, we could push predicate down to join1 and join2.
        // Currently, we push predicate to join1 and keep join predicate for join2
        sql = "select *\n from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "where join2.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("other join predicates: `join2`.`id` > 1"));
        */

        // test left join: right table join predicate, only push down right table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left join join2 on join1.id = join2.id\n"
                + "and join2.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        /*
        // test inner join: left table where predicate, both push down left table and right table
        sql = "select *\n from join1\n"
                + "join join2 on join1.id = join2.id\n"
                + "where join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        */

        // test inner join: left table join predicate, both push down left table and right table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "join join2 on join1.id = join2.id\n"
                + "and join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        /*
        // test inner join: right table where predicate, both push down left table and right table
        sql = "select *\n from join1\n"
                + "join join2 on join1.id = join2.id\n"
                + "where join2.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        */

        // test inner join: right table join predicate, both push down left table and right table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"

                + "join join2 on join1.id = join2.id\n" + "and 1 < join2.id;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "join join2 on join1.id = join2.value\n"
                + "and join2.value in ('abc');";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertFalse(explainString.contains("'abc' is not a number"));
        Assert.assertFalse(explainString.contains("`join1`.`value` IN ('abc')"));

        // test anti join, right table join predicate, only push to right table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left anti join join2 on join1.id = join2.id\n"
                + "and join2.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test semi join, right table join predicate, only push to right table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left semi join join2 on join1.id = join2.id\n"
                + "and join2.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test anti join, left table join predicate, left table couldn't push down
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left anti join join2 on join1.id = join2.id\n"
                + "and join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("other join predicates: <slot 7> <slot 0> > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        // test semi join, left table join predicate, only push to left table
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ *\n from join1\n"
                + "left semi join join2 on join1.id = join2.id\n"
                + "and join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));

        /*
        // test anti join, left table where predicate, only push to left table
        sql = "select join1.id\n"
                + "from join1\n"
                + "left anti join join2 on join1.id = join2.id\n"
                + "where join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join2`.`id` > 1"));

        // test semi join, left table where predicate, only push to left table
        sql = "select join1.id\n"
                + "from join1\n"
                + "left semi join join2 on join1.id = join2.id\n"
                + "where join1.id > 1;";
        explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `join1`.`id` > 1"));
        Assert.assertFalse(explainString.contains("PREDICATES: `join2`.`id` > 1"));
        */
    }

    @Disabled
    public void testConvertCaseWhenToConstant() throws Exception {
        // basic test
        String caseWhenSql = "select "
                + "case when date_format(now(),'%H%i')  < 123 then 1 else 0 end as col "
                + "from test.test1 "
                + "where time_col = case when date_format(now(),'%H%i')  < 123 then date_format(date_sub("
                + "now(),2),'%Y%m%d') else date_format(date_sub(now(),1),'%Y%m%d') end";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + caseWhenSql),
                "CASE WHEN"));

        // test 1: case when then
        // 1.1 multi when in on `case when` and can be converted to constants
        String sql11 = "select case when false then 2 when true then 3 else 0 end as col11;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql11),
                "constant exprs: \n         3"));

        // 1.2 multi `when expr` in on `case when` ,`when expr` can not be converted to constants
        String sql121 = "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from"
                + " test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql121),
                "OUTPUT EXPRS:\n    CASE WHEN substr(`k7`, 2, 1) THEN 3 ELSE 0 END"));

        // 1.2.2 when expr which can not be converted to constants in the first
        String sql122 = "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122"
                + " from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql122),
                "OUTPUT EXPRS:\n    CASE WHEN substr(`k7`, 2, 1) THEN 2 WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.2.3 test return `then expr` in the middle
        String sql124 = "select case when false then 1 when true then 2 when false then 3 else 'other' end as col124";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql124),
                "constant exprs: \n         '2'"));

        // 1.3 test return null
        String sql3 = "select case when false then 2 end as col3";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql3),
                "constant exprs: \n         NULL"));

        // 1.3.1 test return else expr
        String sql131 = "select case when false then 2 when false then 3 else 4 end as col131";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql131),
                "constant exprs: \n         4"));

        // 1.4 nest `case when` and can be converted to constants
        String sql14 = "select case when (case when true then true else false end) then 2 when false then 3 else 0 end as col";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql14),
                "constant exprs: \n         2"));

        // 1.5 nest `case when` and can not be converted to constants
        String sql15 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case when case when substr(k7,2,1) then true else false end then 2 when false then 3"
                + " else 0 end as col from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql15),
                "OUTPUT EXPRS:\n    CASE WHEN CASE WHEN substr(`k7`, 2, 1) THEN TRUE ELSE FALSE END THEN 2"
                        + " WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.6 test when expr is null
        String sql16 = "select case when null then 1 else 2 end as col16;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql16),
                "constant exprs: \n         2"));

        // test 2: case xxx when then
        // 2.1 test equal
        String sql2 = "select case 1 when 1 then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql2),
                "constant exprs: \n         'a'"));

        // 2.1.2 test not equal
        String sql212 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 'a' when 1 then 'a' when 'a' then 'b' else 'other' end as col212;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql212),
                "constant exprs: \n         'b'"));

        // 2.2 test return null
        String sql22 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 'a' when 1 then 'a' when 'b' then 'b' end as col22;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql22),
                "constant exprs: \n         NULL"));

        // 2.2.2 test return else
        String sql222 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 1 when 2 then 'a' when 3 then 'b' else 'other' end as col222;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql222),
                "constant exprs: \n         'other'"));

        // 2.3 test can not convert to constant,middle when expr is not constant
        String sql23 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 'a' when 'b' then 'a' when substr(k7,2,1) then 2 when false then 3"
                + " else 0 end as col23 from test.baseall";
        String a = getSQLPlanOrErrorMsg("explain " + sql23);
        Assert.assertTrue(StringUtils.containsIgnoreCase(a,
                "OUTPUT EXPRS:\n    CASE 'a' WHEN substr(`k7`, 2, 1) THEN '2' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.3.1  first when expr is not constant
        String sql231 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 'a' when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end"
                + " as col231 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql231),
                "OUTPUT EXPRS:\n    CASE 'a' WHEN substr(`k7`, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0'"
                        + " THEN '3' ELSE '0' END"));

        // 2.3.2 case expr is not constant
        String sql232 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case k1 when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end"
                + " as col232 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql232),
                "OUTPUT EXPRS:\n    CASE `k1` WHEN substr(`k7`, 2, 1) THEN '2' WHEN '1' THEN 'a' "
                        + "WHEN '0' THEN '3' ELSE '0' END"));

        // 3.1 test float,float in case expr
        String sql31 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case cast(100 as float) when 1 then 'a' when 2 then 'b' else 'other' end as col31;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql31),
                "constant exprs: \n         CASE 100 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'other' END"));

        // 4.1 test null in case expr return else
        String sql41 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case null when 1 then 'a' when 2 then 'b' else 'other' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql41),
                "constant exprs: \n         'other'"));

        // 4.1.2 test null in case expr return null
        String sql412 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case null when 1 then 'a' when 2 then 'b' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql412),
                "constant exprs: \n         NULL"));

        // 4.2.1 test null in when expr
        String sql421 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case 'a' when null then 'a' else 'other' end as col421";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql421),
                "constant exprs: \n         'other'"));

        // 5.1 test same type in then expr and else expr
        String sql51 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case when 132 then k7 else 'all' end as col51 from test.baseall group by col51";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql51),
                "CASE WHEN 132 THEN `k7` ELSE 'all' END"));

        // 5.2 test same type in then expr and else expr
        String sql52 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case when 2 < 1 then 'all' else k7 end as col52 from test.baseall group by col52";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql52),
                "`k7`"));

        // 5.3 test different type in then expr and else expr, and return CastExpr<SlotRef>
        String sql53 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case when 2 < 1 then 'all' else k1 end as col53 from test.baseall group by col53";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql53),
                "`k1`"));

        // 5.4 test return CastExpr<SlotRef> with other SlotRef in selectListItem
        String sql54 = "select /*+ SET_VAR(enable_nereids_planner=false) */ k2, case when 2 < 1 then 'all' else k1 end as col54, k7 from test.baseall"
                + " group by k2, col54, k7";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql54),
                "OUTPUT EXPRS:\n    <slot 3> `k2`\n    <slot 4> `k1`\n    <slot 5> `k7`"));

        // 5.5 test return CastExpr<CastExpr<SlotRef>> with other SlotRef in selectListItem
        String sql55 = "select /*+ SET_VAR(enable_nereids_planner=false) */ case when 2 < 1 then 'all' else cast(k1 as int) end as col55, k7 from"
                + " test.baseall group by col55, k7";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getSQLPlanOrErrorMsg("explain " + sql55),
                "OUTPUT EXPRS:\n    <slot 2> CAST(`k1` AS INT)\n    <slot 3> `k7`"));
    }

    @Test
    public void testJoinPredicateTransitivityWithSubqueryInWhereClause() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "SELECT *\n"
                + "FROM test.pushdown_test\n"
                + "WHERE 0 < (\n"
                + "    SELECT MAX(k9)\n" + "    FROM test.pushdown_test);";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT"));
        Assert.assertTrue(explainString.contains("NESTED LOOP JOIN"));
        Assert.assertTrue(!explainString.contains("PREDICATES") || explainString.contains("PREDICATES: TRUE"));
    }

    @Test
    public void testDistinctPushDown() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "select distinct k1 from (select distinct k1 from test.pushdown_test) t where k1 > 1";
        String explainString = getSQLPlanOrErrorMsg("explain " + sql);
        Assert.assertTrue(explainString.contains("PLAN FRAGMENT"));
    }

    @Test
    public void testConstInPartitionPrune() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryStr = "explain select * from (select 'aa' as kk1, sum(id) from test.join1 where dt = 9"
                + " group by kk1)tt where kk1 in ('aa');";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        FeConstants.runningUnitTest = false;
        Assert.assertTrue(explainString.contains("partitions=1/1"));
    }

    @Test
    public void testOrCompoundPredicateFold() throws Exception {
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from baseall where (k1 > 1) or (k1 > 1 and k2 < 1)";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: (`k1` > 1)\n"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from  baseall where (k1 > 1 and k2 < 1) or  (k1 > 1)";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: `k1` > 1\n"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from  baseall where (k1 > 1) or (k1 > 1)";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: (`k1` > 1)\n"));
    }

    @Test
    public void testColocateJoin() throws Exception {
        FeConstants.runningUnitTest = true;

        String queryStr = "explain select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k1 and"
                + " t1.k2 = t2.k2 and t1.k3 = t2.k3";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.colocate1 t1 join [shuffle] test.colocate2 t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));

        // t1.k1 = t2.k2 not same order with distribute column
        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k2 and t1.k2 = t2.k1 and t1.k3 = t2.k3";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.colocate1 t1, test.colocate2 t2 where t1.k2 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));
    }

    @Test
    public void testSelfColocateJoin() throws Exception {
        FeConstants.runningUnitTest = true;

        // single partition
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.jointest t1, test.jointest t2 where t1.k1 = t2.k1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));

        // multi partition, should not be colocate
        queryStr = "explain select * from test.dynamic_partition t1, test.dynamic_partition t2 where t1.k1 = t2.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains(ColocatePlanTest.COLOCATE_ENABLE));
    }

    @Test
    public void testBucketShuffleJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        // enable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", true);

        // set data size and row count for the olap table
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("bucket_shuffle1");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 200000, 0, 10000);
                    }
                }
            }
        }

        db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        tbl = (OlapTable) db.getTableOrMetaException("bucket_shuffle2");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 200000, 0, 10000);
                    }
                }
            }
        }

        // single partition
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.jointest t1, test.bucket_shuffle1 t2 where t1.k1 = t2.k1"
                + " and t1.k1 = t2.k2";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("BUCKET_SHFFULE"));
        Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t1`.`k1`, `t1`.`k1`"));

        // not bucket shuffle join do not support different type
        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.jointest t1, test.bucket_shuffle1 t2 where cast (t1.k1 as tinyint)"
                + " = t2.k1 and t1.k1 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(!explainString.contains("BUCKET_SHFFULE"));

        // left table distribution column not match
        queryStr = "explain select * from test.jointest t1, test.bucket_shuffle1 t2 where t1.k1 = t2.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(!explainString.contains("BUCKET_SHFFULE"));

        // multi partition, should not be bucket shuffle join
        queryStr = "explain select * from test.jointest t1, test.bucket_shuffle2 t2 where t1.k1 = t2.k1"
                + " and t1.k1 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(!explainString.contains("BUCKET_SHFFULE"));

        // left table is colocate table, should be bucket shuffle
        queryStr = "explain select * from test.colocate1 t1, test.bucket_shuffle2 t2 where t1.k1 = t2.k1"
                + " and t1.k1 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(!explainString.contains("BUCKET_SHFFULE"));

        // support recurse of bucket shuffle join
        // TODO: support the UT in the future
        queryStr = "explain select * from test.jointest t1 join test.bucket_shuffle1 t2"
                + " on t1.k1 = t2.k1 and t1.k1 = t2.k2 join test.colocate1 t3"
                + " on t2.k1 = t3.k1 and t2.k2 = t3.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        // Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t1`.`k1`, `t1`.`k1`"));
        //  Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t3`.`k1`, `t3`.`k2`"));

        // support recurse of bucket shuffle because t4 join t2 and join column name is same as t2 distribute column name
        queryStr = "explain select * from test.jointest t1 join test.bucket_shuffle1 t2 on t1.k1 = t2.k1 and"
                + " t1.k1 = t2.k2 join test.colocate1 t3 on t2.k1 = t3.k1 join test.jointest t4 on t4.k1 = t2.k1 and"
                + " t4.k1 = t2.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        //Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t1`.`k1`, `t1`.`k1`"));
        //Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t4`.`k1`, `t4`.`k1`"));

        // some column name in join expr t3 join t4 and t1 distribute column name, so should not be bucket shuffle join
        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.jointest t1 join test.bucket_shuffle1 t2 on t1.k1 = t2.k1 and t1.k1 ="
                + " t2.k2 join test.colocate1 t3 on t2.k1 = t3.k1 join test.jointest t4 on t4.k1 = t3.k1 and"
                + " t4.k2 = t3.k2";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t1`.`k1`, `t1`.`k1`"));
        Assert.assertTrue(!explainString.contains("BUCKET_SHFFULE_HASH_PARTITIONED: `t4`.`k1`, `t4`.`k1`"));

        // here only a bucket shuffle + broadcast jost join
        queryStr = "explain SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * FROM test.bucket_shuffle1 T LEFT JOIN test.bucket_shuffle1 T1 ON T1.k2 = T.k1 and T.k2 = T1.k3 LEFT JOIN"
                + " test.bucket_shuffle2 T2 ON T2.k2 = T1.k1 and T2.k1 = T1.k2;";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("BUCKET_SHFFULE"));
        Assert.assertTrue(explainString.contains("BROADCAST"));
        // disable bucket shuffle join again
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);
    }

    @Test
    public void testJoinWithMysqlTable() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // set data size and row count for the olap table
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 200000, 0, 10000);
                    }
                }
            }
        }

        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);

        String queryStr = "explain select * from mysql_table t2, jointest t1 where t1.k1 = t2.k1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 1, "SCAN MYSQL"));

        queryStr = "explain select * from jointest t1, mysql_table t2 where t1.k1 = t2.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 1, "SCAN MYSQL"));

        queryStr = "explain select * from jointest t1, mysql_table t2, mysql_table t3 where t1.k1 = t3.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("INNER JOIN(PARTITIONED)"));

        // should clear the jointest table to make sure do not affect other test
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(0);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 0, 0, 0);
                    }
                }
            }
        }
    }

    @Test
    public void testJoinWithOdbcTable() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // set data size and row count for the olap table
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 200000, 0, 10000);
                    }
                }
            }
        }

        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);
        String queryStr = "explain select * from odbc_mysql t2, jointest t1 where t1.k1 = t2.k1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 1, "SCAN ODBC"));

        queryStr = "explain select * from jointest t1, odbc_mysql t2 where t1.k1 = t2.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainString, 1, "SCAN ODBC"));

        queryStr = "explain select * from jointest t1, odbc_mysql t2, odbc_mysql t3 where t1.k1 = t3.k1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("INNER JOIN(PARTITIONED)"));

        // should clear the jointest table to make sure do not affect other test
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                mIndex.setRowCount(0);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 0, 0, 0);
                    }
                }
            }
        }
    }

    @Disabled
    public void testPushDownOfOdbcTable() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // MySQL ODBC table can push down all filter
        String queryStr = "explain select * from odbc_mysql where k1 > 10 and abs(k1) > 10";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("`k1` > 10"));
        Assert.assertTrue(explainString.contains("abs(`k1`) > 10"));

        // now we do not support odbc scan node push down function call, except MySQL ODBC table
        // this table is Oracle ODBC table, so abs(k1) should not be pushed down
        queryStr = "explain select * from odbc_oracle where k1 > 10 and abs(k1) > 10";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("\"K1\" > 10"));
        Assert.assertTrue(!explainString.contains("abs(k1) > 10"));
    }

    @Test
    public void testLimitOfExternalTable() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // ODBC table (MySQL)
        String queryStr = "explain select * from odbc_mysql where k1 > 10 and abs(k1) > 10 limit 10";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("LIMIT 10"));

        // ODBC table (Oracle) not push down limit
        queryStr = "explain select * from odbc_oracle where k1 > 10 and abs(k1) > 10 limit 10";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        // abs is function, so Doris do not push down function except MySQL Database
        // so should not push down limit operation
        Assert.assertTrue(!explainString.contains("ROWNUM <= 10"));

        // ODBC table (Oracle) push down limit
        queryStr = "explain select * from odbc_oracle where k1 > 10 limit 10";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("ROWNUM <= 10"));

        // MySQL table
        queryStr = "explain select * from mysql_table where k1 > 10 and abs(k1) > 10 limit 10";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("LIMIT 10"));
    }

    @Test
    public void testOdbcSink() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // insert into odbc_oracle table
        String queryStr = "explain insert into odbc_oracle select * from odbc_mysql";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("TABLENAME IN DORIS: odbc_oracle"));
        Assert.assertTrue(explainString.contains("TABLE TYPE: ORACLE"));
        Assert.assertTrue(explainString.contains("TABLENAME OF EXTERNAL TABLE: \"TBL1\""));

        // enable transaction of ODBC Sink
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableOdbcTransaction", true);
        queryStr = "explain insert into odbc_oracle select * from odbc_mysql";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("EnableTransaction: true"));
    }

    @Test
    public void testPreferBroadcastJoin() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from (select k2 from jointest)t2, jointest t1 where t1.k1 = t2.k2";
        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);

        // default set PreferBroadcastJoin true
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));

        connectContext.getSessionVariable().setPreferJoinMethod("shuffle");
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(PARTITIONED)"));

        connectContext.getSessionVariable().setPreferJoinMethod("broadcast");
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN(BROADCAST)"));
    }

    @Test
    public void testRuntimeFilterMode() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from jointest t2, jointest t1 where t1.k1 = t2.k1";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "LOCAL");
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("runtime filter"));
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "REMOTE");
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("runtime filter"));
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "OFF");
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("runtime filter"));
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "GLOBAL");
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("runtime filter"));

        queryStr = "explain select * from jointest t2, jointest t1 where t1.k1 <=> t2.k1";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "LOCAL");
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 15);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("runtime filter"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from jointest as a where k1 = (select count(1) from jointest as b"
                + " where a.k1 = b.k1);";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "GLOBAL");
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 15);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("runtime filter"));
    }

    @Test
    public void testRuntimeFilterType() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from jointest t2, jointest t1 where t1.k1 = t2.k1";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "GLOBAL");

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 0);
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("runtime filter"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 1);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 2);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 3);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] <- `t1`.`k1`"));

        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 4);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[min_max] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 5);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 6);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 7);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[min_max] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 8);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 9);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 10);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 11);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] -> `t2`.`k1`"));


        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 12);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[min_max] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 13);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 14);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[min_max] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[in_or_bloom] -> `t2`.`k1`"));

        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", 15);
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[min_max] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF003[in_or_bloom] <- `t1`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF001[bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF002[min_max] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF003[in_or_bloom] -> `t2`.`k1`"));

        // support merge in filter, and forbidden implicit conversion to bloom filter
        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from jointest t2 join [shuffle] jointest t1 where t1.k1 = t2.k1";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "GLOBAL");
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", TRuntimeFilterType.IN.getValue());
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in] <- `t1`.`k1`"));
        Assert.assertFalse(explainString.contains("RF000[bloom] -> `t2`.`k1`"));
        Assert.assertFalse(explainString.contains("RF000[bloom] <- `t1`.`k1`"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from jointest t2 join [shuffle] jointest t1 where t1.k1 = t2.k1";
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterMode", "GLOBAL");
        Deencapsulation.setField(connectContext.getSessionVariable(), "runtimeFilterType", TRuntimeFilterType.IN_OR_BLOOM.getValue());
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("RF000[in_or_bloom] -> `t2`.`k1`"));
        Assert.assertTrue(explainString.contains("RF000[in_or_bloom] <- `t1`.`k1`"));
    }

    @Test
    public void testEmptyNode() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String emptyNode = "EMPTYSET";

        List<String> sqls = Lists.newArrayList();
        sqls.add("explain select * from baseall limit 0");
        sqls.add("explain select count(*) from baseall limit 0;");
        sqls.add("explain select k3, dense_rank() OVER () AS rank FROM baseall limit 0;");
        sqls.add("explain select rank from (select k3, dense_rank() OVER () AS rank FROM baseall) a limit 0;");
        sqls.add("explain select * from baseall join bigtable as b limit 0");

        sqls.add("explain select * from baseall where 1 = 2");
        sqls.add("explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from baseall where null = null");
        sqls.add("explain select count(*) from baseall where 1 = 2;");
        sqls.add("explain select /*+ SET_VAR(enable_nereids_planner=false) */ k3, dense_rank() OVER () AS rank FROM baseall where 1 =2;");
        sqls.add("explain select rank from (select k3, dense_rank() OVER () AS rank FROM baseall) a where 1 =2;");
        sqls.add("explain select * from baseall join bigtable as b where 1 = 2");

        for (String sql : sqls) {
            String explainString = getSQLPlanOrErrorMsg(sql);
            Assert.assertTrue(explainString.contains(emptyNode));
        }
    }

    @Test
    public void testInformationFunctions() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        Analyzer analyzer = new Analyzer(connectContext.getEnv(), connectContext);
        InformationFunction infoFunc = new InformationFunction("database");
        infoFunc.analyze(analyzer);
        Assert.assertEquals("test", infoFunc.getStrValue());

        infoFunc = new InformationFunction("user");
        infoFunc.analyze(analyzer);
        Assert.assertEquals("'root'@'127.0.0.1'", infoFunc.getStrValue());

        infoFunc = new InformationFunction("current_user");
        infoFunc.analyze(analyzer);
        Assert.assertEquals("'root'@'%'", infoFunc.getStrValue());
    }

    @Test
    public void testAggregateSatisfyOlapTableDistribution() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("default_cluster:test");
        String sql = "SELECT dt, dis_key, COUNT(1) FROM table_unpartitioned  group by dt, dis_key";
        String explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("AGGREGATE (update finalize)"));
    }


    @Test
    public void testLeadAndLagFunction() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ time_col, lead(query_time, 1, NULL) over () as time2 from test.test1";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("lead(`query_time`, 1, NULL)"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ time_col, lead(query_time, 1, 2) over () as time2 from test.test1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("lead(`query_time`, 1, 2)"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ time_col, lead(time_col, 1, '2020-01-01 00:00:00') over () as time2 from test.test1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("lead(`time_col`, 1, '2020-01-01 00:00:00')"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ time_col, lag(query_time, 1, 2) over () as time2 from test.test1";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("lag(`query_time`, 1, 2)"));
    }

    @Disabled
    public void testIntDateTime() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        //valid date
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day in ('2020-10-30')";
        String explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion ? "PREDICATES: `day` IN ('2020-10-30')"
                : "PREDICATES: `day` IN ('2020-10-30 00:00:00')"));
        //valid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day in ('2020-10-30','2020-10-29')";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion
                ? "PREDICATES: `day` IN ('2020-10-30', '2020-10-29')"
                : "PREDICATES: `day` IN ('2020-10-30 00:00:00', '2020-10-29 00:00:00')"));

        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date in ('2020-10-30 12:12:30')";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` IN ('2020-10-30 12:12:30')"));
        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date in ('2020-10-30')";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` IN ('2020-10-30 00:00:00')"));

        //int date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day in (20201030)";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `day` IN ('2020-10-30 00:00:00')"));
        //int datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date in (20201030)";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` IN ('2020-10-30 00:00:00')"));
    }

    @Test
    public void testOutJoinSmapReplace() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        //valid date
        String sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.aid, b.bid FROM (SELECT 3 AS aid) a right outer JOIN (SELECT 4 AS bid) b ON (a.aid=b.bid)";
        assertSQLPlanOrErrorMsgContains(sql, "OUTPUT EXPRS:\n" + "    <slot 2> <slot 0> 3\n" + "    <slot 3>  4");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.aid, b.bid FROM (SELECT 3 AS aid) a left outer JOIN (SELECT 4 AS bid) b ON (a.aid=b.bid)";
        assertSQLPlanOrErrorMsgContains(sql, "OUTPUT EXPRS:\n" + "    <slot 2> <slot 0> 3\n" + "    <slot 3>  4");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.aid, b.bid FROM (SELECT 3 AS aid) a full outer JOIN (SELECT 4 AS bid) b ON (a.aid=b.bid)";
        assertSQLPlanOrErrorMsgContains(sql, "OUTPUT EXPRS:\n" + "    <slot 2> <slot 0> 3\n" + "    <slot 3>  4");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.aid, b.bid FROM (SELECT 3 AS aid) a JOIN (SELECT 4 AS bid) b ON (a.aid=b.bid)";
        assertSQLPlanOrErrorMsgContains(sql, "OUTPUT EXPRS:\n" + "    <slot 2> <slot 0> 3\n" + "    <slot 3>  4");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.k1, b.k2 FROM (SELECT k1 from baseall) a LEFT OUTER JOIN (select k1, 999 as k2 from baseall) b ON (a.k1=b.k1)";
        assertSQLPlanOrErrorMsgContains(sql, "<slot 7>  `k1`\n" + "    <slot 9> <slot 4> 999");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.k1, b.k2 FROM (SELECT 1 as k1 from baseall) a RIGHT OUTER JOIN (select k1, 999 as k2 from baseall) b ON (a.k1=b.k1)";
        assertSQLPlanOrErrorMsgContains(sql, "<slot 8> <slot 0> 1\n" + "    <slot 10> <slot 3> 999");

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.k1, b.k2 FROM (SELECT 1 as k1 from baseall) a FULL JOIN (select k1, 999 as k2 from baseall) b ON (a.k1=b.k1)";
        assertSQLPlanOrErrorMsgContains(sql, "<slot 8> <slot 0> 1\n" + "    <slot 10> <slot 3> 999");
    }

    @Test
    public void testFromUnixTimeRewrite() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        //default format
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where from_unixtime(query_time) > '2021-03-02 10:01:28'";
        String explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` <= 253402271999 AND `query_time` > 1614650488"));
    }

    @Disabled
    public void testCheckInvalidDate() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("default_cluster:test");
        //valid date
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = '2020-10-30'";
        String explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion ? "PREDICATES: `day` = '2020-10-30'"
                : "PREDICATES: `day` = '2020-10-30 00:00:00'"));
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = from_unixtime(1196440219)";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `day` = '2007-12-01 00:30:19'"));
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `day` = '2014-12-21 12:34:56'"));
        //valid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = 20201030";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion ? "PREDICATES: `day` = '2020-10-30'"
                : "PREDICATES: `day` = '2020-10-30 00:00:00'"));
        //valid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = '20201030'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion ? "PREDICATES: `day` = '2020-10-30'"
                : "PREDICATES: `day` = '2020-10-30 00:00:00'"));
        //valid date contains micro second
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = '2020-10-30 10:00:01.111111'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion
                ? "VEMPTYSET"
                : "PREDICATES: `day` = '2020-10-30 10:00:01'"));
        //invalid date

        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = '2020-10-32'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: '2020-10-32' in expression: `day` = '2020-10-32'"));

        //invalid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = '20201032'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: '20201032' in expression: `day` = '20201032'"));
        //invalid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = 20201032";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: 20201032 in expression: `day` = 20201032"));
        //invalid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = 'hello'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: 'hello' in expression: `day` = 'hello'"));
        //invalid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = 2020-10-30";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: 1980 in expression: `day` = 1980"));
        //invalid date
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where day = 10-30";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: -20 in expression: `day` = -20"));
        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-30 12:12:30'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 12:12:30'"));
        //valid datetime, support parsing to minute
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-30 12:12'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 12:12:00'"));
        //valid datetime, support parsing to hour
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-30 12'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 12:00:00'"));
        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = 20201030";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 00:00:00'"));
        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '20201030'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 00:00:00'"));
        //valid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-30'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2020-10-30 00:00:00'"));
        //valid datetime contains micro second
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-30 10:00:01.111111'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(Config.enable_date_conversion
                ? "VEMPTYSET" : "PREDICATES: `date` = '2020-10-30 10:00:01'"));
        //invalid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-32'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: '2020-10-32' in expression: `date` = '2020-10-32'"));
        //invalid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = 'hello'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: 'hello' in expression: `date` = 'hello'"));
        //invalid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = 2020-10-30";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: 1980 in expression: `date` = 1980"));
        //invalid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = 10-30";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: -20 in expression: `date` = -20"));
        //invalid datetime
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '2020-10-12 12:23:76'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("Incorrect datetime value: '2020-10-12 12:23:76' in expression: `date` = '2020-10-12 12:23:76'"));

        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '1604031150'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2016-04-03 11:50:00'"));

        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ day from tbl_int_date where date = '1604031150000'";
        explainString = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `date` = '2016-04-03 11:50:00'"));

        String queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ count(*) from test.baseall where k11 > to_date(now())";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: `k11` > to_date"));

        queryStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ count(*) from test.baseall where k11 > '2021-6-1'";
        explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: `k11` > '2021-06-01 00:00:00'"));
    }

    @Test
    public void testCompoundPredicateWriteRule() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        // false or e ==> e
        String sql1 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where 2=-2 OR query_time=0;";
        String explainString1 = getSQLPlanOrErrorMsg("EXPLAIN " + sql1);
        Assert.assertTrue(explainString1.contains("PREDICATES: `query_time` = 0"));

        //true or e ==> true
        String sql2 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where -5=-5 OR query_time=0;";
        String explainString2 = getSQLPlanOrErrorMsg("EXPLAIN " + sql2);
        Assert.assertTrue(!explainString2.contains("OR"));

        //e or true ==> true
        String sql3 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where query_time=0 OR -5=-5;";
        String explainString3 = getSQLPlanOrErrorMsg("EXPLAIN " + sql3);
        Assert.assertTrue(!explainString3.contains("OR"));

        //e or false ==> e
        String sql4 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where -5!=-5 OR query_time=0;";
        String explainString4 = getSQLPlanOrErrorMsg("EXPLAIN " + sql4);
        Assert.assertTrue(explainString4.contains("PREDICATES: `query_time` = 0"));


        // true and e ==> e
        String sql5 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where -5=-5 AND query_time=0;";
        String explainString5 = getSQLPlanOrErrorMsg("EXPLAIN " + sql5);
        Assert.assertTrue(explainString5.contains("PREDICATES: `query_time` = 0"));

        // e and true ==> e
        String sql6 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where query_time=0 AND -5=-5;";
        String explainString6 = getSQLPlanOrErrorMsg("EXPLAIN " + sql6);
        Assert.assertTrue(explainString6.contains("PREDICATES: `query_time` = 0"));

        // false and e ==> false
        String sql7 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where -5!=-5 AND query_time=0;";
        String explainString7 = getSQLPlanOrErrorMsg("EXPLAIN " + sql7);
        Assert.assertTrue(!explainString7.contains("FALSE"));

        // e and false ==> false
        String sql8 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where query_time=0 AND -5!=-5;";
        String explainString8 = getSQLPlanOrErrorMsg("EXPLAIN " + sql8);
        Assert.assertTrue(!explainString8.contains("FALSE"));

        // (false or expr1) and (false or expr2) ==> expr1 and expr2
        String sql9 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where (-2=2 or query_time=2) and (-2=2 or stmt_id=2);";
        String explainString9 = getSQLPlanOrErrorMsg("EXPLAIN " + sql9);
        Assert.assertTrue(explainString9.contains("PREDICATES: `query_time` = 2 AND `stmt_id` = 2"));

        // false or (expr and true) ==> expr
        String sql10 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test.test1 where (2=-2) OR (query_time=0 AND 1=1);";
        String explainString10 = getSQLPlanOrErrorMsg("EXPLAIN " + sql10);
        Assert.assertTrue(explainString10.contains("PREDICATES: `query_time` = 0"));
    }

    @Test
    public void testOutfile() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        Config.enable_outfile_to_local = true;
        createTable("CREATE TABLE test.`outfile1` (\n"
                + "  `date` date NOT NULL,\n"
                + "  `road_code` int(11) NOT NULL DEFAULT \"-1\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`date`, `road_code`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`date`)\n"
                + "(PARTITION v2x_ads_lamp_source_percent_statistic_20210929 VALUES [('2021-09-29'), ('2021-09-30')))\n"
                + "DISTRIBUTED BY HASH(`road_code`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        // test after query rewrite, outfile still work
        String sql = "select * from test.outfile1 where `date` between '2021-10-07' and '2021-10-11'"
                + "INTO OUTFILE \"file:///tmp/1_\" FORMAT AS CSV PROPERTIES ("
                + "     \"column_separator\" = \",\","
                + "     \"line_delimiter\" = \"\\n\","
                + "     \"max_file_size\" = \"500MB\" );";
        String explainStr = getSQLPlanOrErrorMsg("EXPLAIN " + sql);
        if (Config.enable_date_conversion) {
            Assert.assertTrue(explainStr.contains("PREDICATES: `date` >= '2021-10-07' AND"
                    + " `date` <= '2021-10-11'"));
        } else {
            Assert.assertTrue(explainStr.contains("PREDICATES: `date` >= '2021-10-07 00:00:00' AND"
                    + " `date` <= '2021-10-11 00:00:00'"));
        }
    }

    // Fix: issue-#7929
    @Test
    public void testEmptyNodeWithOuterJoinAndAnalyticFunction() throws Exception {
        // create database
        String createDbStmtStr = "create database issue7929;";
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().createDb(createDbStmt);
        createTable(" CREATE TABLE issue7929.`t1` (\n"
                + "  `k1` int(11) NULL COMMENT \"\",\n"
                + "  `k2` int(11) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")");
        createTable("CREATE TABLE issue7929.`t2` (\n"
                + "  `j1` int(11) NULL COMMENT \"\",\n"
                + "  `j2` int(11) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`j1`, `j2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`j1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")");
        String sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from issue7929.t1 left join (select max(j1) over() as x from issue7929.t2) a"
                + " on t1.k1 = a.x where 1 = 0;";
        String explainStr = getSQLPlanOrErrorMsg(sql, true);
        Assert.assertTrue(UtFrameUtils.checkPlanResultContainsNode(explainStr, 5, "EMPTYSET"));
        Assert.assertTrue(explainStr.contains("tuple ids: 5"));
    }

    @Ignore
    // Open it after fixing issue #7971
    public void testGroupingSetOutOfBoundError() throws Exception {
        String createDbStmtStr = "create database issue1111;";
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().createDb(createDbStmt);
        createTable("CREATE TABLE issue1111.`test1` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");");
        String sql = "SELECT k1 ,GROUPING(k2) FROM issue1111.test1 GROUP BY CUBE (k1) ORDER BY k1";
        String explainStr = getSQLPlanOrErrorMsg(sql, true);
        System.out.println(explainStr);
    }

    // --begin-- implicit cast in explain verbose
    @Disabled
    public void testExplainInsertInto() throws Exception {
        ExplainTest explainTest = new ExplainTest();
        explainTest.before(connectContext);
        explainTest.testExplainInsertInto();
        explainTest.testExplainVerboseInsertInto();
        explainTest.testExplainSelect();
        explainTest.testExplainVerboseSelect();
        explainTest.testExplainConcatSelect();
        explainTest.testExplainVerboseConcatSelect();
        explainTest.after();
    }
    // --end--

    // --begin-- rewrite date literal rule
    @Disabled
    public void testRewriteDateLiteralRule() throws Exception {
        RewriteDateLiteralRuleTest rewriteDateLiteralRuleTest = new RewriteDateLiteralRuleTest();
        rewriteDateLiteralRuleTest.before(connectContext);
        rewriteDateLiteralRuleTest.testWithDoubleFormatDate();
        rewriteDateLiteralRuleTest.testWithIntFormatDate();
        rewriteDateLiteralRuleTest.testWithInvalidFormatDate();
        rewriteDateLiteralRuleTest.testWithStringFormatDate();
        rewriteDateLiteralRuleTest.testWithDoubleFormatDateV2();
        rewriteDateLiteralRuleTest.testWithIntFormatDateV2();
        rewriteDateLiteralRuleTest.testWithInvalidFormatDateV2();
        rewriteDateLiteralRuleTest.testWithStringFormatDateV2();
        rewriteDateLiteralRuleTest.after();
    }
    // --end--

    @Test
    public void testGroupingSets() throws Exception {
        String createDbStmtStr = "create database issue7971;";
        CreateDbStmt createDbStmt = (CreateDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().createDb(createDbStmt);
        createTable("CREATE TABLE issue7971.`t` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` smallint(6) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")");
        createTable("CREATE TABLE issue7971.`t1` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k21` smallint(6) NULL COMMENT \"\",\n"
                + "  `k31` smallint(6) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ")");
        String sql = "SELECT k1, k2, GROUPING(k1), GROUPING(k2), SUM(k3) FROM issue7971.t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );";
        String explainStr = getSQLPlanOrErrorMsg(sql);
        Assert.assertTrue(explainStr.contains("REPEAT_NODE"));
        sql = "SELECT k1 ,GROUPING(k2) FROM issue7971.t GROUP BY CUBE (k1) ORDER BY k1;";
        explainStr = getSQLPlanOrErrorMsg(sql);
        Assert.assertTrue(explainStr.contains("errCode = 2"));
        sql = "select grouping_id(t1.k1), t1.k1, max(k2) from issue7971.t left join issue7971.t1 on t.k3 = t1.k1 group by grouping sets ((k1), ());";
        explainStr = getSQLPlanOrErrorMsg(sql);
        Assert.assertTrue(explainStr.contains("REPEAT_NODE"));
    }

    @Test
    public void testQueryWithUsingClause() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String iSql1 = "explain insert into test.tbl_using_a values(1,3,7),(2,2,8),(3,1,9)";
        String iSql2 = "explain insert into test.tbl_using_b values(1,3,1),(3,1,1),(4,1,1),(5,2,1)";
        getSQLPlanOrErrorMsg(iSql1);
        getSQLPlanOrErrorMsg(iSql2);
        String qSQL = "explain  select t1.* from test.tbl_using_a t1 join test.tbl_using_b t2 using(k1,k2) where t1.k1 "
                + "between 1 and 3 and t2.k3 between 1+0 and 3+0";
        try {
            getSQLPlanOrErrorMsg(qSQL);
        } catch (AnalysisException e) {
            Assert.fail();
        }
    }

    @Test
    public void testResultExprs() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("CREATE TABLE test.result_exprs (\n"
                + "  `aid` int(11) NULL,\n"
                + "  `bid` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`aid`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`aid`) BUCKETS 7\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_medium\" = \"HDD\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");\n");
        String queryStr = "EXPLAIN VERBOSE INSERT INTO result_exprs\n" + "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.aid,\n" + "       b.bid\n" + "FROM\n"
                + "  (SELECT 3 AS aid)a\n" + "RIGHT JOIN\n" + "  (SELECT 4 AS bid)b ON (a.aid=b.bid)\n";
        String explainString = getSQLPlanOrErrorMsg(queryStr);
        Assert.assertFalse(explainString.contains("OUTPUT EXPRS:\n    3\n    4"));
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains(
                "OUTPUT EXPRS:\n" + "    CAST(<slot 4> <slot 2> 3 AS INT)\n" + "    CAST(<slot 5> <slot 3> 4 AS INT)"));
    }

    @Test
    public void testInsertIntoSelect() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("CREATE TABLE test.`decimal_tb` (\n"
                + "  `k1` decimal(1, 0) NULL COMMENT \"\",\n"
                + "  `v1` decimal(1, 0) SUM NULL COMMENT \"\",\n"
                + "  `v2` decimal(1, 0) MAX NULL COMMENT \"\",\n"
                + "  `v3` decimal(1, 0) MIN NULL COMMENT \"\",\n"
                + "  `v4` decimal(1, 0) REPLACE NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`)\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ")");
        String sql = "explain insert into test.decimal_tb select 1, 1, 1, 1, 1;";
        String explainString = getSQLPlanOrErrorMsg(sql);
        Assert.assertTrue(explainString.contains("1 | 1 | 1 | 1 | 1"));
    }

    @Test
    public void testOutJoinWithOnFalse() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("create table out_join_1\n"
                + "(\n"
                + "    k1 int,\n"
                + "    v int\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable("create table out_join_2\n"
                + "(\n"
                + "    k1 int,\n"
                + "    v int\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        String sql = "explain select * from out_join_1 left join out_join_2 on out_join_1.k1 = out_join_2.k1 and 1=2;";
        String explainString = getSQLPlanOrErrorMsg(sql);
        Assert.assertFalse(explainString.contains("non-equal LEFT OUTER JOIN is not supported"));

        sql = "explain select * from out_join_1 right join out_join_2 on out_join_1.k1 = out_join_2.k1 and 1=2;";
        explainString = getSQLPlanOrErrorMsg(sql);
        Assert.assertFalse(explainString.contains("non-equal RIGHT OUTER JOIN is not supported"));

        sql = "explain select * from out_join_1 full join out_join_2 on out_join_1.k1 = out_join_2.k1 and 1=2;";
        explainString = getSQLPlanOrErrorMsg(sql);
        Assert.assertFalse(explainString.contains("non-equal FULL OUTER JOIN is not supported"));

    }

    @Test
    public void testDefaultJoinReorder() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("CREATE TABLE t1 (col1 varchar, col2 varchar, col3 int)\n" + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 3\n" + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n" + ");");
        createTable("CREATE TABLE t2 (col1 varchar, col2 varchar, col3 int)\n" + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 3\n" + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n" + ");");
        createTable("CREATE TABLE t3 (col1 varchar, col2 varchar, col3 int)\n" + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 3\n" + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n" + ");");
        String sql = "explain select x.col2 from t1,t2,t3 x,t3 y "
                + "where x.col1=t2.col1 and y.col1=t2.col2 and t1.col1=y.col1";
        String explainString = getSQLPlanOrErrorMsg(sql);
        Assert.assertFalse(explainString.contains("CROSS JOIN"));

    }

    @Test
    public void testDefaultJoinReorderWithView() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("CREATE TABLE t_1 (col1 varchar, col2 varchar, col3 int)\n" + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 3\n" + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n" + ");");
        createTable("CREATE TABLE t_2 (col1 varchar, col2 varchar, col3 int)\n" + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 3\n" + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n" + ");");
        createView("CREATE VIEW v_1 as select col1 from t_1;");
        createView("CREATE VIEW v_2 as select x.col2 from (select t_2.col2, 1 + 1 from t_2) x;");

        String sql = "explain select t_1.col2, v_1.col1 from t_1 inner join t_2 on t_1.col1 = t_2.col1 inner join v_1 "
                + "on v_1.col1 = t_2.col2 inner join v_2 on v_2.col2 = t_2.col1";
        String explainString = getSQLPlanOrErrorMsg(sql);
        System.out.println(explainString);
        // errCode = 2, detailMessage = Unknown column 'col2' in 't_2'
        Assert.assertFalse(explainString.contains("errCode"));
    }

    @Test
    public void testKeyOrderError() throws Exception {
        Assertions.assertTrue(getSQLPlanOrErrorMsg("CREATE TABLE `test`.`test_key_order` (\n"
                + "  `k1` tinyint(4) NULL COMMENT \"\",\n"
                + "  `k2` smallint(6) NULL COMMENT \"\",\n"
                + "  `k3` int(11) NULL COMMENT \"\",\n"
                + "  `v1` double MAX NULL COMMENT \"\",\n"
                + "  `v2` float SUM NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k3`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");").contains("Key columns should be a ordered prefix of the schema. "
                + "KeyColumns[1] (starts from zero) is k3, "
                + "but corresponding column is k2 in the previous columns declaration."));
    }

    @Test
    public void testPreaggregationOfOrthogonalBitmapUDAF() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("CREATE TABLE test.bitmap_tb (\n"
                + "  `id` int(11) NULL COMMENT \"\",\n"
                + "  `id2` int(11) NULL COMMENT \"\",\n"
                + "  `id3` bitmap bitmap_union NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`id`,`id2`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\"\n"
                + ");");

        String queryBaseTableStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ id,id2,orthogonal_bitmap_union_count(id3) from test.bitmap_tb t1 group by id,id2";
        String explainString1 = getSQLPlanOrErrorMsg(queryBaseTableStr);
        Assert.assertTrue(explainString1.contains("PREAGGREGATION: ON"));

        String queryTableStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ id,orthogonal_bitmap_union_count(id3) from test.bitmap_tb t1 group by id";
        String explainString2 = getSQLPlanOrErrorMsg(queryTableStr);
        Assert.assertTrue(explainString2.contains("PREAGGREGATION: ON"));
    }

    @Test
    public void testPreaggregationOfHllUnion() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        createTable("create table test.test_hll(\n"
                + "    dt date,\n"
                + "    id int,\n"
                + "    name char(10),\n"
                + "    province char(10),\n"
                + "    os char(10),\n"
                + "    pv hll hll_union\n"
                + ")\n"
                + "Aggregate KEY (dt,id,name,province,os)\n"
                + "distributed by hash(id) buckets 10\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"in_memory\"=\"false\"\n"
                + ");");

        String queryBaseTableStr = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ dt, hll_union(pv) from test.test_hll group by dt";
        String explainString = getSQLPlanOrErrorMsg(queryBaseTableStr);
        Assert.assertTrue(explainString.contains("PREAGGREGATION: ON"));
    }

    /*
    NOTE:
    explainString.contains("PREDICATES: xxx\n")
    add '\n' at the end of line to ensure there are no other predicates
     */
    @Test
    public void testRewriteOrToIn() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where query_time = 1 or query_time = 2 or query_time in (3, 4)";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2, 3, 4)\n"));

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2) and query_time in (3, 4)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) AND `query_time` IN (3, 4)\n"));

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2 or scan_bytes = 2) and scan_bytes in (2, 3)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) OR `scan_bytes` = 2 AND `scan_bytes` IN (2, 3)\n"));

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2) and (scan_bytes = 2 or scan_bytes = 3)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) AND `scan_bytes` IN (2, 3)\n"));

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where query_time = 1 or query_time = 2 or query_time = 3 or query_time = 1";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2, 3)\n"));

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where query_time = 1 or query_time = 2 or query_time in (3, 2)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2, 3)\n"));

        connectContext.getSessionVariable().setRewriteOrToInPredicateThreshold(100);
        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where query_time = 1 or query_time = 2 or query_time in (3, 4)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` = 1 OR `query_time` = 2 OR `query_time` IN (3, 4)\n"));
        connectContext.getSessionVariable().setRewriteOrToInPredicateThreshold(2);

        sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2) and query_time in (3, 4)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) AND `query_time` IN (3, 4)\n"));

        //test we can handle `!=` and `not in`
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2 or query_time!= 3 or query_time not in (5, 6))";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) OR `query_time` != 3 OR `query_time` NOT IN (5, 6)\n"));

        //test we can handle merge 2 or more columns
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2 or scan_rows = 3 or scan_rows = 4)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2) OR `scan_rows` IN (3, 4)"));

        //merge in-pred or in-pred
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (query_time = 1 or query_time = 2 or query_time = 3 or query_time = 4)";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains("PREDICATES: `query_time` IN (1, 2, 3, 4)\n"));

        //rewrite recursively
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 "
                + "where query_id=client_ip "
                + "      and (stmt_id=1 or stmt_id=2 or stmt_id=3 "
                + "           or (user='abc' and (state = 'a' or state='b' or state in ('c', 'd'))))"
                + "      or (db not in ('x', 'y')) ";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(
                "PREDICATES: `query_id` = `client_ip` "
                        + "AND (`stmt_id` IN (1, 2, 3) OR `user` = 'abc' AND `state` IN ('a', 'b', 'c', 'd')) "
                        + "OR (`db` NOT IN ('x', 'y'))\n"));

        //ExtractCommonFactorsRule may generate more expr, test the rewriteOrToIn applied on generated exprs
        sql = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from test1 where (stmt_id=1 and state='a') or (stmt_id=2 and state='b')";
        explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        Assert.assertTrue(explainString.contains(
                "PREDICATES: `state` IN ('a', 'b') AND `stmt_id` IN (1, 2) AND"
                        + " `stmt_id` = 1 AND `state` = 'a' OR `stmt_id` = 2 AND `state` = 'b'\n"
        ));
    }
}
