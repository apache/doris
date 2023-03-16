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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;


public class ExplainTest {
    private static ConnectContext ctx;

    public void before(ConnectContext ctx) throws Exception {
        this.ctx = ctx;
        String createDbStmtStr = "create database test_explain;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Env.getCurrentEnv().createDb(createDbStmt);

        String t1 = ("CREATE TABLE test_explain.explain_t1 (\n"
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
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(t1, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);

        String t2 = ("CREATE TABLE test_explain.explain_t2 (\n"
                + "  `dt` bigint(11) COMMENT \"\",\n"
                + "  `id` bigint(11) COMMENT \"\",\n"
                + "  `value` bigint(8) COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`dt`, `id`)\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"10\"))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ");");
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(t2, ctx);
        Env.getCurrentEnv().createTable(createTableStmt);
    }

    public void after() throws Exception {
        String dropSchemaSql = "drop schema if exists test_explain";
        String dropDbSql = "drop database if exists test_explain";
        DropDbStmt dropSchemaStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropSchemaSql, ctx);
        DropDbStmt dropDbStmt = (DropDbStmt) UtFrameUtils.parseAndAnalyzeStmt(dropDbSql, ctx);
        Assert.assertEquals(dropDbStmt.toSql(), dropSchemaStmt.toSql());
    }

    public void testExplainInsertInto() throws Exception {
        String sql = "explain insert into test_explain.explain_t1 select * from test_explain.explain_t2";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, false);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    public void testExplainVerboseInsertInto() throws Exception {
        String sql = "explain verbose insert into test_explain.explain_t1 select * from test_explain.explain_t2";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, true);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("CAST"));
    }

    public void testExplainSelect() throws Exception {
        String sql = "explain select * from test_explain.explain_t1 where dt = '1001';";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, false);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    public void testExplainVerboseSelect() throws Exception {
        String queryStr = "explain verbose select * from test_explain.explain_t1 where dt = '1001';";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, queryStr, true);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    public void testExplainConcatSelect() throws Exception {
        String sql = "explain select concat(dt, id) from test_explain.explain_t1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, false);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    public void testExplainVerboseConcatSelect() throws Exception {
        String sql = "explain verbose select concat(dt, id) from test_explain.explain_t1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, true);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("CAST"));
    }
}
