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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;


public class ExplainTest {
    private static String runningDir = "fe/mocked/TableFunctionPlanTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);

        String t1 =("CREATE TABLE test.t1 (\n" +
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
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(t1, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);

        String t2 =("CREATE TABLE test.t2 (\n" +
                "  `dt` bigint(11) COMMENT \"\",\n" +
                "  `id` bigint(11) COMMENT \"\",\n" +
                "  `value` bigint(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");
        createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(t2, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);

    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @Test
    public void testExplainInsertInto() throws Exception {
        String sql = "explain insert into test.t1 select * from test.t2";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, true);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("CAST"));
    }

    @Test
    public void testExplainSelect() throws Exception {
        String sql = "explain select * from test.t1 where dt = '1001';";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, false);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    @Test
    public void testExplainVerboseSelect() throws Exception {
        String queryStr = "explain verbose select * from test.t1 where dt = '1001';";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, queryStr, true);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("CAST"));
    }

    @Test
    public void testExplainConcatSelect() throws Exception {
        String sql = "explain select concat(dt, id) from test.t1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, false);
        System.out.println(explainString);
        Assert.assertFalse(explainString.contains("CAST"));
    }

    @Test
    public void testExplainVerboseConcatSelect() throws Exception {
        String sql = "explain verbose select concat(dt, id) from test.t1;";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql, true);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("CAST"));
    }
}
