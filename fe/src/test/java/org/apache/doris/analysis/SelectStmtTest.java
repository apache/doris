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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class SelectStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("db1").useDatabase("db1");
        dorisAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testGroupingSets() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String selectStmtStr = "select k1,k2,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k2),(k1),(k2),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr, ctx);
        String selectStmtStr2 = "select k1,k4,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("column: `k4` cannot both in select list and aggregate functions when using GROUPING"
                + " SETS/CUBE/ROLLUP, please use union instead.");
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr2, ctx);
        String selectStmtStr3 = "select k1,k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr3, ctx);
        String selectStmtStr4 = "select k1,k4+k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),()"
                + ");";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr4, ctx);
    }

    @Test
    public void testSubqueryInCase() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql1 = "SELECT CASE\n" +
                "        WHEN (\n" +
                "            SELECT COUNT(*) / 2\n" +
                "            FROM db1.tbl1\n" +
                "        ) > k4 THEN (\n" +
                "            SELECT AVG(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "        ELSE (\n" +
                "            SELECT SUM(k4)\n" +
                "            FROM db1.tbl1\n" +
                "        )\n" +
                "    END AS kk4\n" +
                "FROM db1.tbl1;";
        SelectStmt stmt = (SelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql1, ctx);
        stmt.rewriteExprs(new Analyzer(ctx.getCatalog(), ctx).getExprRewriter());
        Assert.assertEquals("SELECT CASE WHEN `$a$1`.`$c$1` > `k4` THEN `$a$2`.`$c$2` ELSE `$a$3`.`$c$3` END" +
                " AS `kk4` FROM `default_cluster:db1`.`tbl1` (SELECT count(*) / 2.0 AS `count(*) / 2.0` FROM " +
                "`default_cluster:db1`.`tbl1`) $a$1 (SELECT avg(`k4`) AS `avg(``k4``)` FROM" +
                " `default_cluster:db1`.`tbl1`) $a$2 (SELECT sum(`k4`) AS `sum(``k4``)` " +
                "FROM `default_cluster:db1`.`tbl1`) $a$3", stmt.toSql());
    }
}
