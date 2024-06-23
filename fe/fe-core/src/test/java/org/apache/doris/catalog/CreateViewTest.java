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

package org.apache.doris.catalog;

import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class CreateViewTest {

    private static String runningDir = "fe/mocked/CreateViewTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
        // create table
        String createTableStmtStr = "create table test.tbl1(k1 int, k2 int, v1 int, v2 int) duplicate key(k1)"
                + " distributed by hash(k2) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTableStmtStr,
                connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
        // create table with array type
        String createTableWithArrayStmtStr = "create table test.tbl2(id int, c_array array<int(11)>) duplicate key(id)"
                + " distributed by hash(id) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableWithArrayStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(
                createTableWithArrayStmtStr, connectContext);
        Env.getCurrentEnv().createTable(createTableWithArrayStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createView(String sql) throws Exception {
        CreateViewStmt createViewStmt = (CreateViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createView(createViewStmt);
    }

    @Test
    public void testNormal() throws DdlException {

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view1(t1, t2, t3, t4) as select 'a', 'b', 1, 1.2;"));

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view2 as select k1, k2, v1, v2 from test.tbl1;"));

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view3 as select \"hello ' world\" as a1;"));

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view4 as select abs(-1) as s1;"));

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view5 as select * from test.tbl1 where hour(now()) > 3"
                        + " and curdate() > '2021-06-26';"));

        // test union all
        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view6 as "
                        + "select * from test.tbl1 where curdate() > '2021-06-26' order by k1 limit 10 "
                        + "union all "
                        + "select * from test.tbl1 where curdate() > '2021-06-26' order by k2 limit 10, 50;"));
        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view7 (k1, k2) as "
                        + "select k1, k2 from test.tbl1 where curdate() > '2021-06-26' order by k1 limit 10 "
                        + "union all "
                        + "select k1, k2 from test.tbl1 where curdate() > '2021-06-26' order by k2 limit 10, 50;"));

        // test array type
        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.view8 as select * from test.tbl2;"));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");

        View view1 = (View) db.getTableOrDdlException("view1");
        Assert.assertEquals(4, view1.getFullSchema().size());
        Assert.assertNotNull(view1.getColumn("t1"));
        Assert.assertNotNull(view1.getColumn("t2"));
        Assert.assertNotNull(view1.getColumn("t3"));
        Assert.assertNotNull(view1.getColumn("t4"));

        View view2 = (View) db.getTableOrDdlException("view2");
        Assert.assertEquals(4, view1.getFullSchema().size());
        Assert.assertNotNull(view2.getColumn("k1"));
        Assert.assertNotNull(view2.getColumn("k2"));
        Assert.assertNotNull(view2.getColumn("v1"));
        Assert.assertNotNull(view2.getColumn("v2"));

        View view3 = (View) db.getTableOrDdlException("view3");
        Assert.assertEquals(1, view3.getFullSchema().size());
        Assert.assertNotNull(view3.getColumn("a1"));

        View view4 = (View) db.getTableOrDdlException("view4");
        Assert.assertEquals(1, view4.getFullSchema().size());
        Assert.assertNotNull(view4.getColumn("s1"));

        View view6 = (View) db.getTableOrDdlException("view6");
        Assert.assertEquals(4, view6.getFullSchema().size());
        Assert.assertNotNull(view6.getColumn("k1"));
        Assert.assertNotNull(view6.getColumn("k2"));
        Assert.assertNotNull(view6.getColumn("v1"));
        Assert.assertNotNull(view6.getColumn("v2"));

        View view7 = (View) db.getTableOrDdlException("view7");
        Assert.assertEquals(2, view7.getFullSchema().size());
        Assert.assertNotNull(view7.getColumn("k1"));
        Assert.assertNotNull(view7.getColumn("k2"));

        View view8 = (View) db.getTableOrDdlException("view8");
        Assert.assertEquals(2, view8.getFullSchema().size());
        Assert.assertNotNull(view8.getColumn("id"));
        Assert.assertNotNull(view8.getColumn("c_array"));
    }

    @Test
    public void testNestedViews() throws Exception {
        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.nv1 as select * from test.tbl1;"));

        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.nv2 as select * from test.nv1;"));

        String sql = "select * from test.nv2";
        String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("OlapScanNode"));
    }

    @Test
    public void testAlterView() throws Exception {
        String originStmt = "select k1 as kc1, sum(k2) as kc2 from test.tbl1 group by kc1";
        ExceptionChecker.expectThrowsNoException(
                () -> createView("create view test.alter1 as " + originStmt));
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        View alter1 = (View) db.getTableOrDdlException("alter1");
        Assert.assertEquals(
                "SELECT `k1` AS `kc1`, sum(`k2`) AS `kc2` FROM `test`.`tbl1` GROUP BY `kc1`",
                alter1.getInlineViewDef());

        String alterStmt
                = "alter view test.alter1 as with test1_cte (w1, w2) as (select k1, k2 from test.tbl1) "
                + "select w1 as c1, sum(w2) as c2 from test1_cte where w1 > 10 group by w1 order by w1";
        AlterViewStmt alterViewStmt = (AlterViewStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmt, connectContext);
        Env.getCurrentEnv().alterView(alterViewStmt);

        alter1 = (View) db.getTableOrDdlException("alter1");
        Assert.assertEquals(
                "WITH test1_cte(w1, w2) AS (SELECT `k1`, `k2` FROM `test`.`tbl1`) "
                        + "SELECT `w1` AS `c1`, sum(`w2`) AS `c2` FROM `test1_cte` WHERE (`w1` > 10) GROUP BY `w1` "
                        + "ORDER BY `w1` ASC NULLS FIRST",
                alter1.getInlineViewDef());
    }
}
