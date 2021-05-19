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
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // create table
        String createTableStmtStr = "create table test.tbl1(k1 int, k2 int, v1 int, v2 int) duplicate key(k1)"
                + " distributed by hash(k2) buckets 1 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTableStmtStr, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createView(String sql) throws Exception {
        CreateViewStmt createViewStmt = (CreateViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createView(createViewStmt);
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


        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");

        View view1 = (View) db.getTable("view1");
        Assert.assertEquals(4, view1.getFullSchema().size());
        Assert.assertNotNull(view1.getColumn("t1"));
        Assert.assertNotNull(view1.getColumn("t2"));
        Assert.assertNotNull(view1.getColumn("t3"));
        Assert.assertNotNull(view1.getColumn("t4"));

        View view2 = (View) db.getTable("view2");
        Assert.assertEquals(4, view1.getFullSchema().size());
        Assert.assertNotNull(view2.getColumn("k1"));
        Assert.assertNotNull(view2.getColumn("k2"));
        Assert.assertNotNull(view2.getColumn("v1"));
        Assert.assertNotNull(view2.getColumn("v2"));

        View view3 = (View) db.getTable("view3");
        Assert.assertEquals(1, view3.getFullSchema().size());
        Assert.assertNotNull(view3.getColumn("a1"));

        View view4 = (View) db.getTable("view4");
        Assert.assertEquals(1, view4.getFullSchema().size());
        Assert.assertNotNull(view4.getColumn("s1"));
    }
}
