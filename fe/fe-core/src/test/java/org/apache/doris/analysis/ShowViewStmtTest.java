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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ShowViewStmtTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static String runningDir = "fe/mocked/ShowViewTest/" + UUID.randomUUID().toString() + "/";
    private static DorisAssert dorisAssert;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        String testTbl1 = "CREATE TABLE `test1` (\n"
                + "  `a` int(11) NOT NULL COMMENT \"\",\n"
                + "  `b` int(11) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`a`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`a`) BUCKETS 8\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        String testTbl2 = "CREATE TABLE `test2` (\n"
                + "  `c` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d` int(11) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`c`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`c`) BUCKETS 8\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        String testTbl3 = "CREATE TABLE `test3` (\n"
                + "  `e` int(11) NOT NULL COMMENT \"\",\n"
                + "  `f` int(11) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`e`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`e`) BUCKETS 8\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");";

        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase("testDb").useDatabase("testDb");
        dorisAssert.withTable(testTbl1)
                   .withTable(testTbl2)
                   .withTable(testTbl3);
    }

    @Test
    public void testNormal() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test1"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        Assert.assertEquals("SHOW VIEW FROM `testDb`.`test1`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals("test1", stmt.getTbl());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("View", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Create View", stmt.getMetaData().getColumn(1).getName());
    }

    @Test(expected = UserException.class)
    public void testNoDb() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "", "testTbl"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        Assert.fail();
    }

    @Test
    public void testShowView() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String testView1 = "CREATE VIEW `view1` as \n"
                + "SELECT a, b FROM test1;";
        dorisAssert.withView(testView1);

        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test1"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view1", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        dorisAssert.dropView("view1");
    }

    @Test
    public void testShowViewWithJoin() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String testView2 = "CREATE VIEW `view2` as \n"
                + "SELECT a, c FROM test1 \n"
                + "LEFT OUTER JOIN test2 \n"
                + "ON test1.a = test2.c;";
        dorisAssert.withView(testView2);

        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test1"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test2"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        dorisAssert.dropView("view2");
    }

    @Test
    public void testShowViewWithNestedSqlView() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String testView3 = "CREATE VIEW `view3` as \n"
                + "SELECT a, d FROM test1 \n"
                + "LEFT OUTER JOIN \n"
                + "(SELECT d, e FROM test3 LEFT OUTER JOIN test2 ON test3.e = test2.c) test4 \n"
                + "ON test1.a = test4.e;";
        dorisAssert.withView(testView3);

        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test1"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view3", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test2"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view3", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test3"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("view3", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        dorisAssert.dropView("view3");
    }

    @Test
    public void testShowViewWithNestedView() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String testView4 = "CREATE VIEW `view4` as \n"
                + "SELECT a, b FROM test1;";
        String testView5 = "CREATE VIEW `view5` as \n"
                + "SELECT c FROM test2 \n"
                + "LEFT OUTER JOIN view4 \n"
                + "ON test2.c = view4.a;";
        dorisAssert.withView(testView4);
        dorisAssert.withView(testView5);

        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "test1"));
        stmt.analyze(new Analyzer(ctx.getEnv(), ctx));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(2, resultSet.getResultRows().size());
        List<String> views = Arrays.asList("view4", "view5");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(views.contains(resultSet.getString(0)));
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(views.contains(resultSet.getString(0)));

        dorisAssert.dropView("view4")
                   .dropView("view5");
    }

    @Test
    public void testGetTableRefs() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "with w as (select a from testDb.test1) "
                + "select c, d from testDb.test2 "
                + "left outer join "
                + "(select e from testDb.test3 join w on testDb.test3.e = w.a) test4 "
                + "on test1.b = test4.d";
        SqlScanner input = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(input);
        QueryStmt queryStmt = (QueryStmt) SqlParserUtils.getFirstStmt(parser);
        List<TableRef> tblRefs = Lists.newArrayList();
        Set<String> parentViewNameSet = Sets.newHashSet();
        queryStmt.getTableRefs(new Analyzer(ctx.getEnv(), ctx), tblRefs, parentViewNameSet);

        Assert.assertEquals(3, tblRefs.size());
        Assert.assertEquals("test1", tblRefs.get(0).getName().getTbl());
        Assert.assertEquals("test2", tblRefs.get(1).getName().getTbl());
        Assert.assertEquals("test3", tblRefs.get(2).getName().getTbl());
    }
}
