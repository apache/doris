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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.DescribeStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowColumnStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowDbStmt;
import org.apache.doris.analysis.ShowEnginesStmt;
import org.apache.doris.analysis.ShowProcedureStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.analysis.ShowTableStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.analysis.ShowViewStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.function.Function;

public class ShowExecutorTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext ctx;
    private Env env;
    private InternalCatalog catalog;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);
        column1.setIsKey(true);
        column2.setIsKey(true);
        // mock index 1
        MaterializedIndex index1 = new MaterializedIndex();

        // mock partition
        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index1;
            }
        };

        // mock table
        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getName();
                minTimes = 0;
                result = "testTbl";

                table.getType();
                minTimes = 0;
                result = TableType.OLAP;

                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                table.getKeysType();
                minTimes = 0;
                result = KeysType.AGG_KEYS;

                table.getPartitionInfo();
                minTimes = 0;
                result = new SinglePartitionInfo();

                table.getDefaultDistributionInfo();
                minTimes = 0;
                result = new RandomDistributionInfo(10);

                table.getIndexIdByName(anyString);
                minTimes = 0;
                result = 0L;

                table.getStorageTypeByIndexId(0L);
                minTimes = 0;
                result = TStorageType.COLUMN;

                table.getPartition(anyLong);
                minTimes = 0;
                result = partition;

                table.getCopiedBfColumns();
                minTimes = 0;
                result = null;
            }
        };

        // mock database
        Database db = new Database();
        new Expectations(db) {
            {
                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getTableNullable(anyString);
                minTimes = 0;
                result = table;
            }
        };

        // mock auth
        AccessControllerManager accessManager = AccessTestUtil.fetchAdminAccess();

        // mock catalog
        catalog = Deencapsulation.newInstance(InternalCatalog.class);
        new Expectations(catalog) {
            {
                catalog.getDbNullable("testDb");
                minTimes = 0;
                result = db;

                catalog.getDbNullable("emptyDb");
                minTimes = 0;
                result = null;
            }
        };

        CatalogMgr dsMgr = new CatalogMgr();
        new Expectations(dsMgr) {
            {
                dsMgr.getCatalog((String) any);
                minTimes = 0;
                result = catalog;

                dsMgr.getCatalogOrException((String) any, (Function) any);
                minTimes = 0;
                result = catalog;

                dsMgr.getCatalogOrAnalysisException((String) any);
                minTimes = 0;
                result = catalog;
            }
        };

        // mock catalog.
        env = Deencapsulation.newInstance(Env.class);
        new Expectations(env) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                env.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getDdlStmt((Table) any, (List) any, (List) any, (List) any, anyBoolean, anyBoolean, anyLong);
                minTimes = 0;

                Env.getDdlStmt((Table) any, (List) any, null, null, anyBoolean, anyBoolean, anyLong);
                minTimes = 0;

                env.getCatalogMgr();
                minTimes = 0;
                result = dsMgr;
            }
        };

        // mock scheduler
        ConnectScheduler scheduler = new ConnectScheduler(10);
        new Expectations(scheduler) {
            {
                scheduler.listConnection("testUser", anyBoolean);
                minTimes = 0;
                result = Lists.newArrayList(ctx.toThreadInfo(false));
            }
        };

        ctx.changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        ctx.setConnectScheduler(scheduler);
        ctx.setEnv(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("testUser");
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        new Expectations(ctx) {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };
    }

    @Test
    public void testShowDb() throws AnalysisException {
        Analyzer analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        ShowDbStmt stmt = new ShowDbStmt(null);
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
    }

    @Test
    public void testShowDbPattern() throws AnalysisException {
        ShowDbStmt stmt = new ShowDbStmt("empty%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowDbFromCatalog() throws AnalysisException {
        ShowDbStmt stmt = new ShowDbStmt(null, null, InternalCatalog.INTERNAL_CATALOG_NAME);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
    }

    @Test
    public void testShowDbPriv() throws AnalysisException {
        Analyzer analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        ShowDbStmt stmt = new ShowDbStmt(null);
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ctx.setEnv(AccessTestUtil.fetchBlockCatalog());
        executor.execute();
    }

    @Test
    public void testShowTable() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", null, false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowViews() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", null, false, TableType.VIEW,
                null, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableFromCatalog() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", "internal", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableFromUnknownDatabase() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("emptyDb", null, false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();
    }

    @Test
    public void testShowTablePattern() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", null, false, "empty%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Ignore
    @Test
    public void testDescribe() {
        SystemInfoService clusterInfo = AccessTestUtil.fetchSystemInfoService();
        Analyzer analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        Env env = AccessTestUtil.fetchAdminCatalog();

        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }

            @Mock
            SystemInfoService getCurrentSystemInfo() {
                return clusterInfo;
            }
        };

        DescribeStmt stmt = new DescribeStmt(new TableName(internalCtl, "testDb", "testTbl"), false);
        try {
            stmt.analyze(analyzer);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet;
        try {
            resultSet = executor.execute();
            Assert.assertFalse(resultSet.next());
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testShowVariable() throws AnalysisException {
        // Mock variable
        VariableMgr variableMgr = new VariableMgr();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("var1", "abc"));
        rows.add(Lists.newArrayList("var2", "abc"));
        new Expectations(variableMgr) {
            {
                VariableMgr.dump((SetType) any, (SessionVariable) any, (PatternMatcher) any);
                minTimes = 0;
                result = rows;

                VariableMgr.dump((SetType) any, (SessionVariable) any, null);
                minTimes = 0;
                result = rows;
            }
        };

        ShowVariablesStmt stmt = new ShowVariablesStmt(SetType.SESSION, "var%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var1", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        stmt = new ShowVariablesStmt(SetType.SESSION, null);
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var1", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableVerbose() throws AnalysisException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", null, true, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowCreateDb() throws AnalysisException {
        ctx.setEnv(env);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt(new DbName(InternalCatalog.INTERNAL_CATALOG_NAME, "testDb"));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
        Assert.assertEquals("CREATE DATABASE `testDb`", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test(expected = AnalysisException.class)
    public void testShowCreateNoDb() throws AnalysisException {
        ctx.setEnv(env);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt(new DbName(InternalCatalog.INTERNAL_CATALOG_NAME, "emptyDb"));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        executor.execute();

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testShowCreateTableEmptyDb() throws AnalysisException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName(internalCtl, "emptyDb", "testTable"));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        executor.execute();

        Assert.fail("No Exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testShowCreateTableEmptyTbl() throws AnalysisException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName(internalCtl, "testDb", "emptyTable"));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowColumn() throws AnalysisException {
        ctx.setEnv(env);
        ctx.setQualifiedUser("testUser");
        ShowColumnStmt stmt = new ShowColumnStmt(new TableName(internalCtl, "testDb", "testTbl"), null, null, false);
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(2));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // verbose
        stmt = new ShowColumnStmt(new TableName(internalCtl, "testDb", "testTbl"), null, null, true);
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col2", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertFalse(resultSet.next());

        // pattern
        stmt = new ShowColumnStmt(new TableName(internalCtl, "testDb", "testTable"), null, "%1", true);
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowView() throws UserException {
        ctx.setEnv(env);
        ctx.setQualifiedUser("testUser");
        ShowViewStmt stmt = new ShowViewStmt("", new TableName(internalCtl, "testDb", "testTbl"));
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(true));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowColumnFromUnknownTable() throws AnalysisException {
        ShowColumnStmt stmt = new ShowColumnStmt(new TableName(internalCtl, "emptyDb", "testTable"), null, null, false);
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();

        // empty table
        stmt = new ShowColumnStmt(new TableName(internalCtl, "testDb", "emptyTable"), null, null, true);
        stmt.analyze(AccessTestUtil.fetchAdminAnalyzer(false));
        executor = new ShowExecutor(ctx, stmt);

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();
    }

    @Test
    public void testShowAuthors() throws AnalysisException {
        ShowAuthorStmt stmt = new ShowAuthorStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Assert.assertEquals("Name", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Location", resultSet.getMetaData().getColumn(1).getName());
        Assert.assertEquals("Comment", resultSet.getMetaData().getColumn(2).getName());
    }

    @Test
    public void testShowEngine() throws AnalysisException {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("Olap engine", resultSet.getString(0));
    }

    @Test
    public void testShowEmpty() throws AnalysisException {
        ShowProcedureStmt stmt = new ShowProcedureStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testHelp() throws AnalysisException, IOException, UserException {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("test-help-resource-show-help.zip");
        module.setUpByZip(help.getPath());
        new Expectations(module) {
            {
                HelpModule.getInstance();
                minTimes = 0;
                result = module;
            }
        };

        // topic
        HelpStmt stmt = new HelpStmt("ADD");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("ADD", resultSet.getString(0));
        Assert.assertEquals("add function\n", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());

        // topic
        stmt = new HelpStmt("logical");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("OR", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // keywords
        stmt = new HelpStmt("MATH");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("ADD", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("MINUS", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // category
        stmt = new HelpStmt("functions");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("HELP", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("binary function", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("bit function", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // empty
        stmt = new HelpStmt("empty");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowSqlBlockRule() throws AnalysisException {
        ShowSqlBlockRuleStmt stmt = new ShowSqlBlockRuleStmt("test_rule");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals(8, resultSet.getMetaData().getColumnCount());
        Assert.assertEquals("Name", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Sql", resultSet.getMetaData().getColumn(1).getName());
        Assert.assertEquals("SqlHash", resultSet.getMetaData().getColumn(2).getName());
        Assert.assertEquals("PartitionNum", resultSet.getMetaData().getColumn(3).getName());
        Assert.assertEquals("TabletNum", resultSet.getMetaData().getColumn(4).getName());
        Assert.assertEquals("Cardinality", resultSet.getMetaData().getColumn(5).getName());
        Assert.assertEquals("Global", resultSet.getMetaData().getColumn(6).getName());
        Assert.assertEquals("Enable", resultSet.getMetaData().getColumn(7).getName());
    }

    @Test
    public void testIsShowTablesCaseSensitive() {
        ShowSqlBlockRuleStmt stmt = new ShowSqlBlockRuleStmt("test_case_sensitive");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        GlobalVariable.lowerCaseTableNames = 0;
        Assert.assertEquals(CaseSensibility.TABLE.getCaseSensibility(), executor.isShowTablesCaseSensitive());
        GlobalVariable.lowerCaseTableNames = 1;
        Assert.assertEquals(false, executor.isShowTablesCaseSensitive());
        GlobalVariable.lowerCaseTableNames = 2;
        Assert.assertEquals(false, executor.isShowTablesCaseSensitive());
    }
}
