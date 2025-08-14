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
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.ShowProcedureStmt;
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
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.DescribeCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabasesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStorageEnginesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowViewCommand;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.help.HelpModule;
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
        ShowDatabasesCommand command = new ShowDatabasesCommand(null, null, null);
        ShowResultSet resultSet = null;
        try {
            resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
    }

    @Test
    public void testShowDbPattern() throws AnalysisException {
        ShowDatabasesCommand command = new ShowDatabasesCommand(null, "empty%", null);
        ShowResultSet resultSet = null;
        try {
            resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowDbFromCatalog() throws AnalysisException {
        ShowDatabasesCommand command = new ShowDatabasesCommand(InternalCatalog.INTERNAL_CATALOG_NAME, null, null);
        ShowResultSet resultSet = null;
        try {
            resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
    }

    @Test
    public void testShowDbPriv() throws Exception {
        ctx.setEnv(AccessTestUtil.fetchBlockCatalog());
        ShowDatabasesCommand command = new ShowDatabasesCommand(null, null, null);
        command.doRun(ctx, new StmtExecutor(ctx, ""));
    }

    @Test
    public void testShowTable() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                null, false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowViews() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                null, false, PlanType.SHOW_VIEWS);
        ShowResultSet resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableFromCatalog() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                "internal", false, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableFromUnknownDatabase() throws Exception {
        ShowTableCommand command = new ShowTableCommand("emptyDb",
                null, false, PlanType.SHOW_TABLES);

        expectedEx.expect(Exception.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        command.doRun(ctx, new StmtExecutor(ctx, ""));
    }

    @Test
    public void testShowTablePattern() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                null, false, "empty%", null, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));

        Assert.assertFalse(resultSet.next());
    }

    @Ignore
    @Test
    public void testDescribe() {
        SystemInfoService clusterInfo = AccessTestUtil.fetchSystemInfoService();
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

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "testDb", "testTbl");
        DescribeCommand command = new DescribeCommand(tableNameInfo, false, null);
        ShowResultSet resultSet = null;
        try {
            resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));
            Assert.assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testShowTableVerbose() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                null, true, PlanType.SHOW_TABLES);
        ShowResultSet resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowView() throws UserException {
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testUser", "%"));
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "testDb", "testTbl");
        ShowViewCommand command = new ShowViewCommand("testDb", tableNameInfo);
        ShowResultSet resultSet = null;
        try {
            resultSet = command.doRun(ctx, new StmtExecutor(ctx, ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowEngine() throws Exception {
        ShowStorageEnginesCommand command = new ShowStorageEnginesCommand();
        ShowResultSet resultSet = command.doRun(ctx, null);

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
}
