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
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.DescribeCommand;
import org.apache.doris.nereids.trees.plans.commands.HelpCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowDatabasesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowStorageEnginesCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowViewCommand;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.URL;

public class ShowExecutorTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext ctx;
    private Env env;
    private InternalCatalog catalog;
    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<ConnectContext> mockedConnectContextStatic;

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
        Partition partition = Mockito.spy(Deencapsulation.newInstance(Partition.class));
        Mockito.doReturn(index1).when(partition).getBaseIndex();

        // mock table
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn("testTbl").when(table).getName();
        Mockito.doReturn(TableType.OLAP).when(table).getType();
        Mockito.doReturn(Lists.newArrayList(column1, column2)).when(table).getBaseSchema();
        Mockito.doReturn(KeysType.AGG_KEYS).when(table).getKeysType();
        Mockito.doReturn(new SinglePartitionInfo()).when(table).getPartitionInfo();
        Mockito.doReturn(new RandomDistributionInfo(10)).when(table).getDefaultDistributionInfo();
        Mockito.doReturn(0L).when(table).getIndexIdByName(Mockito.anyString());
        Mockito.doReturn(TStorageType.COLUMN).when(table).getStorageTypeByIndexId(0L);
        Mockito.doReturn(partition).when(table).getPartition(Mockito.anyLong());
        Mockito.doReturn(null).when(table).getCopiedBfColumns();

        // mock database
        Database db = Mockito.spy(new Database());
        Mockito.doNothing().when(db).readLock();
        Mockito.doNothing().when(db).readUnlock();
        Mockito.doReturn(table).when(db).getTableNullable(Mockito.anyString());

        // mock auth
        AccessControllerManager accessManager = AccessTestUtil.fetchAdminAccess();

        // mock catalog
        catalog = Mockito.spy(Deencapsulation.newInstance(InternalCatalog.class));
        Mockito.doReturn(db).when(catalog).getDbNullable("testDb");
        Mockito.doReturn(null).when(catalog).getDbNullable("emptyDb");

        CatalogMgr dsMgr = Mockito.spy(new CatalogMgr());
        Mockito.doReturn(catalog).when(dsMgr).getCatalog(Mockito.anyString());
        Mockito.doReturn(catalog).when(dsMgr).getCatalogOrException(Mockito.anyString(), Mockito.any());
        Mockito.doReturn(catalog).when(dsMgr).getCatalogOrAnalysisException(Mockito.anyString());

        // mock catalog.
        env = Mockito.spy(Deencapsulation.newInstance(Env.class));
        Mockito.doReturn(catalog).when(env).getInternalCatalog();
        Mockito.doReturn(catalog).when(env).getCurrentCatalog();
        Mockito.doReturn(accessManager).when(env).getAccessManager();
        Mockito.doReturn(dsMgr).when(env).getCatalogMgr();

        mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(() -> Env.getDdlStmt(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyLong()
        )).thenAnswer(inv -> null);

        // mock scheduler
        ConnectScheduler scheduler = Mockito.spy(new ConnectScheduler(10));
        Mockito.doReturn(Lists.newArrayList(ctx.toThreadInfo(false))).when(scheduler)
                .listConnection(Mockito.eq("testUser"), Mockito.anyBoolean());

        ctx.changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        ctx.setConnectScheduler(scheduler);
        ctx.setEnv(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        mockedConnectContextStatic = Mockito.mockStatic(ConnectContext.class, Mockito.CALLS_REAL_METHODS);
        mockedConnectContextStatic.when(ConnectContext::get).thenReturn(ctx);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedConnectContextStatic != null) {
            mockedConnectContextStatic.close();
        }
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
    public void testShowStream() throws Exception {
        ShowTableCommand command = new ShowTableCommand("testDb",
                null, false, PlanType.SHOW_STREAMS);
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

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(clusterInfo);

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
    public void testHelp() throws Exception {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("test-help-resource-show-help.zip");
        module.setUpByZip(help.getPath());
        try (MockedStatic<HelpModule> mockedHelpModule = Mockito.mockStatic(HelpModule.class)) {
            mockedHelpModule.when(HelpModule::getInstance).thenReturn(module);

            // topic
            HelpCommand command = new HelpCommand("ADD");
            ShowResultSet resultSet = command.doRun(ctx, null);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("ADD", resultSet.getString(0));
            Assertions.assertEquals("add function\n", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());

            // topic
            command = new HelpCommand("logical");
            resultSet = command.doRun(ctx, null);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("OR", resultSet.getString(0));
            Assertions.assertFalse(resultSet.next());

            // keywords
            command = new HelpCommand("MATH");
            resultSet = command.doRun(ctx, null);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("ADD", resultSet.getString(0));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("MINUS", resultSet.getString(0));
            Assertions.assertFalse(resultSet.next());

            // category
            command = new HelpCommand("functions");
            resultSet = command.doRun(ctx, null);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("HELP", resultSet.getString(0));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("binary function", resultSet.getString(0));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("bit function", resultSet.getString(0));
            Assertions.assertFalse(resultSet.next());

            // empty
            command = new HelpCommand("empty");
            resultSet = command.doRun(ctx, null);

            Assertions.assertFalse(resultSet.next());
        }
    }
}
