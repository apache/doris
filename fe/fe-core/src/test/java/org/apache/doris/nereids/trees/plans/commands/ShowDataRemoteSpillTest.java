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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;

public class ShowDataRemoteSpillTest {
    private static final TableNameInfo tableNameInfo =
            new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    private Database database = Mockito.mock(Database.class);
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;
    private String savedDeployMode;
    private String savedCloudUniqueId;

    @BeforeEach
    public void setUp() {
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getDefaultCatalog()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
        mockedConnectContext.close();
        mockedEnv.close();
    }

    // Two alive BEs report 20 and 12 bytes of remote spill; a dead BE is not counted.
    private SystemInfoService mockBackendsWithRemoteSpill() throws AnalysisException {
        Backend be1 = new Backend(10001L, "127.0.0.1", 9050);
        be1.setAlive(true);
        be1.setRemoteSpillBytes(20L);
        Backend be2 = new Backend(10002L, "127.0.0.2", 9050);
        be2.setAlive(true);
        be2.setRemoteSpillBytes(12L);
        Backend deadBe = new Backend(10003L, "127.0.0.3", 9050);
        deadBe.setAlive(false);
        deadBe.setRemoteSpillBytes(100L);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(
                be1.getId(), be1, be2.getId(), be2, deadBe.getId(), deadBe));
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        return systemInfo;
    }

    @Test
    public void testWarehouseIncludesRemoteSpillInCloudTotal() throws Exception {
        Config.deploy_mode = "cloud";
        mockBackendsWithRemoteSpill();
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(ImmutableMap.of("db1", 10L));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getName()).thenReturn("db1");
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(3, result.getMetaData().getColumnCount());
        Assertions.assertEquals(ImmutableList.of("db1", "10", "0"), result.getResultRows().get(0));
        Assertions.assertEquals(ImmutableList.of("__remote_spill__", "32", "0"),
                result.getResultRows().get(1));
        Assertions.assertEquals(ImmutableList.of("total", "42", "0"), result.getResultRows().get(2));
    }

    @Test
    public void testWarehouseRestrictedToDatabasesDoesNotShowRemoteSpill() throws Exception {
        Config.deploy_mode = "cloud";
        SystemInfoService systemInfo = mockBackendsWithRemoteSpill();
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(ImmutableMap.of("db1", 10L));
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getName()).thenReturn("db1");
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true", "db_names", "db1"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("db1", "10", "0"),
                ImmutableList.of("total", "10", "0")), result.getResultRows());
        Mockito.verifyNoInteractions(systemInfo);
    }

    @Test
    public void testWarehouseOmitsRemoteSpillOutsideCloudMode() throws Exception {
        SystemInfoService systemInfo = mockBackendsWithRemoteSpill();
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(new CatalogRecycleBin());
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(ImmutableMap.of());

        ShowDataCommand command = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true"), false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("total", "0", "0")),
                result.getResultRows());
        Mockito.verifyNoInteractions(systemInfo);
    }

    // SHOW DATA without a database lists every database; the remote spill gets its own row and
    // is counted in the total size.
    @Test
    public void testAllDatabasesIncludeRemoteSpillInCloudTotal() throws Exception {
        Config.deploy_mode = "cloud";
        mockBackendsWithRemoteSpill();
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        Mockito.when(recycleBin.getDbToRecycleSize()).thenReturn(new HashMap<>());
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(connectContext.getDatabase()).thenReturn("");
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getUsedDataSize()).thenReturn(Pair.of(10L, 5L));
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null, null, false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(6, result.getMetaData().getColumnCount());
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("1", "db1", "10", "5", "0", "0"),
                ImmutableList.of("NULL", "__remote_spill__", "32", "0", "0", "0"),
                ImmutableList.of("Total", "NULL", "42", "5", "0", "0")), result.getResultRows());
    }

    @Test
    public void testAllDatabasesOmitRemoteSpillOutsideCloudMode() throws Exception {
        SystemInfoService systemInfo = mockBackendsWithRemoteSpill();
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        Mockito.when(recycleBin.getDbToRecycleSize()).thenReturn(new HashMap<>());
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(connectContext.getDatabase()).thenReturn("");
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(1L);
        Mockito.when(database.getUsedDataSize()).thenReturn(Pair.of(10L, 5L));
        Mockito.when(database.getTables()).thenReturn(ImmutableList.of());

        ShowDataCommand command = new ShowDataCommand(null, null, null, false);
        ShowResultSet result = command.doRun(connectContext, null);
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("1", "db1", "10", "5", "0", "0"),
                ImmutableList.of("Total", "NULL", "10", "5", "0", "0")), result.getResultRows());
        Mockito.verifyNoInteractions(systemInfo);
    }

    // Only the master FE keeps the remote spill size reported by the BEs: the two cloud listings
    // that show it are forwarded, every other form is served locally.
    @Test
    public void testForwardedToMasterOnlyForCloudWarehouseListings() {
        Config.deploy_mode = "cloud";
        Mockito.when(connectContext.getDatabase()).thenReturn("");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowDataCommand(null, null, null, false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowDataCommand(null, null, ImmutableMap.of("entire_warehouse", "true"), false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null,
                        ImmutableMap.of("entire_warehouse", "true", "db_names", "db1"), false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(tableNameInfo, null, null, false).toRedirectStatus());

        // A database is selected: the tables of that database, served locally.
        Mockito.when(connectContext.getDatabase()).thenReturn(CatalogMocker.TEST_DB_NAME);
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null, null, false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowDataCommand(null, null, ImmutableMap.of("entire_warehouse", "true"), false).toRedirectStatus());

        Config.deploy_mode = "";
        Mockito.when(connectContext.getDatabase()).thenReturn("");
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null, null, false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null, ImmutableMap.of("entire_warehouse", "true"), false).toRedirectStatus());
    }

}
