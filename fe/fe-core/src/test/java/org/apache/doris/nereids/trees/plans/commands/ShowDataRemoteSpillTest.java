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
import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
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
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedContext;
    private InternalCatalog catalog;
    private ConnectContext context;
    private String savedDeployMode;
    private String savedCloudUniqueId;

    @BeforeEach
    public void setUp() throws AnalysisException {
        savedDeployMode = Config.deploy_mode;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedContext = Mockito.mockStatic(ConnectContext.class);
        catalog = Mockito.mock(InternalCatalog.class);
        context = Mockito.mock(ConnectContext.class);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedContext.when(ConnectContext::get).thenReturn(context);

        Backend alive = new Backend(10001L, "127.0.0.1", 9050);
        alive.setAlive(true);
        alive.setRemoteSpillBytes(32L);
        Backend dead = new Backend(10002L, "127.0.0.2", 9050);
        dead.setAlive(false);
        dead.setRemoteSpillBytes(100L);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllBackendsByAllCluster()).thenReturn(
                ImmutableMap.of(alive.getId(), alive, dead.getId(), dead));
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);

        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        Mockito.when(recycleBin.getDbToRecycleSize()).thenAnswer(invocation -> new HashMap<>());
        mockedEnv.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
    }

    @AfterEach
    public void tearDown() {
        mockedContext.close();
        mockedEnv.close();
        Config.deploy_mode = savedDeployMode;
        Config.cloud_unique_id = savedCloudUniqueId;
    }

    @Test
    public void testEntireWarehouseIncludesAliveRemoteSpill() throws Exception {
        Database db = Mockito.mock(Database.class);
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(ImmutableMap.of("db1", 10L));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db);
        Mockito.when(db.getName()).thenReturn("db1");

        ShowResultSet result = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true"), false).doRun(context, null);
        Assertions.assertEquals(3, result.getMetaData().getColumnCount());
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("db1", "10", "0"),
                ImmutableList.of("__remote_spill__", "32", "0"),
                ImmutableList.of("total", "42", "0")), result.getResultRows());
    }

    @Test
    public void testRestrictedWarehouseAndSharedNothingDoNotIncludeSpill() throws Exception {
        Mockito.when(catalog.getUsedDataQuota()).thenReturn(ImmutableMap.of());
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of());
        ShowResultSet restricted = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true", "db_names", "db1"), false).doRun(context, null);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("total", "0", "0")),
                restricted.getResultRows());

        Config.deploy_mode = "";
        ShowResultSet sharedNothing = new ShowDataCommand(null, null,
                ImmutableMap.of("entire_warehouse", "true"), false).doRun(context, null);
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of("total", "0", "0")),
                sharedNothing.getResultRows());
    }

    @Test
    public void testAllDatabasesIncludeRemoteSpillInTotal() throws Exception {
        Database db = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNames()).thenReturn(ImmutableList.of("db1"));
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db);
        Mockito.when(db.getUsedDataSize()).thenReturn(Pair.of(10L, 5L));
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(context.getDatabase()).thenReturn("");

        ShowResultSet result = new ShowDataCommand(null, null, null, false).doRun(context, null);
        Assertions.assertEquals(6, result.getMetaData().getColumnCount());
        Assertions.assertEquals(ImmutableList.of(
                ImmutableList.of("0", "db1", "10", "5", "0", "0"),
                ImmutableList.of("NULL", "__remote_spill__", "32", "0", "0", "0"),
                ImmutableList.of("Total", "NULL", "42", "5", "0", "0")), result.getResultRows());
    }

    @Test
    public void testOnlyCloudWarehouseListingsForwardToMaster() {
        Mockito.when(context.getDatabase()).thenReturn("");
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowDataCommand(null, null, null, false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowDataCommand(null, null, ImmutableMap.of("entire_warehouse", "true"), false).toRedirectStatus());
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null,
                        ImmutableMap.of("entire_warehouse", "true", "db_names", "db1"), false).toRedirectStatus());
        Config.deploy_mode = "";
        Assertions.assertEquals(RedirectStatus.NO_FORWARD,
                new ShowDataCommand(null, null, null, false).toRedirectStatus());
    }
}
