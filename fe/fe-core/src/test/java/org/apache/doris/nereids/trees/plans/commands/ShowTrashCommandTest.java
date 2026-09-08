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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.proc.TrashProcDir;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowTrashCommandTest {

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<ConnectContext> mockedConnectContextStatic;
    private MockedStatic<TrashProcDir> mockedTrashProcDirStatic;

    private SystemInfoService systemInfoService;
    private ConnectContext ctx;
    private StmtExecutor executor;
    private AccessControllerManager accessManager;

    @BeforeEach
    public void setUp() {
        systemInfoService = Mockito.mock(SystemInfoService.class);
        ctx = Mockito.mock(ConnectContext.class);
        executor = Mockito.mock(StmtExecutor.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        mockedConnectContextStatic = Mockito.mockStatic(ConnectContext.class);
        mockedConnectContextStatic.when(ConnectContext::get).thenReturn(ctx);

        Env fakeEnv = Mockito.mock(Env.class);
        Mockito.when(fakeEnv.getAccessManager()).thenReturn(accessManager);

        Mockito.when(accessManager.checkGlobalPriv(Mockito.eq(ctx), Mockito.any())).thenReturn(true);

        mockedEnvStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(fakeEnv);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

        mockedTrashProcDirStatic = Mockito.mockStatic(TrashProcDir.class);
        mockedTrashProcDirStatic.when(() -> TrashProcDir.getTrashInfo(
                Mockito.anyList(),
                Mockito.anyList()
        )).thenAnswer(invocation -> null);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedConnectContextStatic != null) {
            mockedConnectContextStatic.close();
        }
        if (mockedTrashProcDirStatic != null) {
            mockedTrashProcDirStatic.close();
        }
    }

    @Test
    public void testHandleShowTrashWithLegacySingleBackend() throws Exception {
        Backend be1 = new Backend(1001L, "192.168.1.1", 9050);
        ImmutableMap<Long, Backend> allBackendsMap = ImmutableMap.of(1001L, be1);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(allBackendsMap);

        // Test legacy single backend syntax
        ShowTrashCommand command = new ShowTrashCommand(Lists.newArrayList("192.168.1.1:9050"));
        command.doRun(ctx, executor);
        List<Backend> resultBackends = command.getBackends();
        Assertions.assertNotNull(resultBackends);
        Assertions.assertEquals(1, resultBackends.size());
        Assertions.assertEquals(1001L, resultBackends.get(0).getId());
    }

    @Test
    public void testHandleShowTrashWithNewSyntaxSingleBackend() throws Exception {
        Backend be1 = new Backend(1001L, "192.168.1.1", 9050);
        ImmutableMap<Long, Backend> allBackendsMap = ImmutableMap.of(1001L, be1);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(allBackendsMap);

        // Test new syntax with single backend in parentheses
        ShowTrashCommand command = new ShowTrashCommand(Lists.newArrayList("192.168.1.1:9050"));
        command.doRun(ctx, executor);
        List<Backend> resultBackends = command.getBackends();
        Assertions.assertNotNull(resultBackends);
        Assertions.assertEquals(1, resultBackends.size());
        Assertions.assertEquals(1001L, resultBackends.get(0).getId());
    }

    @Test
    public void testHandleShowTrashWithoutFilter() throws Exception {
        // Cover the no-filter branch (SHOW TRASH without ON)
        Backend be1 = new Backend(1001L, "192.168.1.1", 9050);
        Backend be2 = new Backend(1002L, "192.168.1.2", 9050);
        ImmutableMap<Long, Backend> allBackendsMap = ImmutableMap.of(1001L, be1, 1002L, be2);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(allBackendsMap);

        // Construct command with no backend filters
        ShowTrashCommand command = new ShowTrashCommand();
        command.doRun(ctx, executor);

        // Verify that all backends are returned
        List<Backend> resultBackends = command.getBackends();
        Assertions.assertNotNull(resultBackends);
        Assertions.assertEquals(2, resultBackends.size());
    }

    @Test
    public void testHandleShowTrashWithUnmatchedBackend() throws Exception {
        // Cover the non-matching-backend branch
        Backend be1 = new Backend(1001L, "192.168.1.1", 9050);
        ImmutableMap<Long, Backend> allBackendsMap = ImmutableMap.of(1001L, be1);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(allBackendsMap);

        // Pass a non-existent backend address
        ShowTrashCommand command = new ShowTrashCommand(Lists.newArrayList("999.999.999.999:9999"));
        command.doRun(ctx, executor);

        // Verify that the matched result is empty
        List<Backend> resultBackends = command.getBackends();
        Assertions.assertNotNull(resultBackends);
        Assertions.assertTrue(resultBackends.isEmpty());
    }

}
