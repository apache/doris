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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AdminCleanTrashCommandTest {
    private Env env = Mockito.mock(Env.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    public void setUp() {
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);

        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(env.getCurrentSystemInfo()).thenReturn(systemInfoService);
    }

    @AfterEach
    public void tearDown() {
        mockedConnectContext.close();
        mockedEnv.close();
    }

    @Test
    public void testDedupBackendsWithDuplicateQueries() throws Exception {
        Backend be1 = new Backend(10001L, "192.168.0.1", 9050);
        Backend be2 = new Backend(10002L, "192.168.0.2", 9050);
        ImmutableMap<Long, Backend> backendsMap = ImmutableMap.of(
                be1.getId(), be1,
                be2.getId(), be2);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(backendsMap);

        List<AgentBatchTask> capturedTasks = new ArrayList<>();
        try (MockedStatic<AgentTaskExecutor> mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                    .thenAnswer(invocation -> {
                        capturedTasks.add(invocation.getArgument(0));
                        return null;
                    });

            List<String> duplicateQueries = Arrays.asList("192.168.0.1:9050", "192.168.0.1:9050");
            AdminCleanTrashCommand command = new AdminCleanTrashCommand(duplicateQueries);
            command.run(connectContext, null);

            Assertions.assertEquals(1, capturedTasks.size());
            Assertions.assertEquals(1, capturedTasks.get(0).getTaskNum(),
                    "Should only send one clean trash task for duplicate backend queries");
        }
    }

    @Test
    public void testDedupBackendsWithDistinctQueries() throws Exception {
        Backend be1 = new Backend(10003L, "192.168.1.1", 9050);
        Backend be2 = new Backend(10004L, "192.168.1.2", 9050);
        ImmutableMap<Long, Backend> backendsMap = ImmutableMap.of(
                be1.getId(), be1,
                be2.getId(), be2);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(backendsMap);

        List<AgentBatchTask> capturedTasks = new ArrayList<>();
        try (MockedStatic<AgentTaskExecutor> mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                    .thenAnswer(invocation -> {
                        capturedTasks.add(invocation.getArgument(0));
                        return null;
                    });

            List<String> queries = Arrays.asList("192.168.1.1:9050", "192.168.1.2:9050");
            AdminCleanTrashCommand command = new AdminCleanTrashCommand(queries);
            command.run(connectContext, null);

            Assertions.assertEquals(1, capturedTasks.size());
            Assertions.assertEquals(2, capturedTasks.get(0).getTaskNum(),
                    "Should send one clean trash task per distinct backend");
        }
    }

    @Test
    public void testCleanAllBackendsWithNoDuplicates() throws Exception {
        Backend be1 = new Backend(10005L, "192.168.2.1", 9050);
        Backend be2 = new Backend(10006L, "192.168.2.2", 9050);
        ImmutableMap<Long, Backend> backendsMap = ImmutableMap.of(
                be1.getId(), be1,
                be2.getId(), be2);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(backendsMap);

        List<AgentBatchTask> capturedTasks = new ArrayList<>();
        try (MockedStatic<AgentTaskExecutor> mockedExecutor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            mockedExecutor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                    .thenAnswer(invocation -> {
                        capturedTasks.add(invocation.getArgument(0));
                        return null;
                    });

            AdminCleanTrashCommand command = new AdminCleanTrashCommand();
            command.run(connectContext, null);

            Assertions.assertEquals(1, capturedTasks.size());
            Assertions.assertEquals(2, capturedTasks.get(0).getTaskNum(),
                    "Should send one clean trash task per backend when cleaning all");
        }
    }
}
