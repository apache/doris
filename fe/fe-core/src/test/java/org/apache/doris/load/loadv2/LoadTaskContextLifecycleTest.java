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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.cloud.load.CloudBrokerLoadJob;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class LoadTaskContextLifecycleTest {
    @ParameterizedTest
    @CsvSource({"false, true, false", "false, false, true", "true, true, true", "true, true, false"})
    void scopesPlanningAndExecutionToTheJob(boolean cloud, boolean hasCaller, boolean failPlanning) throws Exception {
        String previousCloudId = Config.cloud_unique_id;
        String previousDeployMode = Config.deploy_mode;
        Config.cloud_unique_id = cloud ? "load-test" : "";
        Config.deploy_mode = cloud ? "cloud" : "local";
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        CloudSystemInfoService cluster = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(cluster.getClusterNameByClusterId("job-id")).thenReturn("job-group");
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        ComputeGroup group = new ComputeGroup("job-group", "job-id", null);
        Mockito.when(env.getAuth().getComputeGroup(UserIdentity.ROOT.getQualifiedUser())).thenReturn(group);
        BackendSelection.SelectionHint hint = BackendSelection.SelectionHint.noSelection();
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(12L);
        AtomicReference<ConnectContext> planning = new AtomicReference<>();
        AtomicReference<ConnectContext> execution = new AtomicReference<>();
        StatementContext plannedStatement = Mockito.mock(StatementContext.class);
        StatementContext executingStatement = Mockito.mock(StatementContext.class);
        RuntimeException failure = new IllegalStateException("task failed");
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            environment.when(Env::getCurrentSystemInfo).thenReturn(cluster);
            ConnectContext caller = hasCaller ? new ConnectContext() : null;
            try (AutoCloseConnectContext scope = hasCaller ? new AutoCloseConnectContext(caller) : null;
                    MockedConstruction<NereidsLoadingTaskPlanner> planners = Mockito.mockConstruction(
                            NereidsLoadingTaskPlanner.class, (planner, construction) -> Mockito.doAnswer(call -> {
                                ConnectContext task = ConnectContext.get();
                                planning.set(task);
                                assertJobContext(caller, task, cloud, group, hint);
                                task.setStatementContext(plannedStatement);
                                task.getBackendSelectionProfile().recordLoadCoordinator(hint, backend);
                                if (failPlanning) {
                                    throw failure;
                                }
                                return null;
                            }).when(planner).plan(Mockito.any(), Mockito.anyList(), Mockito.eq(0)))) {
                if (hasCaller) {
                    caller.setCurrentUserIdentity(UserIdentity.ADMIN);
                    caller.setAuthenticatedRoles(Set.of("worker-role"));
                    caller.getSessionVariable().setCloudCluster("worker-group");
                }
                BrokerLoadJob job = cloud ? new CloudBrokerLoadJob() : new BrokerLoadJob();
                job.sessionVariables.put("clusterId", "job-id");
                job.userInfo = UserIdentity.ROOT;
                job.setLoadBackendSelectionHint(hint);
                Database db = Mockito.mock(Database.class);
                BrokerPendingTaskAttachment files = Mockito.mock(BrokerPendingTaskAttachment.class);
                if (failPlanning) {
                    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                            () -> job.createTask(db, null, List.of(), false, 1024, null, files)));
                } else {
                    LoadLoadingTask task = job.createTask(db, null, List.of(), false, 1024, null, files);
                    Mockito.verify(planners.constructed().get(0)).setLoadBackendSelectionHint(hint);
                    String summary = Deencapsulation.getField(job, "loadBackendSelectionSummary");
                    Assertions.assertTrue(summary.contains("coordinator_backend=12"));
                    job.setLoadBackendSelectionHint(null);
                    try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
                        factories.when(EnvFactory::getInstance).thenAnswer(call -> {
                            ConnectContext active = ConnectContext.get();
                            execution.set(active);
                            assertJobContext(caller, active, cloud, group, hint);
                            active.setStatementContext(executingStatement);
                            throw failure;
                        });
                        Assertions.assertSame(failure,
                                Assertions.assertThrows(RuntimeException.class, task::executeOnce));
                    }
                    Assertions.assertNotSame(planning.get(), execution.get());
                    Assertions.assertTrue(execution.get().isClosingOrClosed());
                    Mockito.verify(executingStatement).close();
                }
                Assertions.assertTrue(planning.get().isClosingOrClosed());
                Mockito.verify(plannedStatement).close();
                Assertions.assertSame(caller, ConnectContext.get());
                if (hasCaller) {
                    Assertions.assertFalse(caller.isClosingOrClosed());
                    Assertions.assertSame(UserIdentity.ADMIN, caller.getCurrentUserIdentity());
                    Assertions.assertEquals(Set.of("worker-role"), caller.getAuthenticatedRoles());
                    Assertions.assertEquals("worker-group", caller.getSessionVariable().getCloudCluster());
                    Assertions.assertNull(caller.getLoadBackendSelectionHint());
                    Assertions.assertNull(caller.getStatementContext());
                }
            }
        } finally {
            Config.cloud_unique_id = previousCloudId;
            Config.deploy_mode = previousDeployMode;
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static void assertJobContext(ConnectContext caller, ConnectContext task, boolean cloud,
            ComputeGroup group, BackendSelection.SelectionHint hint) throws Exception {
        Assertions.assertNotSame(caller, task);
        Assertions.assertSame(UserIdentity.ROOT, task.getCurrentUserIdentity());
        Assertions.assertTrue(task.getAuthenticatedRoles().isEmpty());
        Assertions.assertSame(hint, task.getLoadBackendSelectionHint());
        if (cloud) {
            Assertions.assertEquals("job-group", task.getCloudCluster());
        } else {
            Assertions.assertSame(group, task.getComputeGroupSafely());
        }
    }
}
