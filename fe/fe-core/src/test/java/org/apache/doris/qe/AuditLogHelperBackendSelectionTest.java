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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;

public class AuditLogHelperBackendSelectionTest {

    @After
    public void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    public void testAuditLogDoesNotResolveQuerySelectionDecision() throws Exception {
        assertAuditComputeGroup(null, "UNKNOWN", "", null);
    }

    @Test
    public void testIntegratedAuditLogUsesRecordedLoadResourceGroupAsComputeGroup() throws Exception {
        assertAuditComputeGroup(new BackendSelection.SelectionHint(
                "load_group", BackendSelection.Mode.PREFER, "test"), "load_group", "", null);
    }

    @Test
    public void testCloudAuditLogKeepsCloudComputeGroup() throws Exception {
        assertAuditComputeGroup(new BackendSelection.SelectionHint(
                "load_group", BackendSelection.Mode.PREFER, "test"), "cloud_group", "cloud", "cloud_group");
    }

    @Test
    public void testSuccessfulExternalInsertWaitsForItsCoordinatorBackends() throws Exception {
        InsertIntoTableCommand command = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(command.stmtType()).thenReturn(StmtType.INSERT);
        assertExternalDmlWaitsForCoordinatorBackends(command, true);
    }

    @Test
    public void testFailedExternalInsertWaitsForItsCoordinatorBackends() throws Exception {
        InsertIntoTableCommand command = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(command.stmtType()).thenReturn(StmtType.INSERT);
        assertExternalDmlWaitsForCoordinatorBackends(command, false);
    }

    @Test
    public void testExternalInsertOverwriteWaitsForItsCoordinatorBackends() throws Exception {
        InsertOverwriteTableCommand command = Mockito.mock(InsertOverwriteTableCommand.class);
        Mockito.when(command.stmtType()).thenReturn(StmtType.INSERT);
        assertExternalDmlWaitsForCoordinatorBackends(command, true);
    }

    @Test
    public void testFilesInsertWaitsForItsCoordinatorBackends() throws Exception {
        InsertIntoTVFCommand command = Mockito.mock(InsertIntoTVFCommand.class);
        Mockito.when(command.stmtType()).thenReturn(StmtType.INSERT);
        assertExternalDmlWaitsForCoordinatorBackends(command, true);
    }

    @Test
    public void testInternalInsertDoesNotUseExternalDmlBarrier() {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class, Mockito.CALLS_REAL_METHODS);

        Assert.assertTrue(AuditLogHelper.getExternalDmlAuditBackendIds(executor).isEmpty());
    }

    @Test
    public void testForwardedExternalDmlUsesBackendIdsReturnedByMaster() {
        Set<Long> expectedBackendIds = Set.of(10001L, 10002L);
        MasterOpExecutor masterExecutor = Mockito.mock(MasterOpExecutor.class);
        Mockito.when(masterExecutor.getAuditStatisticsBackendIds()).thenReturn(expectedBackendIds);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(executor, "masterOpExecutor", masterExecutor);

        Assert.assertEquals(expectedBackendIds,
                AuditLogHelper.getExternalDmlAuditBackendIds(executor));
    }

    @Test
    public void testResolvedExternalDmlUsesOnlyDispatchedBackends() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getDispatchedBackendIdsForAudit()).thenReturn(Set.of(10001L));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class, Mockito.CALLS_REAL_METHODS);

        executor.setExternalDmlAuditCoordinator(coordinator);

        Assert.assertEquals(Set.of(10001L), executor.getExternalDmlAuditBackendIds());
    }

    private void assertExternalDmlWaitsForCoordinatorBackends(LogicalPlan command, boolean success)
            throws Exception {
        ConnectContext context = Mockito.spy(new ConnectContext());
        context.setStartTime();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        if (success) {
            context.getState().setOk();
        } else {
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "external write failed");
        }

        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getExternalDmlAuditBackendIds()).thenReturn(Set.of(10001L, 10002L));
        Mockito.when(executor.getSummaryProfile()).thenReturn(Mockito.mock(SummaryProfile.class));
        context.setExecutor(executor);

        LogicalPlanAdapter statement = new LogicalPlanAdapter(command, new StatementContext());

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            AuditLogHelper.logAuditLog(context, "insert into external_table select 1",
                    statement, null, true);

            Mockito.verify(statusMgr).submitFinishQueryToAudit(
                    Mockito.any(AuditEvent.class), Mockito.eq(Set.of(10001L, 10002L)));
        }
    }

    private void assertAuditComputeGroup(BackendSelection.SelectionHint hint, String expectedComputeGroup,
            String deployMode, String cloudComputeGroup) throws Exception {
        String oldDeployMode = Config.deploy_mode;
        Config.deploy_mode = deployMode;
        ConnectContext context = Mockito.spy(new ConnectContext());
        context.setStartTime();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.getState().setOk();
        context.recordLoadBackendSelectionDecision(hint);
        if (cloudComputeGroup != null) {
            Mockito.doReturn(cloudComputeGroup).when(context).getCloudCluster(false);
        }

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);
        BackendSelectionProvider provider = Mockito.mock(BackendSelectionProvider.class);

        BackendSelectionManager.setProviderForTest(provider);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            AuditLogHelper.logAuditLog(context, "insert into t values (1)", null, null, true);

            ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr).submitFinishQueryToAudit(captor.capture());
            Assert.assertEquals(expectedComputeGroup, captor.getValue().cloudClusterName);
            Mockito.verifyNoInteractions(provider);
        } finally {
            Config.deploy_mode = oldDeployMode;
        }
    }
}
