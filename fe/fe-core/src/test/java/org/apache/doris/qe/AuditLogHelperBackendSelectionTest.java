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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
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
