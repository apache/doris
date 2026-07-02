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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.ResourceGroupAffinityPolicyFactory;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AuditLogHelperTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    private ConnectContext createMockContext(boolean isQuery, boolean isInternal) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStartTime();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.getState().setIsQuery(isQuery);
        ctx.getState().setInternal(isInternal);
        return ctx;
    }

    @Test
    public void testUpdateMetricsQueryOk() {
        ConnectContext ctx = createMockContext(true, false);
        ctx.getState().setOk(0, 0, "");

        long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

        Assert.assertEquals(1, after - before);
    }

    @Test
    public void testUpdateMetricsQueryErr() {
        ConnectContext ctx = createMockContext(true, false);
        ctx.getState().setError("test error");

        long beforeAll = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long beforeErr = MetricRepo.COUNTER_QUERY_ERR.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long afterAll = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long afterErr = MetricRepo.COUNTER_QUERY_ERR.getValue();

        Assert.assertEquals(1, afterAll - beforeAll);
        Assert.assertEquals(1, afterErr - beforeErr);
    }

    @Test
    public void testUpdateMetricsNotQuery() {
        // INSERT scenario: isQuery = false, counters should not change
        ConnectContext ctx = createMockContext(false, false);
        ctx.getState().setOk(1, 0, "");

        long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
        AuditLogHelper.updateMetrics(ctx);
        long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

        Assert.assertEquals(0, after - before);
    }

    @Test
    public void testUpdateMetricsDebugModeShortCircuit() {
        boolean original = Config.enable_bdbje_debug_mode;
        try {
            Config.enable_bdbje_debug_mode = true;

            ConnectContext ctx = createMockContext(true, false);
            ctx.getState().setOk(0, 0, "");

            long before = MetricRepo.COUNTER_QUERY_ALL.getValue();
            AuditLogHelper.updateMetrics(ctx);
            long after = MetricRepo.COUNTER_QUERY_ALL.getValue();

            Assert.assertEquals(0, after - before);
        } finally {
            Config.enable_bdbje_debug_mode = original;
        }
    }

    @Test
    public void testAuditLogDoesNotResolveQueryAffinityDecision() {
        ConnectContext ctx = createMockContext(false, false);
        ctx.getState().setOk();

        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        WorkloadRuntimeStatusMgr workloadRuntimeStatusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(workloadRuntimeStatusMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                        Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get)
                    .thenThrow(new AssertionError("audit log should not resolve query affinity"));

            AuditLogHelper.logAuditLog(ctx, "set enable_profile = true", null, null, true);

            ArgumentCaptor<AuditEvent> auditEventCaptor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(workloadRuntimeStatusMgr).submitFinishQueryToAudit(auditEventCaptor.capture());
            Assert.assertEquals("", auditEventCaptor.getValue().effectivePreferredResourceGroup);
            Assert.assertEquals("random", auditEventCaptor.getValue().resourceGroupSelectPolicy);
            mockedFactory.verifyNoInteractions();
        }
    }
}
