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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests to verify that {@link AuditLogHelper} correctly updates query related metrics
 * when logging audit information.
 */
public class AuditLogHelperTest {

    private static final String TEST_USER = "test_user";

    @BeforeClass
    public static void setUpClass() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Before
    public void resetMetrics() {
        // reset counters to zero before every test
        MetricRepo.COUNTER_QUERY_ALL.reset();
        MetricRepo.COUNTER_QUERY_ERR.reset();
        MetricRepo.COUNTER_ANALYSIS_ERR.reset();
        MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(TEST_USER).reset();
        MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd(TEST_USER).reset();
    }

    private void mockEnvIsMaster() {
        new MockUp<org.apache.doris.catalog.Env>() {
            @Mock
            public boolean isMaster() {
                return true; // always behave like master FE in unit tests
            }
        };
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
    public void testSuccessQueryMetrics(@Mocked final ConnectContext ctx) {
        mockEnvIsMaster();
        QueryState state = new QueryState();
        state.setIsQuery(true);
        state.setOk();
        new Expectations() {{
                ctx.getStartTime();
                result = System.currentTimeMillis() - 100; // simulate 100ms latency
                ctx.getQualifiedUser();
                result = TEST_USER;
                minTimes = 0;
                ctx.getClientIP();
                result = "127.0.0.1";
                minTimes = 0;
                ctx.getBdpAuthContext();
                result = null;
                minTimes = 0;
                ctx.getAuditEventBuilder();
                result = new AuditEventBuilder();
                minTimes = 0;
                ctx.getState();
                result = state;
                minTimes = 1;
                ctx.getDatabase();
                result = "testDb";
                minTimes = 0;
                ctx.getSessionVariable();
                result = new SessionVariable();
                minTimes = 0;
                ctx.getReturnRows();
                result = 0L;
                minTimes = 0;
                ctx.getWorkloadGroupName();
                result = "default";
                minTimes = 0;
                ctx.queryId();
                result = null;
                minTimes = 0;
            }};
        long beforeTotal = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long beforeUserTotal = MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(TEST_USER).getValue();
        long beforeHistCount = MetricRepo.HISTO_QUERY_LATENCY.getCount();
        AuditLogHelper.logAuditLog(ctx, "select 1", null, null, false);
        Assert.assertEquals(beforeTotal + 1, (long) MetricRepo.COUNTER_QUERY_ALL.getValue());
        Assert.assertEquals(beforeUserTotal + 1,
                (long) MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(TEST_USER).getValue());
        Assert.assertEquals(beforeHistCount + 1, MetricRepo.HISTO_QUERY_LATENCY.getCount());
    }

    @Test
    public void testErrorQueryMetrics(@Mocked final ConnectContext ctx) {
        mockEnvIsMaster();
        QueryState state = new QueryState();
        state.setIsQuery(true);
        state.setError("some error");
        state.setErrType(QueryState.ErrType.OTHER_ERR);
        new Expectations() {{
                ctx.getStartTime();
                result = System.currentTimeMillis() - 50;
                ctx.getQualifiedUser();
                result = TEST_USER;
                minTimes = 0;
                ctx.getClientIP();
                result = "127.0.0.1";
                minTimes = 0;
                ctx.getBdpAuthContext();
                result = null;
                minTimes = 0;
                ctx.getAuditEventBuilder();
                result = new AuditEventBuilder();
                minTimes = 0;
                ctx.getState();
                result = state;
                minTimes = 1;
                ctx.getDatabase();
                result = "testDb";
                minTimes = 0;
                ctx.getSessionVariable();
                result = new SessionVariable();
                minTimes = 0;
                ctx.getReturnRows();
                result = 0L;
                minTimes = 0;
                ctx.getWorkloadGroupName();
                result = "default";
                minTimes = 0;
                ctx.queryId();
                result = null;
                minTimes = 0;
            }};
        long beforeTotal = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long beforeErr = MetricRepo.COUNTER_QUERY_ERR.getValue();
        AuditLogHelper.logAuditLog(ctx, "bad sql", null, null, false);
        Assert.assertEquals(beforeTotal + 1, (long) MetricRepo.COUNTER_QUERY_ALL.getValue());
        Assert.assertEquals(beforeErr + 1, (long) MetricRepo.COUNTER_QUERY_ERR.getValue());
    }

    @Test
    public void testAnalysisErrorMetrics(@Mocked final ConnectContext ctx) {
        mockEnvIsMaster();
        QueryState state = new QueryState();
        state.setIsQuery(true);
        state.setError("analysis error");
        state.setErrType(QueryState.ErrType.ANALYSIS_ERR);
        new Expectations() {{
                ctx.getStartTime();
                result = System.currentTimeMillis() - 30;
                ctx.getQualifiedUser();
                result = TEST_USER;
                minTimes = 0;
                ctx.getClientIP();
                result = "127.0.0.1";
                minTimes = 0;
                ctx.getBdpAuthContext();
                result = null;
                minTimes = 0;
                ctx.getAuditEventBuilder();
                result = new AuditEventBuilder();
                minTimes = 0;
                ctx.getState();
                result = state;
                minTimes = 1;
                ctx.getDatabase();
                result = "testDb";
                minTimes = 0;
                ctx.getSessionVariable();
                result = new SessionVariable();
                minTimes = 0;
                ctx.getReturnRows();
                result = 0L;
                minTimes = 0;
                ctx.getWorkloadGroupName();
                result = "default";
                minTimes = 0;
                ctx.queryId();
                result = null;
                minTimes = 0;
            }};
        long beforeAnalysisErr = MetricRepo.COUNTER_ANALYSIS_ERR.getValue();
        AuditLogHelper.logAuditLog(ctx, "analysis error sql", null, null, false);
        Assert.assertEquals(beforeAnalysisErr + 1, (long) MetricRepo.COUNTER_ANALYSIS_ERR.getValue());
    }
}
