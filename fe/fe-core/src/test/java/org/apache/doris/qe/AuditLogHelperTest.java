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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
}
