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

package org.apache.doris.nereids;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NereidsPlannerQueryLogContextTest {
    private boolean savedEnabled;
    private boolean savedRunningUnitTest;
    private String savedQueryId;
    private ConnectContext savedConnectContext;

    @BeforeEach
    public void setUp() {
        savedEnabled = Config.sys_log_enable_query_id;
        savedRunningUnitTest = FeConstants.runningUnitTest;
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        savedConnectContext = ConnectContext.get();
        Config.sys_log_enable_query_id = true;
        FeConstants.runningUnitTest = true;
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (savedConnectContext != null) {
            savedConnectContext.setThreadLocalInfo();
        }
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
        Config.sys_log_enable_query_id = savedEnabled;
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNestedPlannerFailureKeepsIdentityThroughResourceRelease(boolean innerHasQueryId) {
        List<String> observations = new ArrayList<>();
        ConnectContext outer = new ConnectContext();
        outer.setQueryId(new TUniqueId(1, 2));
        outer.setThreadLocalInfo();
        ConnectContext inner = new ConnectContext();
        if (innerHasQueryId) {
            inner.setQueryId(new TUniqueId(3, 4));
        }
        IllegalArgumentException failure = new IllegalArgumentException("inner preprocessing failed");
        NereidsPlanner innerPlanner = new NereidsPlanner(observingStatement(inner, observations, "inner")) {
            @Override
            protected LogicalPlan preprocess(LogicalPlan plan) {
                observations.add("inner:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                throw failure;
            }
        };
        NereidsPlanner outerPlanner = new NereidsPlanner(observingStatement(outer, observations, "outer")) {
            @Override
            protected LogicalPlan preprocess(LogicalPlan plan) {
                observations.add("outer:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(inner)) {
                    try {
                        innerPlanner.planWithLock(plan, PhysicalProperties.ANY, ExplainLevel.NONE);
                    } finally {
                        observations.add("inner-return:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                    }
                } finally {
                    observations.add("outer-resumed:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                }
                throw new AssertionError("inner planning must fail");
            }
        };
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> outerPlanner.planWithLock(Mockito.mock(LogicalPlan.class),
                        PhysicalProperties.ANY, ExplainLevel.NONE)));
        String innerIdentity = innerHasQueryId ? "3-4" : "1-2";
        Assertions.assertEquals(Arrays.asList("outer:1-2", "inner:" + innerIdentity,
                "inner-release:" + innerIdentity, "inner-return:" + (innerHasQueryId ? "3-4" : "null"),
                "outer-resumed:1-2", "outer-release:1-2"), observations);
        Assertions.assertSame(outer, ConnectContext.get());
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        assertNextPlannerHasNoInheritedIdentity();
    }

    @Test
    public void testCleanupFailureRestoresIdentityAfterParsedPlanReturn() {
        List<String> observations = new ArrayList<>();
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, 2));
        context.setThreadLocalInfo();
        IllegalStateException failure = new IllegalStateException("resource release failed");
        StatementContext statement = new StatementContext(context, new OriginStatement("select 1", 0)) {
            @Override
            public synchronized void releasePlannerResources() {
                observations.add(ThreadContext.get(QueryLogContext.QUERY_ID));
                super.releasePlannerResources();
                throw failure;
            }
        };
        NereidsPlanner planner = new NereidsPlanner(statement);
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> planner.planWithLock(Mockito.mock(LogicalPlan.class),
                        PhysicalProperties.ANY, ExplainLevel.PARSED_PLAN)));
        Assertions.assertEquals(Arrays.asList("1-2"), observations);
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        assertNextPlannerHasNoInheritedIdentity();
    }

    @Test
    public void testDisabledPlanningLeavesCallerIdentityUntouched() {
        Config.sys_log_enable_query_id = false;
        List<String> observations = new ArrayList<>();
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, 2));
        context.setThreadLocalInfo();
        IllegalArgumentException failure = new IllegalArgumentException("preprocessing failed");
        NereidsPlanner planner = new NereidsPlanner(observingStatement(context, observations, "disabled")) {
            @Override
            protected LogicalPlan preprocess(LogicalPlan plan) {
                observations.add("disabled:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                return QueryLogContext.withPlanningContext(new TUniqueId(3, 4), () -> {
                    observations.add("nested:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                    throw failure;
                });
            }
        };
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> planner.planWithLock(Mockito.mock(LogicalPlan.class),
                        PhysicalProperties.ANY, ExplainLevel.NONE)));
        Assertions.assertEquals(Arrays.asList("disabled:caller", "nested:caller", "disabled-release:caller"),
                observations);
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        Config.sys_log_enable_query_id = true;
        assertNextPlannerHasNoInheritedIdentity();
    }

    private StatementContext observingStatement(ConnectContext context, List<String> observations, String name) {
        return new StatementContext(context, new OriginStatement("select 1", 0)) {
            @Override
            public synchronized void releasePlannerResources() {
                observations.add(name + "-release:" + ThreadContext.get(QueryLogContext.QUERY_ID));
                super.releasePlannerResources();
            }
        };
    }

    private void assertNextPlannerHasNoInheritedIdentity() {
        List<String> observations = new ArrayList<>();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        NereidsPlanner planner = new NereidsPlanner(observingStatement(context, observations, "next"));
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        ThreadContext.put(QueryLogContext.QUERY_ID, "reused-worker");
        Assertions.assertSame(plan, planner.planWithLock(plan, PhysicalProperties.ANY, ExplainLevel.PARSED_PLAN));
        Assertions.assertEquals(Arrays.asList("next-release:null"), observations);
        Assertions.assertEquals("reused-worker", ThreadContext.get(QueryLogContext.QUERY_ID));
    }
}
