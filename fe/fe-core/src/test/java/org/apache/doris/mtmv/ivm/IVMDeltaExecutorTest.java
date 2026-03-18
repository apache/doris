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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IVMDeltaExecutorTest {

    private IVMDeltaExecutor deltaExecutor;

    @BeforeEach
    public void setUp() {
        deltaExecutor = new IVMDeltaExecutor();
    }

    @Test
    public void testExecuteEmptyBundles(@Mocked MTMV mtmv) throws AnalysisException {
        IVMRefreshContext context = newContext(mtmv);
        deltaExecutor.execute(context, Collections.emptyList());
    }

    @Test
    public void testExecuteSingleBundleSuccess(@Mocked MTMV mtmv,
            @Mocked Command command) throws Exception {
        ConnectContext mockCtx = new ConnectContext();
        new MockUp<MTMVPlanUtil>() {
            @Mock
            public ConnectContext createMTMVContext(MTMV mv, List<RuleType> disableRules) {
                return mockCtx;
            }
        };

        List<Boolean> runCalled = new ArrayList<>();
        new Expectations() {
            {
                command.run((ConnectContext) any, (StmtExecutor) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void run(ConnectContext ctx, StmtExecutor executor) {
                        runCalled.add(true);
                        ctx.getState().setOk();
                    }
                };
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(mtmv, 0L);
        DeltaCommandBundle bundle = new DeltaCommandBundle(baseTableInfo, command);

        deltaExecutor.execute(newContext(mtmv), Collections.singletonList(bundle));
        Assertions.assertEquals(1, runCalled.size());
    }

    @Test
    public void testExecuteCommandRunThrowsException(@Mocked MTMV mtmv,
            @Mocked Command command) throws Exception {
        ConnectContext mockCtx = new ConnectContext();
        new MockUp<MTMVPlanUtil>() {
            @Mock
            public ConnectContext createMTMVContext(MTMV mv, List<RuleType> disableRules) {
                return mockCtx;
            }
        };

        new Expectations() {
            {
                command.run((ConnectContext) any, (StmtExecutor) any);
                result = new RuntimeException("command run failed");
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(mtmv, 0L);
        DeltaCommandBundle bundle = new DeltaCommandBundle(baseTableInfo, command);

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> deltaExecutor.execute(newContext(mtmv), Collections.singletonList(bundle)));
        Assertions.assertTrue(ex.getMessage().contains("IVM delta execution failed"));
        Assertions.assertTrue(ex.getMessage().contains("command run failed"));
    }

    @Test
    public void testExecuteCommandReturnsErrorState(@Mocked MTMV mtmv,
            @Mocked Command command) throws Exception {
        ConnectContext mockCtx = new ConnectContext();
        new MockUp<MTMVPlanUtil>() {
            @Mock
            public ConnectContext createMTMVContext(MTMV mv, List<RuleType> disableRules) {
                return mockCtx;
            }
        };

        new Expectations() {
            {
                command.run((ConnectContext) any, (StmtExecutor) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void run(ConnectContext ctx, StmtExecutor executor) {
                        ctx.getState().setError("something went wrong");
                    }
                };
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(mtmv, 0L);
        DeltaCommandBundle bundle = new DeltaCommandBundle(baseTableInfo, command);

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> deltaExecutor.execute(newContext(mtmv), Collections.singletonList(bundle)));
        Assertions.assertTrue(ex.getMessage().contains("IVM delta execution failed"));
        Assertions.assertTrue(ex.getMessage().contains("something went wrong"));
    }

    @Test
    public void testExecuteMultipleBundlesStopsOnFirstFailure(@Mocked MTMV mtmv,
            @Mocked Command okCommand, @Mocked Command failCommand,
            @Mocked Command thirdCommand) throws Exception {
        ConnectContext mockCtx = new ConnectContext();
        new MockUp<MTMVPlanUtil>() {
            @Mock
            public ConnectContext createMTMVContext(MTMV mv, List<RuleType> disableRules) {
                return mockCtx;
            }
        };

        List<Integer> executionOrder = new ArrayList<>();
        new Expectations() {
            {
                okCommand.run((ConnectContext) any, (StmtExecutor) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void run(ConnectContext ctx, StmtExecutor executor) {
                        executionOrder.add(1);
                        ctx.getState().setOk();
                    }
                };
                failCommand.run((ConnectContext) any, (StmtExecutor) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
                        executionOrder.add(2);
                        throw new RuntimeException("second bundle failed");
                    }
                };
                thirdCommand.run((ConnectContext) any, (StmtExecutor) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void run(ConnectContext ctx, StmtExecutor executor) {
                        executionOrder.add(3);
                        ctx.getState().setOk();
                    }
                };
                minTimes = 0;
            }
        };

        List<DeltaCommandBundle> bundles = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(mtmv, 0L);
        bundles.add(new DeltaCommandBundle(baseTableInfo, okCommand));
        bundles.add(new DeltaCommandBundle(baseTableInfo, failCommand));
        bundles.add(new DeltaCommandBundle(baseTableInfo, thirdCommand));

        Assertions.assertThrows(AnalysisException.class,
                () -> deltaExecutor.execute(newContext(mtmv), bundles));
        Assertions.assertEquals(Arrays.asList(1, 2), executionOrder);
    }

    private static IVMRefreshContext newContext(MTMV mtmv) {
        return new IVMRefreshContext(mtmv, new ConnectContext(),
                new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
    }
}
