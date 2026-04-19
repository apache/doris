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
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IvmDeltaExecutorTest {

    private IvmDeltaExecutor deltaExecutor;

    @BeforeEach
    public void setUp() {
        deltaExecutor = new IvmDeltaExecutor();
    }

    @Test
    public void testExecuteEmptyBundles() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        IvmRefreshContext context = newContext(mtmv);
        // Should complete without error when bundle list is empty — no static mocking needed.
        deltaExecutor.execute(context, Collections.emptyList(), 0);
    }

    @Test
    public void testExecuteSingleBundleSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Command command = Mockito.mock(Command.class);
        StmtExecutor mockExecutor = Mockito.mock(StmtExecutor.class);

        List<Boolean> runCalled = new ArrayList<>();

        try (MockedStatic<MTMVPlanUtil> planUtilMock = Mockito.mockStatic(MTMVPlanUtil.class)) {
            planUtilMock.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmv),
                    Mockito.eq(command),
                    Mockito.any(StatementContext.class),
                    Mockito.any(),
                    Mockito.anyBoolean()
            )).thenAnswer(inv -> {
                runCalled.add(true);
                return mockExecutor;
            });

            deltaExecutor.execute(newContext(mtmv), Collections.singletonList(command), 0);
        }

        Assertions.assertEquals(1, runCalled.size());
    }

    @Test
    public void testExecuteCommandRunThrowsException() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Command command = Mockito.mock(Command.class);

        try (MockedStatic<MTMVPlanUtil> planUtilMock = Mockito.mockStatic(MTMVPlanUtil.class)) {
            planUtilMock.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmv),
                    Mockito.eq(command),
                    Mockito.any(StatementContext.class),
                    Mockito.any(),
                    Mockito.anyBoolean()
            )).thenThrow(new RuntimeException("command run failed"));

            AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                    () -> deltaExecutor.execute(newContext(mtmv), Collections.singletonList(command), 0));
            Assertions.assertTrue(ex.getMessage().contains("IVM delta execution failed"));
            Assertions.assertTrue(ex.getMessage().contains("command run failed"));
        }
    }

    @Test
    public void testExecuteMultipleBundlesStopsOnFirstFailure() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Command okCommand = Mockito.mock(Command.class);
        Command failCommand = Mockito.mock(Command.class);
        Command thirdCommand = Mockito.mock(Command.class);
        StmtExecutor mockExecutor = Mockito.mock(StmtExecutor.class);

        List<Integer> executionOrder = new ArrayList<>();

        try (MockedStatic<MTMVPlanUtil> planUtilMock = Mockito.mockStatic(MTMVPlanUtil.class)) {
            planUtilMock.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmv),
                    Mockito.eq(okCommand),
                    Mockito.any(StatementContext.class),
                    Mockito.any(),
                    Mockito.anyBoolean()
            )).thenAnswer(inv -> {
                executionOrder.add(1);
                return mockExecutor;
            });
            planUtilMock.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmv),
                    Mockito.eq(failCommand),
                    Mockito.any(StatementContext.class),
                    Mockito.any(),
                    Mockito.anyBoolean()
            )).thenAnswer(inv -> {
                executionOrder.add(2);
                throw new RuntimeException("second bundle failed");
            });
            planUtilMock.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmv),
                    Mockito.eq(thirdCommand),
                    Mockito.any(StatementContext.class),
                    Mockito.any(),
                    Mockito.anyBoolean()
            )).thenAnswer(inv -> {
                executionOrder.add(3);
                return mockExecutor;
            });

            List<Command> commands = new ArrayList<>();
            commands.add(okCommand);
            commands.add(failCommand);
            commands.add(thirdCommand);

            Assertions.assertThrows(AnalysisException.class,
                    () -> deltaExecutor.execute(newContext(mtmv), commands, 0));
            Assertions.assertEquals(Arrays.asList(1, 2), executionOrder);
        }
    }

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext(),
                new MTMVRefreshContext(mtmv));
    }
}
