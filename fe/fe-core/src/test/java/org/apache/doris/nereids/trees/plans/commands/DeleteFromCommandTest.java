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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class DeleteFromCommandTest {

    @Test
    public void testBuildDeleteFallbackExceptionPreservesBothFailureCauses() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception("initial predicate failure");
        Exception fallbackException = new Exception("fallback execution failure");

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify the merged exception surfaces the fallback failure and keeps the initial failure.
        Assertions.assertEquals(
                "Delete fallback execution failed: fallback execution failure"
                        + ". Initial predicate check failed: initial predicate failure",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testBuildDeleteFallbackExceptionFallsBackToThrowableToString() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception((String) null);
        Exception fallbackException = new Exception((String) null);

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify null messages still produce debuggable text.
        Assertions.assertEquals(
                "Delete fallback execution failed: java.lang.Exception"
                        + ". Initial predicate check failed: java.lang.Exception",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testThePlanOfADeleteIsStoppedAsSoonAsItIsPlanned() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("delete from t where k in (...)", 0)));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        // Planning has translated the plan: the scan of the table the predicate's subquery reads started generating
        // its splits. The statement is then refused, or deletes by predicate, or falls back to
        // DeleteFromUsingCommand, which plans it again: no coordinator ever takes this plan.
        ScanNode batchScan = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new org.apache.doris.common.AnalysisException("refused by a SQL block rule"))
                .when(executor).checkBlockRules();
        DeleteFromCommand command = new DeleteFromCommand(ImmutableList.of("internal", "db", "t"), null,
                false, Collections.emptyList(), Mockito.mock(LogicalPlan.class));

        try (MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class,
                (planner, construction) -> Mockito.when(planner.getScanNodes())
                        .thenReturn(Collections.singletonList(batchScan)))) {
            Exception e = Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                    () -> command.run(ctx, executor));
            Assertions.assertTrue(e.getMessage().contains("refused by a SQL block rule"), e.getMessage());
            Assertions.assertEquals(1, planners.constructed().size());
        }

        Mockito.verify(batchScan).stopUndispatched();
    }

    // Use reflection to validate the helper without exposing it only for tests.
    private AnalysisException invokeBuildDeleteFallbackException(DeleteFromCommand command,
            Exception initialException, Exception fallbackException)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = DeleteFromCommand.class.getDeclaredMethod("buildDeleteFallbackException",
                Exception.class, Exception.class);
        method.setAccessible(true);
        return (AnalysisException) method.invoke(command, initialException, fallbackException);
    }
}
