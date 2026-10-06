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

import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.datasource.split.SplitGenerator;
import org.apache.doris.datasource.split.SplitSourceManager;
import org.apache.doris.datasource.split.SplitToScanRange;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.qe.QueryState.MysqlStateType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A binary COM_STMT_EXECUTE never closes the StatementContext of an execution: the prepared statement keeps it until
 * its next execution, for the connector scope. What the execution's planning started is ended when the execution
 * ends all the same, and the context the prepared statement keeps holds on to none of the plan's split assignments -
 * through which it would keep the plan, and the splits no backend fetched, on the frontend's heap.
 */
public class MysqlConnectProcessorExecuteEndTest {

    @Test
    public void testAnExecutionLeavesNoSplitAssignmentToThePreparedStatement() throws Exception {
        SplitAssignment assignment = startableAssignment();
        AtomicReference<StatementContext> execution = new AtomicReference<>();

        PreparedStatementContext prepared = execute(() -> {
            // A batch scan of the plan started generating its splits while planned, and the coordinator dispatching
            // the plan took them over.
            execution.get().addSplitAssignmentStartedWhilePlanning(assignment);
            assignment.startWhilePlanning(60_000, null);
            assignment.start();
        }, execution);

        // The prepared statement keeps the context of the execution ...
        Assertions.assertSame(execution.get(), prepared.getStatementContext());
        // ... which offered the assignment a stop as the execution ended, left it to the coordinator that owns it ...
        Mockito.verify(assignment, Mockito.times(1)).stopIfNotDispatched();
        Assertions.assertFalse(assignment.isStop());
        // ... and no longer holds it.
        prepared.getStatementContext().stopUndispatchedSplitAssignments();
        Mockito.verify(assignment, Mockito.times(1)).stopIfNotDispatched();
        // The coordinator closes.
        assignment.stop();
    }

    @Test
    public void testAnExecutionRefusedBeforeDispatchStopsWhatItsPlanningStarted() throws Exception {
        SplitAssignment assignment = startableAssignment();
        AtomicReference<StatementContext> execution = new AtomicReference<>();

        PreparedStatementContext prepared = execute(() -> {
            execution.get().addSplitAssignmentStartedWhilePlanning(assignment);
            assignment.startWhilePlanning(60_000, null);
            throw new RuntimeException("refused by a SQL block rule after planning");
        }, execution);

        Assertions.assertEquals(MysqlStateType.ERR, prepared.ctx.getState().getStateType());
        // Stopped when the execution ended, not when the prepared statement next runs - if it ever does.
        Assertions.assertTrue(assignment.isStop());
        prepared.getStatementContext().stopUndispatchedSplitAssignments();
        Mockito.verify(assignment, Mockito.times(1)).stopIfNotDispatched();
    }

    private interface Planning {
        void run() throws Exception;
    }

    // Runs a COM_STMT_EXECUTE of a prepared statement whose execution, as ExecuteCommand#run does, moves the prepared
    // statement on to a fresh context and plans in it (planning), then returns the prepared statement.
    private static PreparedStatementContext execute(Planning planning, AtomicReference<StatementContext> execution)
            throws Exception {
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        Mockito.when(command.getOriginalStmt()).thenReturn(new OriginStatement("select * from t", 0));
        StatementContext prepare = new StatementContext(context, new OriginStatement("select * from t", 0));
        PreparedStatementContext prepared = new PreparedStatementContext(command, context, prepare, "select * from t");
        ByteBuffer packet = ByteBuffer.allocate(9);
        packet.position(packet.limit()); // COM_STMT_EXECUTE header consumed, no parameter payload.
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, construction) -> Mockito.doAnswer(invocation -> {
                    execution.set(prepared.nextStatementContext());
                    context.setStatementContext(execution.get());
                    planning.run();
                    return null;
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            new MysqlConnectProcessor(context).handleExecute(command, 7, prepared, packet, null);
            Assertions.assertEquals(1, executors.constructed().size());
        }
        return prepared;
    }

    // The split assignment of a batch scan that plans with its first split. Its generator has no split to wait for,
    // so starting it returns at once.
    private static SplitAssignment startableAssignment() {
        SplitAssignment assignment = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class),
                Mockito.mock(SplitGenerator.class), Mockito.mock(SplitToScanRange.class), new HashMap<>(),
                new ArrayList<>(), true, new SplitSourceManager());
        assignment.finishSchedule();
        return Mockito.spy(assignment);
    }
}
