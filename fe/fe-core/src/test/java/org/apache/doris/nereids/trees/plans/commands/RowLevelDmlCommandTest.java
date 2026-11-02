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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Pins the begin→finalize guarded window of {@link RowLevelDmlCommand}: {@code beginTransaction} registers
 * the transaction with the transaction manager and the global external-transaction registry, and the
 * executor's own failure handling only takes over at {@code executeSingleInsert} — so any throw in between
 * must route through {@code onFail} (abort + registry cleanup), mirroring {@code InsertIntoTableCommand}'s
 * guarded prepare. Without the guard those registrations leak until FE restart.
 */
public class RowLevelDmlCommandTest {

    private final RowLevelDmlTransform transform = Mockito.mock(RowLevelDmlTransform.class);
    private final BaseExternalTableInsertExecutor executor = Mockito.mock(BaseExternalTableInsertExecutor.class);
    private final StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
    private final Plan analyzedPlan = Mockito.mock(Plan.class);
    private final TableIf table = Mockito.mock(TableIf.class);
    private final PlanFragment fragment = Mockito.mock(PlanFragment.class);
    private final DataSink dataSink = Mockito.mock(DataSink.class);
    private final PhysicalSink<?> physicalSink = Mockito.mock(PhysicalSink.class);

    private void invoke() {
        RowLevelDmlCommand.beginTransactionAndFinalizeSink(transform, RowLevelDmlOp.DELETE, executor,
                stmtExecutor, analyzedPlan, table, fragment, dataSink, physicalSink);
    }

    @Test
    public void finalizeSinkFailureRollsBackViaOnFail() {
        // finalizeSink throws AFTER beginTransaction registered the txn (the mid-window failure the guard
        // exists for). MUTATION: dropping the catch lets the exception propagate WITHOUT onFail -> the
        // verify below turns red.
        RuntimeException boom = new RuntimeException("finalize boom");
        Mockito.doThrow(boom).when(transform).finalizeSink(executor, RowLevelDmlOp.DELETE,
                fragment, dataSink, physicalSink);

        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, this::invoke);

        // A RuntimeException is rethrown as-is (not wrapped), mirroring InsertIntoTableCommand.
        Assertions.assertSame(boom, thrown);
        InOrder inOrder = Mockito.inOrder(executor);
        inOrder.verify(executor).beginTransaction();
        inOrder.verify(executor).onFail(boom);
    }

    @Test
    public void nonRuntimeFailureIsWrappedAndRolledBack() {
        // A non-RuntimeException throwable in the window takes the wrap branch: onFail still runs, and the
        // rethrow is IllegalStateException carrying the original as cause (the window's methods declare no
        // checked exceptions, so an Error stands in for the non-runtime lane).
        Error boom = new AssertionError("begin boom");
        Mockito.doThrow(boom).when(executor).beginTransaction();

        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class, this::invoke);

        Assertions.assertSame(boom, thrown.getCause());
        Mockito.verify(executor).onFail(boom);
        // beginTransaction itself failed -> nothing further in the window may run.
        Mockito.verify(transform, Mockito.never()).finalizeSink(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
    }

    @Test
    public void successPathPreparesCoordinatorWithoutRollback() {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(executor.getCoordinator()).thenReturn(coordinator);
        Mockito.when(executor.getTxnId()).thenReturn(42L);

        invoke();

        Mockito.verify(coordinator).setTxnId(42L);
        Mockito.verify(executor, Mockito.never()).onFail(Mockito.any());
    }

    @Test
    public void deleteCapturesThePreRewriteAnalyzedPlanLocally() {
        // NereidsPlanner leaves analyzedPlan null for a plain DELETE. Capture the plan for the metadata-delete
        // attempt only; publishing it through setAnalyzedPlan would also change row-level fallback semantics.
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);
        AtomicReference<Plan> captured = new AtomicReference<>();

        RowLevelDmlCommand.captureAnalyzedPlan(ctx, captured);

        ArgumentCaptor<PlannerHook> captor = ArgumentCaptor.forClass(PlannerHook.class);
        Mockito.verify(statementContext).addPlannerHook(captor.capture());
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Plan analyzed = Mockito.mock(Plan.class);
        Mockito.when(planner.getCascadesContext()).thenReturn(cascadesContext);
        Mockito.when(cascadesContext.getRewritePlan()).thenReturn(analyzed);

        captor.getValue().afterAnalyze(planner);

        Assertions.assertSame(analyzed, captured.get());
        Mockito.verify(planner, Mockito.never()).setAnalyzedPlan(Mockito.any());
    }

    @Test
    public void onlyADeleteMarksItsTargetAsWritingNoRowValues() {
        // Every row-level DML pins the target's writer schema, a DELETE for its delete sink. Only the DELETE writes
        // no row values, so only its target is marked and keeps DEFAULT(column) off the write-only defaults.
        ExternalTable target = Mockito.mock(ExternalTable.class);
        Mockito.when(target.getId()).thenReturn(7L);
        StatementContext statementContext = new StatementContext();
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);

        new RowLevelDmlCommand(transform, RowLevelDmlArgs.forUpdate(target, ImmutableList.of("t"), null,
                ImmutableList.of(), null), RowLevelDmlOp.UPDATE).getExplainPlan(ctx);
        Assertions.assertFalse(statementContext.isConnectorDeleteTarget(7L));

        new RowLevelDmlCommand(transform, RowLevelDmlArgs.forDelete(target, ImmutableList.of("t"), null, false,
                ImmutableList.of(), null), RowLevelDmlOp.DELETE).getExplainPlan(ctx);
        Assertions.assertTrue(statementContext.isConnectorDeleteTarget(7L));
    }

    @Test
    public void retainedCancellationIsReplayedBeforeTheMetadataDelete() {
        // The row-level path replays the retained cancellation when the coordinator is published. The metadata
        // branch has no coordinator, so it must replay it explicitly before any remote mutation.
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Mockito.when(stmtExecutor.getPendingCancelReason())
                .thenReturn(new Status(TStatusCode.CANCELLED, "query was killed"));

        UserException thrown = Assertions.assertThrows(UserException.class,
                () -> RowLevelDmlCommand.throwIfCancelled(stmtExecutor));

        Assertions.assertTrue(thrown.getMessage().contains("query was killed"), thrown.getMessage());
    }
}
