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

package org.apache.doris.connector;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorWriteOps;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.datasource.plugin.CatalogStatementTransaction;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.ExecuteCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the per-statement {@link ConnectorStatementScope}: the {@link ConnectorStatementScope#NONE} no-op,
 * the memoizing {@link ConnectorStatementScopeImpl}, the {@link StatementContext} hosting + per-execution
 * reset primitive, and that {@link ExecuteCommand} allocates a fresh StatementContext (and therefore a fresh
 * connector scope) on every execution (external/connector tables are planned through the context, so a missing
 * fresh context would leak one execution's loaded table into the next — see
 * {@code executeCommandAllocatesFreshConnectorScopePerExecution}).
 */
public class ConnectorStatementScopeTest {

    @Test
    public void noneNeverMemoizes() {
        // NONE = the off-context default (offline / no live statement): the loader runs every call, so a
        // connector under NONE behaves byte-identically to loading every time. MUTATION: NONE memoizing -> the
        // second call reuses the first value -> red.
        AtomicInteger loads = new AtomicInteger();
        ConnectorStatementScope none = ConnectorStatementScope.NONE;
        Object first = none.computeIfAbsent("k", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Object second = none.computeIfAbsent("k", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertEquals(2, loads.get(), "NONE must run the loader every time (no memo)");
        Assertions.assertNotSame(first, second, "NONE returns a fresh value each call");
    }

    @Test
    public void implMemoizesPerKeyAndIsolatesKeys() {
        // The real scope memoizes per key: the same key returns the same instance to every caller (this is what
        // makes a statement's read/scan/write resolvers share ONE loaded table), and the loader runs once.
        // MUTATION: not memoizing -> two instances / two loads -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger loads = new AtomicInteger();
        Object firstA = scope.computeIfAbsent("A", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Object secondA = scope.computeIfAbsent("A", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertSame(firstA, secondA, "the same key returns the same instance (read/write share one load)");
        Assertions.assertEquals(1, loads.get(), "the loader runs once per key");

        Object b = scope.computeIfAbsent("B", Object::new);
        Assertions.assertNotSame(firstA, b, "different keys are isolated (cross-catalog / cross-table)");
    }

    @Test
    public void statementContextScopeIsStableThenResetsForReusedPreparedContext() {
        // StatementContext lazily builds one scope and reuses it across the statement (getOrCreate...). A prepared
        // EXECUTE reuses one StatementContext across executions; resetConnectorStatementScope() (called by
        // ExecuteCommand each execution) drops it so a prior execution's memoized state never leaks into the next.
        // MUTATION: reset not clearing the field -> s2 == s1 and the stale value leaks -> red.
        StatementContext ctx = new StatementContext();
        ConnectorStatementScope s1 = ctx.getOrCreateConnectorStatementScope();
        Assertions.assertSame(s1, ctx.getOrCreateConnectorStatementScope(),
                "the scope is lazily created once and reused across a statement");

        Object memoized = s1.computeIfAbsent("k", Object::new);
        ctx.resetConnectorStatementScope();

        ConnectorStatementScope s2 = ctx.getOrCreateConnectorStatementScope();
        Assertions.assertNotSame(s1, s2, "reset drops the scope so a reused prepared context starts fresh");
        Assertions.assertNotSame(memoized, s2.computeIfAbsent("k", Object::new),
                "a prior execution's memoized value must not leak into the next execution");
    }

    @Test
    public void closeAllClosesCloseableValuesOnceAndIgnoresPlainValues() throws Exception {
        // closeAll() closes every AutoCloseable value the statement memoized (a ConnectorMetadata is Closeable)
        // and leaves non-closeable values (the shared table object, the scan->write delete-supply map) untouched.
        // It must be idempotent: the engine fires it from more than one locus (the query-finish callback + a reused
        // prepared statement's per-execution reset), so a second call must not double-close.
        // MUTATION: dropping the close-once guard -> closes==2 -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable closeable = () -> closes.incrementAndGet();
        scope.computeIfAbsent("closeable", () -> closeable);
        scope.computeIfAbsent("plain", Object::new); // a non-closeable value must be ignored, not crash

        scope.closeAll();
        scope.closeAll();

        Assertions.assertEquals(1, closes.get(),
                "each closeable value is closed exactly once across repeated closeAll (idempotent)");
    }

    @Test
    public void closeAllFinalizesTransactionsBeforeClosingMetadata() {
        // Two-pass teardown: the scope must finalize (roll back an orphaned) write transaction BEFORE it closes
        // the shared metadata instance the transaction was minted from, so the transaction is never left holding a
        // closed instance. MUTATION: single-pass close (or closing metadata first) -> the recorded order flips
        // -> red.
        List<String> order = new ArrayList<>();
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        ConnectorTransaction tx = Mockito.mock(ConnectorTransaction.class);
        Mockito.when(tx.getTransactionId()).thenReturn(90001L);
        Mockito.doAnswer(inv -> {
            order.add("txn-rollback");
            return null;
        }).when(tx).rollback();
        ConnectorWriteOps ops = Mockito.mock(ConnectorWriteOps.class);
        Mockito.when(ops.beginTransaction(Mockito.any(), Mockito.any())).thenReturn(tx);

        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        // The statement's shared metadata: a closeable that records the moment it is closed.
        AutoCloseable metadata = () -> order.add("metadata-close");
        scope.computeIfAbsent("metadata:1", () -> metadata);
        CatalogStatementTransaction holder =
                new CatalogStatementTransaction(ops, Mockito.mock(ConnectorSession.class), mgr);
        scope.computeIfAbsent("txn:1", () -> holder);
        holder.begin(new ConnectorTableHandle() { }); // active orphan: the executor never committed it

        scope.closeAll();

        Assertions.assertEquals(Arrays.asList("txn-rollback", "metadata-close"), order,
                "the transaction is finalized before the shared metadata is closed");
    }

    @Test
    public void resetClosesTheDroppedScopeBeforeStartingFresh() {
        // A prepared EXECUTE / retry reuses one StatementContext and calls resetConnectorStatementScope() at the
        // start of each execution/attempt. Reset must CLOSE the outgoing scope's closeable values (else the prior
        // execution's tables/FileIO leak once close() does real work) and then drop it so the next starts fresh.
        // MUTATION: reset only nulling (not closing) -> closes==0 -> red.
        StatementContext ctx = new StatementContext();
        ConnectorStatementScope s1 = ctx.getOrCreateConnectorStatementScope();
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable closeable = () -> closes.incrementAndGet();
        s1.computeIfAbsent("k", () -> closeable);

        ctx.resetConnectorStatementScope();

        Assertions.assertEquals(1, closes.get(), "reset closes the dropped scope's closeable values before dropping it");
        Assertions.assertNotSame(s1, ctx.getOrCreateConnectorStatementScope(),
                "reset drops the scope so the next execution/attempt starts fresh");
    }

    @Test
    public void executeCommandAllocatesFreshConnectorScopePerExecution() throws Exception {
        // WIRING test: a prepared statement lives as long as its connection, so ExecuteCommand must not let one
        // execution's connector state leak into the next. Previously run() reused ONE StatementContext and reset
        // its connector per-statement scope at the top of every execution; now it allocates a brand-new
        // StatementContext per EXECUTE (dropping the previous one, whose per-statement maps were the OOM source
        // on long-lived connections), so the fresh context starts with a brand-new connector scope by
        // construction. External/connector tables never take the OLAP short-circuit fast path; they always fall
        // to the normal executor.execute() planner, which re-resolves the table per execution through that fresh
        // context. MUTATION: reverting to reusing one StatementContext across executions (without a reset) -> the
        // seeded value survives in the context still used by the next execution -> the assertNotSame below flips
        // -> red.
        StatementContext statementContext = new StatementContext();
        // Seed values the way a first execution's connector planning would (one loaded table the statement
        // shares, plus closeable metadata that must be finalized when the context is dropped).
        ConnectorStatementScope firstScope = statementContext.getOrCreateConnectorStatementScope();
        Object memoizedTable = firstScope.computeIfAbsent("table:1", Object::new);
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable closeable = () -> closes.incrementAndGet();
        firstScope.computeIfAbsent("closeable:1", () -> closeable);

        // A prepared statement wrapping that StatementContext, with a plain (non-cache, non-insert) plan.
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        PrepareCommand prepareCommand = Mockito.mock(PrepareCommand.class);
        Mockito.when(prepareCommand.getLogicalPlan()).thenReturn(plan);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        PreparedStatementContext preparedStmtCtx = new PreparedStatementContext(
                prepareCommand, ctx, statementContext, "SELECT * FROM ext_catalog.db.t WHERE id = ?");
        Mockito.when(ctx.getPreparedStementContext("s")).thenReturn(preparedStmtCtx);
        // Force the plain-SELECT path: skip the OLAP group-commit fast path so run() falls to executor.execute().
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableGroupCommitFullPrepare = false;
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(ctx.getStatementContext()).thenReturn(statementContext);

        // A no-op executor: we pin the fresh-context wiring, not the planner. execute() is a mock no-op.
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getContext()).thenReturn(ctx);

        new ExecuteCommand("s", prepareCommand, statementContext).run(ctx, executor);

        // run() must swap in a fresh StatementContext for this execution, releasing the previous one...
        StatementContext nextContext = preparedStmtCtx.getStatementContext();
        Assertions.assertNotSame(statementContext, nextContext,
                "ExecuteCommand allocates a fresh StatementContext per EXECUTE so the old one is released");
        // ...finalizing the outgoing scope's closeable connector values (COM_STMT_EXECUTE has no per-statement
        // StatementContext.close() finally, so dropping the context must close the scope, or closeable metadata
        // and active connector transactions would be abandoned and never finalized by GC).
        Assertions.assertEquals(1, closes.get(),
                "dropping the outgoing context finalizes its closeable connector values");
        // ...and whose connector scope is brand-new, so a prior execution's memoized connector table cannot leak.
        ConnectorStatementScope secondScope = nextContext.getOrCreateConnectorStatementScope();
        Assertions.assertNotSame(firstScope, secondScope,
                "the fresh context starts with a fresh connector scope");
        Assertions.assertNotSame(memoizedTable, secondScope.computeIfAbsent("table:1", Object::new),
                "a prior execution's memoized connector table must not leak into the next EXECUTE");
    }

    @Test
    public void closeClosesScopeWhenReturningResultLocally() {
        // A statement that never reaches the query-finish callback (external DDL / SHOW via Command.run) is
        // closed by StatementContext.close(), the fallback ConnectProcessor runs in its per-statement finally.
        // It fires only when the result is produced locally. MUTATION: close() not closing the scope -> red.
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.isReturnResultFromLocal()).thenReturn(true);
        StatementContext ctx = new StatementContext(connectContext, null);
        ConnectorStatementScope scope = ctx.getOrCreateConnectorStatementScope();
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable closeable = () -> closes.incrementAndGet();
        scope.computeIfAbsent("k", () -> closeable);

        ctx.close();

        Assertions.assertEquals(1, closes.get(), "close() closes the scope for a locally-returned statement");
    }

    @Test
    public void closeDefersScopeCloseForAsyncResult() {
        // An arrow-flight statement returns results asynchronously (isReturnResultFromLocal == false) and defers
        // scope cleanup to its own query-finish close; StatementContext.close() must NOT close it early here, or an
        // in-flight fetch would touch a closed scope. MUTATION: dropping the guard -> closes==1 -> red.
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.isReturnResultFromLocal()).thenReturn(false);
        StatementContext ctx = new StatementContext(connectContext, null);
        ConnectorStatementScope scope = ctx.getOrCreateConnectorStatementScope();
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable closeable = () -> closes.incrementAndGet();
        scope.computeIfAbsent("k", () -> closeable);

        ctx.close();

        Assertions.assertEquals(0, closes.get(),
                "close() defers to the query-finish callback for async (arrow-flight) results");
    }
}
