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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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
    public void lookupDoesNotLoadAndRespectsPlanningAndStatementCleanup() {
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        Assertions.assertNull(scope.getIfPresent("missing"));
        Object table = scope.computeIfAbsent("table", Object::new);
        Object planning = scope.computeIfAbsentForPlanning("planning", Object::new);
        Assertions.assertSame(table, scope.getIfPresent("table"));
        Assertions.assertSame(planning, scope.getIfPresent("planning"));
        scope.endPlanning();
        Assertions.assertNull(scope.getIfPresent("planning"));
        Assertions.assertSame(table, scope.getIfPresent("table"));
        scope.closeAll();
        Assertions.assertNull(scope.getIfPresent("table"));
        Assertions.assertNull(ConnectorStatementScope.NONE.getIfPresent("table"));
    }

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
    public void nonePlanningValuesAreNeverMemoized() {
        // Under NONE a value only planning reads is loaded on every call too, and ending planning has nothing to do.
        // MUTATION: NONE memoizing planning values -> one load -> red.
        AtomicInteger loads = new AtomicInteger();
        Supplier<Object> loader = () -> {
            loads.incrementAndGet();
            return new Object();
        };
        ConnectorStatementScope none = ConnectorStatementScope.NONE;

        Assertions.assertNotSame(none.computeIfAbsentForPlanning("k", loader),
                none.computeIfAbsentForPlanning("k", loader));
        none.endPlanning();
        Assertions.assertEquals(2, loads.get(), "NONE must run the loader every time (no memo)");
    }

    @Test
    public void endPlanningDropsOnlyThePlanningValues() throws Exception {
        // Identical scans share their planned ranges only within one plan, so a value only planning reads is dropped
        // when planning ends, while the values the statement keeps (its tables, metadata, transactions) stay until
        // closeAll. A later plan of the same statement registers the value again, and the end of that plan drops it.
        // MUTATION: endPlanning dropping nothing, dropping every value, or forgetting a value registered again -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger memoLoads = new AtomicInteger();
        Supplier<Object> memoLoader = () -> {
            memoLoads.incrementAndGet();
            return new Object();
        };
        Object memo = scope.computeIfAbsentForPlanning("memo", memoLoader);
        Assertions.assertSame(memo, scope.computeIfAbsentForPlanning("memo", memoLoader),
                "within one plan the value is shared");
        Object table = scope.computeIfAbsent("table", Object::new);
        AtomicInteger closes = new AtomicInteger();
        AutoCloseable metadata = () -> closes.incrementAndGet();
        scope.computeIfAbsent("metadata", () -> metadata);

        scope.endPlanning();

        Assertions.assertSame(table, scope.computeIfAbsent("table", Object::new), "statement values outlive planning");
        Object memoOfNextPlan = scope.computeIfAbsentForPlanning("memo", memoLoader);
        Assertions.assertNotSame(memo, memoOfNextPlan, "the next plan of the statement loads the value again");
        scope.endPlanning();
        Assertions.assertNotSame(memoOfNextPlan, scope.computeIfAbsentForPlanning("memo", memoLoader));
        Assertions.assertEquals(3, memoLoads.get());

        scope.closeAll();
        Assertions.assertEquals(1, closes.get(), "closeAll still closes the statement values");
    }

    @Test
    public void planningValueMustHoldNothingToClose() {
        // endPlanning drops a value without closing it, so a closeable one would leak. MUTATION: dropping the check
        // -> no exception -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AutoCloseable closeable = () -> { };

        Assertions.assertThrows(IllegalStateException.class,
                () -> scope.computeIfAbsentForPlanning("closeable", () -> closeable));
    }

    @Test
    public void releasingPlannerResourcesEndsThePlanningOfTheScope() {
        // NereidsPlanner.planWithLock releases the planner resources when planning is over, which ends connector
        // planning; a statement no connector has touched has no scope to end. MUTATION: releasePlannerResources not
        // ending connector planning -> red.
        StatementContext ctx = new StatementContext();
        ctx.releasePlannerResources();
        ConnectorStatementScope scope = ctx.getOrCreateConnectorStatementScope();
        Object memo = scope.computeIfAbsentForPlanning("memo", Object::new);
        Object table = scope.computeIfAbsent("table", Object::new);

        ctx.releasePlannerResources();

        Assertions.assertSame(scope, ctx.getOrCreateConnectorStatementScope(), "ending planning keeps the scope");
        Assertions.assertSame(table, scope.computeIfAbsent("table", Object::new));
        Assertions.assertNotSame(memo, scope.computeIfAbsentForPlanning("memo", Object::new));
    }

    @Test
    public void releasingPlannerResourcesEndsPlanningWhenAResourceFailsToClose() {
        // A table lock that fails to unlock still reaches the caller, after the planning values are gone.
        // MUTATION: ending connector planning only after the release succeeded -> the memo survives -> red.
        StatementContext ctx = new StatementContext();
        ConnectorStatementScope scope = ctx.getOrCreateConnectorStatementScope();
        Object memo = scope.computeIfAbsentForPlanning("memo", Object::new);
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("internal.db.t");
        IllegalStateException unlockFailure = new IllegalStateException("unlock failed");
        Mockito.doThrow(unlockFailure).when(table).readUnlock();
        ctx.getTables().put(Arrays.asList("internal", "db", "t"), table);
        ctx.lock();

        Assertions.assertSame(unlockFailure,
                Assertions.assertThrows(IllegalStateException.class, ctx::releasePlannerResources));
        Assertions.assertNotSame(memo, scope.computeIfAbsentForPlanning("memo", Object::new));
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
    public void closedScopeRefusesToLoadAndHandsOutNothing() {
        // The statement is over once closeAll() ran. A pump that outlives it (a split producer still listing
        // files: stopping it only flags it) must not load a value, such as a borrowed table, that nothing would
        // close any more. MUTATION: dropping the closed check in computeIfAbsent -> the loader runs -> red.
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        scope.computeIfAbsent("plain", Object::new);
        scope.closeAll();

        AtomicInteger loads = new AtomicInteger();
        Supplier<AutoCloseable> loader = () -> {
            loads.incrementAndGet();
            return () -> { };
        };
        Assertions.assertTrue(scope.isClosed());
        Assertions.assertThrows(CancellationException.class, () -> scope.computeIfAbsent("lease", loader));
        Assertions.assertThrows(CancellationException.class,
                () -> scope.computeIfAbsentForPlanning("memo", () -> {
                    loads.incrementAndGet();
                    return new Object();
                }));
        Assertions.assertEquals(0, loads.get(), "a closed scope never runs a loader");
        Assertions.assertNull(scope.getIfPresent("plain"), "a closed scope hands out nothing it held");
    }

    @Test
    public void valueLoadedWhileTheScopeClosesIsReleasedOnceByItsCaller() throws Exception {
        // closeAll() can run while a pump is inside a loader. The value is not in the scope yet, so closeAll cannot
        // release it; it used to land in the closed scope afterwards and never be closed. The caller that loaded it
        // releases it now and gets CancellationException instead of the value.
        // MUTATION: dropping the closed check after the load -> closes==0 and the caller gets the value -> red.
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        ExecutorService pump = Executors.newSingleThreadExecutor();
        try {
            Future<AutoCloseable> load = pump.submit(() -> scope.computeIfAbsent("lease", () -> {
                loading.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return (AutoCloseable) closes::incrementAndGet;
            }));
            Assertions.assertTrue(loading.await(5, TimeUnit.SECONDS));
            scope.closeAll();
            release.countDown();

            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> load.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, failure.getCause());
            Assertions.assertEquals(1, closes.get(), "the caller releases the value closeAll could not see");
            scope.closeAll();
            Assertions.assertEquals(1, closes.get(), "and nothing releases it a second time");
        } finally {
            release.countDown();
            pump.shutdownNow();
        }
    }

    @Test
    public void sessionBuiltAfterTheScopeClosesRunsAsOfflineWork() {
        // What still builds a session on a statement that has ended (the lineage of an INSERT, say) must neither
        // fail in nor memoize into the closed scope: it captures NONE and the direct scheduler, as offline work
        // does. A session captured before the close keeps the closed scope and is refused, so a split producer
        // holding it stops instead of borrowing a table nothing returns.
        // MUTATION: capturing the closed scope for a new session -> building it throws -> red.
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            StatementContext stmtCtx = new StatementContext();
            ctx.setStatementContext(stmtCtx);
            ConnectorSession before = ConnectorSessionBuilder.from(ctx).withCatalogId(1L).build();
            ConnectorStatementScope live = stmtCtx.getOrCreateConnectorStatementScope();
            Assertions.assertSame(live, before.getStatementScope());

            live.closeAll();

            ConnectorSession after = ConnectorSessionBuilder.from(ctx).withCatalogId(1L).build();
            Assertions.assertSame(ConnectorStatementScope.NONE, after.getStatementScope());
            Assertions.assertSame(ConnectorTaskScheduler.DIRECT,
                    after.getTaskScheduler(ConnectorTaskScheduler.Stage.SPLIT));
            AtomicInteger loads = new AtomicInteger();
            Assertions.assertThrows(CancellationException.class, () -> ConnectorStatementScopes.resolveInStatement(
                    before, "test_table", "db", "tbl", loads::incrementAndGet));
            Assertions.assertEquals(0, loads.get(), "a session of the ended statement loads nothing");
        } finally {
            ConnectContext.remove();
        }
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
