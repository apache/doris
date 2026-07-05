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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorWriteOps;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.NoOpConnectorTransaction;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.planner.PluginDrivenTableSink;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.transaction.PluginDrivenTransactionManager;
import org.apache.doris.transaction.TransactionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

/**
 * Ordering / routing tests for {@link PluginDrivenInsertExecutor}'s unified single-transaction
 * write lifecycle (P6.3-T01).
 *
 * <p>Every plugin-driven write now opens a {@link ConnectorTransaction} in
 * {@code beginTransaction} (registered globally), then {@code finalizeSink} binds it onto the
 * <em>sink's</em> session <em>before</em> {@code super.finalizeSink -> bindDataSink -> planWrite}
 * runs — because plan-provider {@code planWrite} reads {@code session.getCurrentTransaction()}.
 * Config-bag sinks (jdbc) carry no session, so the bind is skipped (no NPE).
 * {@code transactionType} is sourced from the transaction's {@code profileLabel}, and
 * {@code doBeforeCommit} backfills {@code loadedRows} from {@code getUpdateCnt()} only when the
 * transaction reports a real count (>= 0), preserving the coordinator counter otherwise.</p>
 *
 * <p>The 7-arg executor constructor builds a {@code Coordinator} via
 * {@code EnvFactory.createCoordinator}, which cannot be stood up in a unit test, so the
 * instance is created without invoking the constructor (Objenesis, via Mockito's
 * CALLS_REAL_METHODS) and the collaborator fields are injected directly; the real override
 * bodies then run against hand-written connector doubles. Only construction uses Mockito —
 * every assertion exercises real production code.</p>
 */
public class PluginDrivenInsertExecutorTest {

    @Test
    public void beginTransactionOpensConnectorTxnRegistersGloballyAndStampsTxnId() {
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        StubConnectorTransaction connectorTx = new StubConnectorTransaction(70001L);
        // Pre-seed the lazy setup so ensureConnectorSetup() short-circuits (no real catalog).
        Deencapsulation.setField(exec, "connectorSession", ConnectorSessionBuilder.create().build());
        Deencapsulation.setField(exec, "writeOps", new FakeTxnWriteOps(connectorTx));
        Deencapsulation.setField(exec, "transactionManager", new PluginDrivenTransactionManager());

        exec.beginTransaction();

        try {
            Assertions.assertSame(connectorTx, Deencapsulation.getField(exec, "connectorTx"),
                    "beginTransaction must open the connector transaction via writeOps");
            Assertions.assertEquals(70001L, exec.getTxnId(),
                    "the engine txn id must be the connector transaction's own id");
            Assertions.assertNotNull(
                    Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().getTxnById(70001L),
                    "the connector txn must be globally registered for the BE block-allocation / "
                            + "commit-data RPCs");
        } finally {
            Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().removeTxnById(70001L);
        }
    }

    @Test
    public void finalizeSinkBindsTransactionOntoSinkSessionBeforePlanWrite() {
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        StubConnectorTransaction connectorTx = new StubConnectorTransaction(70010L);
        Deencapsulation.setField(exec, "connectorTx", connectorTx);
        Deencapsulation.setField(exec, "insertCtx", Optional.empty());

        // The sink carries its own session (built at translate time); planWrite reads the txn off it.
        ConnectorSession sinkSession = ConnectorSessionBuilder.create().build();
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, sinkSession, new ConnectorTableHandle() { }, Collections.emptyList());

        exec.finalizeSink(null, sink, null);

        Assertions.assertNotNull(provider.txnSeenAtPlanWrite, "planWrite must have been reached");
        Assertions.assertTrue(provider.txnSeenAtPlanWrite.isPresent(),
                "the transaction must be bound onto the sink's session before planWrite runs, "
                        + "otherwise the plan-provider write plan fails loud");
        Assertions.assertSame(connectorTx, provider.txnSeenAtPlanWrite.get(),
                "planWrite must observe exactly the transaction beginTransaction opened");
    }

    @Test
    public void finalizeSinkSkipsBindForConfigBagSinkWithoutSession() throws Exception {
        // jdbc rides the config-bag sink, which is built without a connector session
        // (getConnectorSession() == null). After unification jdbc carries a non-null no-op
        // transaction, so finalizeSink must guard the bind on a non-null session — otherwise
        // setCurrentTransaction(...) NPEs. (Mutation: drop the null-guard -> this test NPEs.)
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Deencapsulation.setField(exec, "connectorTx", new NoOpConnectorTransaction(70060L, "JDBC"));
        Deencapsulation.setField(exec, "insertCtx", Optional.empty());

        // Mockito sink: getConnectorSession() returns null (config-bag), bindDataSink() is a no-op
        // so super.finalizeSink survives without a real fragment.
        PluginDrivenTableSink configBagSink = Mockito.mock(PluginDrivenTableSink.class);

        Assertions.assertDoesNotThrow(() -> exec.finalizeSink(null, configBagSink, null),
                "binding must be skipped for a config-bag sink with no connector session (no NPE)");
        // The bind-guard skips setCurrentTransaction (null session) but execution must still flow
        // into super.finalizeSink -> bindDataSink — proving the guard short-circuited only the
        // bind, not the whole sink build.
        Mockito.verify(configBagSink).bindDataSink(Mockito.any());
    }

    @Test
    public void beforeExecIsNoOpUnderSingleTransactionModel() throws UserException {
        // The write session is created by planWrite (in finalizeSink); beforeExec opens nothing.
        // writeOps deliberately left null: a clean return proves beforeExec touches no write SPI.
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70020L));

        exec.beforeExec();
    }

    @Test
    public void transactionTypeIsMappedFromConnectorProfileLabel() {
        // The connector tags its transaction with a profile label; the executor maps it to the
        // profiling TransactionType (jdbc -> JDBC, maxcompute -> MAXCOMPUTE, unknown -> UNKNOWN).
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();

        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70030L, 0L, "MAXCOMPUTE"));
        Assertions.assertEquals(TransactionType.MAXCOMPUTE, exec.transactionType(),
                "a transaction labelled MAXCOMPUTE must map to TransactionType.MAXCOMPUTE");

        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70031L, 0L, "JDBC"));
        Assertions.assertEquals(TransactionType.JDBC, exec.transactionType(),
                "a transaction labelled JDBC must map to TransactionType.JDBC");

        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70032L, 0L, "EXTERNAL"));
        Assertions.assertEquals(TransactionType.UNKNOWN, exec.transactionType(),
                "an unrecognized label must fall back to TransactionType.UNKNOWN");
    }

    @Test
    public void transactionTypeIsUnknownWhenNoTransaction() {
        // empty-insert skips beginTransaction; transactionType must not NPE on a null transaction.
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Assertions.assertEquals(TransactionType.UNKNOWN, exec.transactionType(),
                "with no connector transaction (empty insert), transactionType is UNKNOWN");
    }

    @Test
    public void doBeforeCommitBackfillsLoadedRowsWhenTransactionReportsCount() throws UserException {
        // Transaction model with a real row count (e.g. maxcompute): BE reports rows only through
        // the connector transaction's commit-data, so doBeforeCommit must backfill loadedRows from
        // getUpdateCnt() -- otherwise affected-rows is reported as 0.
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70040L, 42L));

        exec.doBeforeCommit();

        Long loadedRows = Deencapsulation.getField(exec, "loadedRows");
        Assertions.assertEquals(42L, loadedRows.longValue(),
                "doBeforeCommit must set loadedRows = connectorTx.getUpdateCnt() when it is >= 0");
    }

    @Test
    public void doBeforeCommitKeepsCoordinatorRowCountWhenTransactionReportsNoCount() throws UserException {
        // A no-op transaction (jdbc) reports getUpdateCnt() == -1 ("no count from the transaction").
        // loadedRows was already set from the coordinator's DPP_NORMAL_ALL counter; doBeforeCommit
        // must NOT clobber it with the sentinel -- otherwise affected-rows regresses to 0/-1.
        // (Mutation: drop the `if (cnt >= 0)` guard -> loadedRows becomes -1 and this test fails.)
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Deencapsulation.setField(exec, "loadedRows", 7L);
        Deencapsulation.setField(exec, "connectorTx", new NoOpConnectorTransaction(70050L, "JDBC"));

        exec.doBeforeCommit();

        Long loadedRows = Deencapsulation.getField(exec, "loadedRows");
        Assertions.assertEquals(7L, loadedRows.longValue(),
                "a -1 (no count) transaction must leave the coordinator-counted loadedRows untouched");
    }

    /**
     * Creates a {@link PluginDrivenInsertExecutor} without running its constructor. See the class
     * javadoc: the constructor builds a Coordinator that needs a live planner/EnvFactory.
     */
    private static PluginDrivenInsertExecutor newUnconstructedExecutor() {
        return Mockito.mock(PluginDrivenInsertExecutor.class, Mockito.CALLS_REAL_METHODS);
    }

    /** Write ops that hand back a fixed connector transaction from beginTransaction. */
    private static final class FakeTxnWriteOps implements ConnectorWriteOps {
        private final ConnectorTransaction txn;

        private FakeTxnWriteOps(ConnectorTransaction txn) {
            this.txn = txn;
        }

        @Override
        public ConnectorTransaction beginTransaction(ConnectorSession session) {
            return txn;
        }
    }

    /** Captures the transaction visible on the session at the moment planWrite is invoked. */
    private static final class RecordingWritePlanProvider implements ConnectorWritePlanProvider {
        private Optional<ConnectorTransaction> txnSeenAtPlanWrite;

        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            this.txnSeenAtPlanWrite = session.getCurrentTransaction();
            return new ConnectorSinkPlan(new TDataSink());
        }
    }

    /** Minimal hand-written {@link ConnectorTransaction}: identity, row count, profile label. */
    private static final class StubConnectorTransaction implements ConnectorTransaction {
        private final long txnId;
        private final long updateCnt;
        private final String profileLabel;

        private StubConnectorTransaction(long txnId) {
            this(txnId, 0L, "MAXCOMPUTE");
        }

        private StubConnectorTransaction(long txnId, long updateCnt) {
            this(txnId, updateCnt, "MAXCOMPUTE");
        }

        private StubConnectorTransaction(long txnId, long updateCnt, String profileLabel) {
            this.txnId = txnId;
            this.updateCnt = updateCnt;
            this.profileLabel = profileLabel;
        }

        @Override
        public long getTransactionId() {
            return txnId;
        }

        @Override
        public long getUpdateCnt() {
            return updateCnt;
        }

        @Override
        public String profileLabel() {
            return profileLabel;
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }
    }
}
