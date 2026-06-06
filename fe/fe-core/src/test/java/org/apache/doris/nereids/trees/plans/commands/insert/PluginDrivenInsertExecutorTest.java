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
 * Ordering / routing tests for {@link PluginDrivenInsertExecutor}'s SPI transaction-model
 * path (P4-T06a W-c / gap G2).
 *
 * <p>The four overrides encode the cutover's critical write-lifecycle constraint
 * (design §1.2): {@code beginTransaction} opens the connector transaction and registers
 * it globally, then {@code finalizeSink} must bind that transaction onto the <em>sink's</em>
 * session <em>before</em> {@code super.finalizeSink -> bindDataSink -> planWrite} runs —
 * because {@code planWrite} reads {@code session.getCurrentTransaction()} and fails loud if
 * it is absent. {@code beforeExec} must skip the JDBC handle-model path, and
 * {@code transactionType} reports MAXCOMPUTE.</p>
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
                            + "commit-data RPCs (W-d)");
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
                        + "otherwise the maxcompute write plan fails loud");
        Assertions.assertSame(connectorTx, provider.txnSeenAtPlanWrite.get(),
                "planWrite must observe exactly the transaction beginTransaction opened");
    }

    @Test
    public void beforeExecSkipsHandleModelForTransactionModel() throws UserException {
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        // connectorTx set, writeOps deliberately left null: the JDBC handle-model branch would
        // dereference writeOps, so a clean return proves the transaction-model early-out is taken.
        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70020L));

        exec.beforeExec();
    }

    @Test
    public void transactionTypeIsMaxComputeForTransactionModel() {
        PluginDrivenInsertExecutor exec = newUnconstructedExecutor();
        Deencapsulation.setField(exec, "connectorTx", new StubConnectorTransaction(70030L));

        Assertions.assertEquals(TransactionType.MAXCOMPUTE, exec.transactionType(),
                "the transaction-model executor reports MAXCOMPUTE (profiling-only; sole adopter)");
    }

    /**
     * Creates a {@link PluginDrivenInsertExecutor} without running its constructor. See the class
     * javadoc: the constructor builds a Coordinator that needs a live planner/EnvFactory.
     */
    private static PluginDrivenInsertExecutor newUnconstructedExecutor() {
        return Mockito.mock(PluginDrivenInsertExecutor.class, Mockito.CALLS_REAL_METHODS);
    }

    /** Write ops that route through the SPI transaction model and hand back a fixed transaction. */
    private static final class FakeTxnWriteOps implements ConnectorWriteOps {
        private final ConnectorTransaction txn;

        private FakeTxnWriteOps(ConnectorTransaction txn) {
            this.txn = txn;
        }

        @Override
        public boolean usesConnectorTransaction() {
            return true;
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

    /** Minimal hand-written {@link ConnectorTransaction}; only identity matters here. */
    private static final class StubConnectorTransaction implements ConnectorTransaction {
        private final long txnId;

        private StubConnectorTransaction(long txnId) {
            this.txnId = txnId;
        }

        @Override
        public long getTransactionId() {
            return txnId;
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
