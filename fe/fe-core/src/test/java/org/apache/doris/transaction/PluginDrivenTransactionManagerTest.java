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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteBlockAllocatingConnectorTransaction;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Delegation tests for {@link PluginDrivenTransactionManager} and its internal
 * {@code PluginDrivenTransaction} bridge (W-phase W4).
 *
 * <p>When a connector supplies a real SPI {@link ConnectorTransaction}, the
 * fe-core {@link Transaction} write callbacks ({@code addCommitData} /
 * {@code supportsWriteBlockAllocation} / {@code allocateWriteBlockRange} /
 * {@code getUpdateCnt}) must delegate to it, so that the generic write
 * orchestration (which after W3 only sees the polymorphic {@link Transaction})
 * reaches the connector. The legacy no-op marker (no connector transaction)
 * must keep inheriting the inert interface defaults.</p>
 */
public class PluginDrivenTransactionManagerTest {

    /** Hand-written {@link ConnectorTransaction} test double recording delegated calls (no write-block). */
    private static class RecordingConnectorTransaction implements ConnectorTransaction {
        private final long txnId;
        private final List<byte[]> commitFragments = new ArrayList<>();
        private long updateCnt;
        private boolean failOnCommit;

        private RecordingConnectorTransaction(long txnId) {
            this.txnId = txnId;
        }

        @Override
        public long getTransactionId() {
            return txnId;
        }

        @Override
        public void commit() {
            if (failOnCommit) {
                throw new RuntimeException("connector commit failed");
            }
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }

        @Override
        public void addCommitData(byte[] commitFragment) {
            commitFragments.add(commitFragment);
        }

        @Override
        public long getUpdateCnt() {
            return updateCnt;
        }
    }

    /**
     * A write-block-capable double: it additionally implements the narrow
     * {@link WriteBlockAllocatingConnectorTransaction}, so the manager wraps it in the write-block subclass
     * and {@code getTransaction(id)} is a {@link WriteBlockAllocatingTransaction}.
     */
    private static final class RecordingWriteBlockConnectorTransaction extends RecordingConnectorTransaction
            implements WriteBlockAllocatingConnectorTransaction {
        private long blockRangeStart;
        private String lastWriteSessionId;
        private long lastCount;

        private RecordingWriteBlockConnectorTransaction(long txnId) {
            super(txnId);
        }

        @Override
        public long allocateWriteBlockRange(String writeSessionId, long count) {
            this.lastWriteSessionId = writeSessionId;
            this.lastCount = count;
            return blockRangeStart;
        }
    }

    @Test
    public void addCommitDataIsDelegatedToConnectorTransaction() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingConnectorTransaction connectorTx = new RecordingConnectorTransaction(777L);
        long txnId = manager.begin(connectorTx);

        byte[] fragment = {1, 2, 3};
        manager.getTransaction(txnId).addCommitData(fragment);

        Assert.assertEquals(1, connectorTx.commitFragments.size());
        Assert.assertSame(fragment, connectorTx.commitFragments.get(0));
    }

    @Test
    public void writeBlockCapableConnectorYieldsWriteBlockTransaction() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingWriteBlockConnectorTransaction connectorTx =
                new RecordingWriteBlockConnectorTransaction(778L);
        long txnId = manager.begin(connectorTx);

        // The narrow capability is exposed as a TYPE (instanceof gate), not a supports*() runtime flag.
        Assert.assertTrue(manager.getTransaction(txnId) instanceof WriteBlockAllocatingTransaction);
    }

    @Test
    public void plainConnectorIsNotAWriteBlockTransaction() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        long txnId = manager.begin(new RecordingConnectorTransaction(781L));

        // A connector without the narrow capability must NOT wrap into a WriteBlockAllocatingTransaction,
        // so the write-block RPC handler's instanceof gate rejects it.
        Assert.assertFalse(manager.getTransaction(txnId) instanceof WriteBlockAllocatingTransaction);
    }

    @Test
    public void allocateWriteBlockRangeIsDelegated() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingWriteBlockConnectorTransaction connectorTx =
                new RecordingWriteBlockConnectorTransaction(779L);
        connectorTx.blockRangeStart = 100L;
        long txnId = manager.begin(connectorTx);

        Transaction txn = manager.getTransaction(txnId);
        Assert.assertTrue(txn instanceof WriteBlockAllocatingTransaction);
        long start = ((WriteBlockAllocatingTransaction) txn).allocateWriteBlockRange("write-session-x", 5L);

        Assert.assertEquals(100L, start);
        Assert.assertEquals("write-session-x", connectorTx.lastWriteSessionId);
        Assert.assertEquals(5L, connectorTx.lastCount);
    }

    @Test
    public void getUpdateCntIsDelegated() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingConnectorTransaction connectorTx = new RecordingConnectorTransaction(780L);
        connectorTx.updateCnt = 42L;
        long txnId = manager.begin(connectorTx);

        Assert.assertEquals(42L, manager.getTransaction(txnId).getUpdateCnt());
    }

    @Test
    public void legacyMarkerKeepsInertWriteDefaults() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        long txnId = manager.begin();
        Transaction txn = manager.getTransaction(txnId);

        // The legacy no-op marker (null connector transaction) must stay inert: addCommitData is a silent
        // no-op, the update count is zero, and it does not carry the write-block capability (so the RPC
        // handler's instanceof gate rejects it).
        txn.addCommitData(new byte[] {9});
        Assert.assertEquals(0L, txn.getUpdateCnt());
        Assert.assertFalse(txn instanceof WriteBlockAllocatingTransaction);
    }

    // ──────────── global registration (P4-T06a W-d / gap G3) ────────────
    //
    // begin(ConnectorTransaction) must also register the txn in the process-wide
    // GlobalExternalTransactionInfoMgr, because the BE block-allocation RPC and the
    // commit-data feedback look it up there by id (FrontendServiceImpl
    // .getMaxComputeBlockIdRange -> getTxnById). Without it those callbacks throw
    // "Can't find txn". commit/rollback must deregister so the registry cannot leak.
    // (Distinct ids 90001+ avoid colliding with the delegation tests above, which
    // intentionally never commit and therefore leave their ids registered.)

    @Test
    public void beginRegistersConnectorTransactionInGlobalRegistry() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        long txnId = manager.begin(new RecordingConnectorTransaction(90001L));
        try {
            Transaction registered =
                    Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().getTxnById(txnId);
            Assert.assertSame("global registry must hold the same wrapped transaction the "
                    + "manager hands out", manager.getTransaction(txnId), registered);
        } finally {
            // do not leak the id into the shared global registry
            manager.commit(txnId);
        }
    }

    @Test
    public void commitDeregistersFromGlobalRegistry() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        long txnId = manager.begin(new RecordingConnectorTransaction(90002L));

        manager.commit(txnId);

        assertNotRegistered(txnId);
    }

    @Test
    public void rollbackDeregistersFromGlobalRegistry() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        long txnId = manager.begin(new RecordingConnectorTransaction(90003L));

        manager.rollback(txnId);

        assertNotRegistered(txnId);
    }

    @Test
    public void commitStillDeregistersWhenConnectorCommitThrows() {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingConnectorTransaction connectorTx = new RecordingConnectorTransaction(90004L);
        connectorTx.failOnCommit = true;
        long txnId = manager.begin(connectorTx);

        try {
            manager.commit(txnId);
            Assert.fail("commit must propagate the connector failure");
        } catch (Exception expected) {
            // the connector's commit failure propagates to the caller
        }

        // commit() wraps deregistration in try/finally, so a failed connector commit must
        // not leave a stale entry behind (mirrors rollback()).
        assertNotRegistered(txnId);
    }

    private static void assertNotRegistered(long txnId) {
        try {
            Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().getTxnById(txnId);
            Assert.fail("txn " + txnId + " should have been deregistered from the global registry");
        } catch (RuntimeException expected) {
            // getTxnById throws "Can't find txn for <id>" once the entry is gone
        }
    }
}
