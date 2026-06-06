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

import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;

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

    /** Hand-written {@link ConnectorTransaction} test double recording delegated calls. */
    private static final class RecordingConnectorTransaction implements ConnectorTransaction {
        private final long txnId;
        private final List<byte[]> commitFragments = new ArrayList<>();
        private boolean supportsBlockAllocation;
        private long blockRangeStart;
        private String lastWriteSessionId;
        private long lastCount;
        private long updateCnt;

        private RecordingConnectorTransaction(long txnId) {
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

        @Override
        public void addCommitData(byte[] commitFragment) {
            commitFragments.add(commitFragment);
        }

        @Override
        public boolean supportsWriteBlockAllocation() {
            return supportsBlockAllocation;
        }

        @Override
        public long allocateWriteBlockRange(String writeSessionId, long count) {
            this.lastWriteSessionId = writeSessionId;
            this.lastCount = count;
            return blockRangeStart;
        }

        @Override
        public long getUpdateCnt() {
            return updateCnt;
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
    public void supportsWriteBlockAllocationIsDelegated() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingConnectorTransaction connectorTx = new RecordingConnectorTransaction(778L);
        connectorTx.supportsBlockAllocation = true;
        long txnId = manager.begin(connectorTx);

        Assert.assertTrue(manager.getTransaction(txnId).supportsWriteBlockAllocation());
    }

    @Test
    public void allocateWriteBlockRangeIsDelegated() throws UserException {
        PluginDrivenTransactionManager manager = new PluginDrivenTransactionManager();
        RecordingConnectorTransaction connectorTx = new RecordingConnectorTransaction(779L);
        connectorTx.blockRangeStart = 100L;
        long txnId = manager.begin(connectorTx);

        long start = manager.getTransaction(txnId).allocateWriteBlockRange("write-session-x", 5L);

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

        // The legacy no-op marker (null connector transaction) must stay inert,
        // matching the interface defaults: addCommitData is a silent no-op,
        // block allocation is unsupported, and the update count is zero.
        txn.addCommitData(new byte[] {9});
        Assert.assertFalse(txn.supportsWriteBlockAllocation());
        Assert.assertEquals(0L, txn.getUpdateCnt());
        try {
            txn.allocateWriteBlockRange("none", 1L);
            Assert.fail("expected UnsupportedOperationException for the legacy marker");
        } catch (UnsupportedOperationException expected) {
            // legacy marker does not support write block allocation
        }
    }
}
