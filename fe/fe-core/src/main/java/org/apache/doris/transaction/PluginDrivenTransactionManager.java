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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction manager for plugin-driven external catalogs.
 *
 * <p>The insert executor opens every plugin-driven write through
 * {@link #begin(ConnectorTransaction)}: connectors return a real {@link ConnectorTransaction}
 * from {@code ConnectorWriteOps.beginTransaction} (a degenerate no-op one for writes that BE
 * auto-commits, such as jdbc). The manager uses {@link ConnectorTransaction#getTransactionId()}
 * as the txn id, registers it globally, and delegates commit/rollback/close to the connector.</p>
 *
 * <p>{@link #begin()} (no-arg) remains only to satisfy the {@link TransactionManager} interface;
 * it allocates a txn id via {@link Env#getNextId()} and stores a marker transaction with no
 * connector delegate. Both paths share the {@link #commit(long)} / {@link #rollback(long)}
 * surface required by {@link TransactionManager}.</p>
 */
public class PluginDrivenTransactionManager implements TransactionManager {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenTransactionManager.class);

    private final ConcurrentHashMap<Long, PluginDrivenTransaction> transactions =
            new ConcurrentHashMap<>();

    @Override
    public long begin() {
        long txnId = Env.getCurrentEnv().getNextId();
        transactions.put(txnId, new PluginDrivenTransaction(txnId, null));
        LOG.debug("Plugin-driven transaction begun: {}", txnId);
        return txnId;
    }

    /**
     * Registers a connector-provided {@link ConnectorTransaction}. Commit / rollback
     * lifecycle is delegated to it (including {@code close()}).
     *
     * @return the txn id, taken from {@code connectorTx.getTransactionId()}
     */
    public long begin(ConnectorTransaction connectorTx) {
        Objects.requireNonNull(connectorTx, "connectorTx");
        long txnId = connectorTx.getTransactionId();
        PluginDrivenTransaction txn = new PluginDrivenTransaction(txnId, connectorTx);
        transactions.put(txnId, txn);
        // Register globally so the BE block-allocation RPC and the commit-data feedback can
        // look the transaction up by id (FrontendServiceImpl.getMaxComputeBlockIdRange ->
        // getTxnById). Mirrors AbstractExternalTransactionManager.begin. Connectors whose writes
        // BE auto-commits (jdbc) register a no-op transaction here too; BE never sends them commit
        // fragments, so the global entry is simply never looked up before it is removed on commit.
        Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().putTxnById(txnId, txn);
        LOG.debug("Plugin-driven transaction begun with SPI ConnectorTransaction: {}", txnId);
        return txnId;
    }

    @Override
    public void commit(long id) throws UserException {
        PluginDrivenTransaction txn = transactions.remove(id);
        try {
            if (txn != null) {
                txn.commit();
                LOG.debug("Plugin-driven transaction committed: {}", id);
            }
        } finally {
            // Always deregister from the global registry, even if connectorTx.commit() throws,
            // so a failed commit cannot leave a stale entry behind (mirrors rollback()).
            Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().removeTxnById(id);
        }
    }

    @Override
    public void rollback(long id) {
        PluginDrivenTransaction txn = transactions.remove(id);
        try {
            if (txn != null) {
                txn.rollback();
                LOG.debug("Plugin-driven transaction rolled back: {}", id);
            }
        } finally {
            Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().removeTxnById(id);
        }
    }

    @Override
    public Transaction getTransaction(long id) throws UserException {
        PluginDrivenTransaction txn = transactions.get(id);
        if (txn == null) {
            throw new UserException("Plugin-driven transaction not found: " + id);
        }
        return txn;
    }

    /**
     * Internal transaction record. When {@code connectorTx} is non-null (every plugin-driven
     * write) the SPI is the source of truth and commit/rollback delegate to it; close() always
     * runs after delegation. {@code connectorTx} is null only for the no-arg {@link #begin()}
     * interface-contract path, where this is an inert no-op marker.
     */
    private static final class PluginDrivenTransaction implements Transaction {
        private final long id;
        private final ConnectorTransaction connectorTx;

        PluginDrivenTransaction(long id, ConnectorTransaction connectorTx) {
            this.id = id;
            this.connectorTx = connectorTx;
        }

        @Override
        public void commit() {
            if (connectorTx == null) {
                return;
            }
            try {
                connectorTx.commit();
            } finally {
                closeQuietly();
            }
        }

        @Override
        public void rollback() {
            if (connectorTx == null) {
                return;
            }
            try {
                connectorTx.rollback();
            } finally {
                closeQuietly();
            }
        }

        @Override
        public void addCommitData(byte[] commitFragment) {
            if (connectorTx != null) {
                connectorTx.addCommitData(commitFragment);
            }
            // legacy no-op marker: nothing to accumulate
        }

        @Override
        public boolean supportsWriteBlockAllocation() {
            return connectorTx != null && connectorTx.supportsWriteBlockAllocation();
        }

        @Override
        public long allocateWriteBlockRange(String writeSessionId, long count) throws UserException {
            if (connectorTx == null) {
                throw new UnsupportedOperationException("write block allocation not supported");
            }
            return connectorTx.allocateWriteBlockRange(writeSessionId, count);
        }

        @Override
        public long getUpdateCnt() {
            return connectorTx == null ? 0 : connectorTx.getUpdateCnt();
        }

        private void closeQuietly() {
            try {
                connectorTx.close();
            } catch (Exception e) {
                LOG.warn("Failed to close ConnectorTransaction {}: {}", id, e.getMessage());
            }
        }
    }
}
