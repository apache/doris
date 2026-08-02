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

package org.apache.doris.datasource.plugin;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorWriteOps;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The per-statement owner of a plugin-driven write transaction, co-held on the statement scope next to the one
 * memoized {@link org.apache.doris.connector.api.ConnectorMetadata} the read and write arms share — mirroring
 * Trino's {@code CatalogTransaction}: one metadata instance and one transaction per (statement, catalog). The
 * insert executor opens the transaction through {@link #begin}, minting it from that shared write-ops facet so
 * the write inherits exactly the read arm's client/ops, and commits / rolls back through the
 * {@link PluginDrivenTransactionManager} as before.
 *
 * <p>This object's own job is the deterministic statement-end backstop. {@link #finalizeAtStatementEnd()} rolls
 * back a transaction the executor never committed or rolled back — only a mid-flight abort leaves one active —
 * and the scope runs it BEFORE closing the shared metadata, so a transaction is always finished before the
 * instance it was minted from is closed. It is idempotent and can never undo a committed write: on every normal
 * path the executor has already finished the transaction, which removes it from the manager, so the backstop
 * finds nothing active and does nothing.</p>
 *
 * <p>fe-core-internal and connector-agnostic: it traffics only in the neutral {@link ConnectorWriteOps} /
 * {@link ConnectorSession} / {@link ConnectorTransaction} SPI types (never a concrete connector type) and is
 * stored on the connector-agnostic {@link org.apache.doris.connector.api.ConnectorStatementScope} as an opaque
 * value, recognized by {@link org.apache.doris.connector.ConnectorStatementScopeImpl} only to order its
 * teardown before metadata close.</p>
 */
public final class CatalogStatementTransaction {

    private static final Logger LOG = LogManager.getLogger(CatalogStatementTransaction.class);

    /** Sentinel for "no transaction opened yet" (never a real connector txn id). */
    public static final long INVALID_TXN_ID = -1L;

    // The write facet of the statement's one shared metadata instance; the transaction is minted from it. Held
    // for co-hold coherence — the metadata itself is closed by the scope's generic pass, this class only
    // finalizes the transaction.
    private final ConnectorWriteOps writeOps;
    private final ConnectorSession session;
    private final PluginDrivenTransactionManager transactionManager;

    private ConnectorTransaction connectorTx;
    private long txnId = INVALID_TXN_ID;

    public CatalogStatementTransaction(ConnectorWriteOps writeOps, ConnectorSession session,
            PluginDrivenTransactionManager transactionManager) {
        this.writeOps = writeOps;
        this.session = session;
        this.transactionManager = transactionManager;
    }

    /**
     * Opens the write transaction from the shared metadata and registers it with the manager (which also
     * publishes it in the global registry the BE block-allocation RPC / commit-data feedback look up by id),
     * returning it so the executor can bind it onto the sink session. Called once, from the insert executor's
     * {@code beginTransaction}.
     */
    public ConnectorTransaction begin(ConnectorTableHandle writeHandle) {
        connectorTx = writeOps.beginTransaction(session, writeHandle);
        txnId = transactionManager.begin(connectorTx);
        return connectorTx;
    }

    public long getTransactionId() {
        return txnId;
    }

    public ConnectorTransaction getConnectorTransaction() {
        return connectorTx;
    }

    /**
     * Statement-end backstop: if the transaction is still active (the executor never reached commit / rollback,
     * i.e. the statement was aborted mid-flight), roll it back. On every normal path the executor already
     * finished it — the manager no longer holds it — so this is a no-op and can never undo a committed write.
     * The scope runs this before it closes the shared metadata instance.
     */
    public void finalizeAtStatementEnd() {
        if (txnId == INVALID_TXN_ID || !transactionManager.isActive(txnId)) {
            return;
        }
        LOG.warn("Statement ended with an uncommitted plugin-driven transaction {}; rolling it back as a backstop",
                txnId);
        transactionManager.rollback(txnId);
    }
}
