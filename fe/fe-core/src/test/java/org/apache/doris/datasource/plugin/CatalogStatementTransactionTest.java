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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Pins {@link CatalogStatementTransaction}, the per-statement owner of a plugin-driven write transaction:
 * {@link CatalogStatementTransaction#begin} mints the transaction from the statement's shared write-ops and
 * registers it with the manager, and the statement-end backstop
 * ({@link CatalogStatementTransaction#finalizeAtStatementEnd}) rolls back only a genuinely orphaned
 * transaction (mid-flight abort) and NEVER undoes one the executor already committed or rolled back.
 */
public class CatalogStatementTransactionTest {

    private static ConnectorTransaction tx(long id) {
        ConnectorTransaction tx = Mockito.mock(ConnectorTransaction.class);
        Mockito.when(tx.getTransactionId()).thenReturn(id);
        return tx;
    }

    private static ConnectorWriteOps writeOpsReturning(ConnectorTransaction tx) {
        ConnectorWriteOps ops = Mockito.mock(ConnectorWriteOps.class);
        Mockito.when(ops.beginTransaction(Mockito.any(), Mockito.any())).thenReturn(tx);
        return ops;
    }

    private static CatalogStatementTransaction holder(ConnectorTransaction tx, PluginDrivenTransactionManager mgr) {
        return new CatalogStatementTransaction(writeOpsReturning(tx), Mockito.mock(ConnectorSession.class), mgr);
    }

    @Test
    public void beginMintsFromWriteOpsAndRegistersWithManager() {
        // begin() opens the transaction from the statement's ONE shared metadata (writeOps) and registers it, so
        // the write inherits the read arm's client/ops and the BE RPC can look it up by id. MUTATION: not
        // registering -> isActive false -> red.
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        ConnectorTransaction tx = tx(80001L);
        CatalogStatementTransaction holder = holder(tx, mgr);

        ConnectorTransaction opened = holder.begin(new ConnectorTableHandle() { });

        Assertions.assertSame(tx, opened, "begin returns the transaction minted from the shared write-ops");
        Assertions.assertEquals(80001L, holder.getTransactionId(), "the holder stamps the connector txn id");
        Assertions.assertTrue(mgr.isActive(80001L), "the transaction is registered active with the manager");
    }

    @Test
    public void finalizeRollsBackAnOrphanedTransaction() {
        // A statement aborted mid-flight leaves the transaction active (the executor reached neither commit nor
        // rollback). The statement-end backstop rolls it back. MUTATION: skipping the rollback -> the orphan
        // leaks its resources -> this verify fails.
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        ConnectorTransaction tx = tx(80002L);
        CatalogStatementTransaction holder = holder(tx, mgr);
        holder.begin(new ConnectorTableHandle() { });

        holder.finalizeAtStatementEnd();

        Mockito.verify(tx).rollback();
        Assertions.assertFalse(mgr.isActive(80002L), "the orphan is deregistered after the backstop rollback");
    }

    @Test
    public void finalizeNeverUndoesACommittedTransaction() throws Exception {
        // THE safety property: on the normal path the executor commits (removing the txn from the manager), so
        // the statement-end backstop must find nothing active and NOT roll back -- otherwise it would undo a
        // durably committed write. MUTATION: dropping the isActive guard -> finalize rolls back a committed txn.
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        ConnectorTransaction tx = tx(80003L);
        CatalogStatementTransaction holder = holder(tx, mgr);
        holder.begin(new ConnectorTableHandle() { });
        mgr.commit(80003L);   // the executor's onComplete path finished it

        holder.finalizeAtStatementEnd();

        Mockito.verify(tx).commit();
        Mockito.verify(tx, Mockito.never()).rollback();
    }

    @Test
    public void finalizeIsANoOpAfterRollback() throws Exception {
        // The executor's onFail already rolled back; the backstop must be idempotent -- roll back once, not
        // twice. MUTATION: dropping the isActive guard -> a second rollback -> red.
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        ConnectorTransaction tx = tx(80004L);
        CatalogStatementTransaction holder = holder(tx, mgr);
        holder.begin(new ConnectorTableHandle() { });
        mgr.rollback(80004L);   // the executor's onFail path finished it

        holder.finalizeAtStatementEnd();

        Mockito.verify(tx, Mockito.times(1)).rollback();
    }

    @Test
    public void finalizeIsANoOpWhenNoTransactionWasEverOpened() {
        // The empty-insert path never calls begin(); a stray finalize must not touch the manager or NPE.
        PluginDrivenTransactionManager mgr = new PluginDrivenTransactionManager();
        CatalogStatementTransaction holder = holder(tx(80005L), mgr);

        Assertions.assertEquals(CatalogStatementTransaction.INVALID_TXN_ID, holder.getTransactionId());
        Assertions.assertDoesNotThrow(holder::finalizeAtStatementEnd);
    }
}
