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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction manager for plugin-driven external catalogs.
 *
 * <p>This is a lightweight implementation that generates transaction IDs via
 * {@link Env#getNextId()} and tracks them in a local map. The actual commit
 * and rollback logic is handled by the connector's {@code ConnectorWriteOps}
 * through the insert executor — this manager simply provides the transaction
 * lifecycle bookkeeping required by {@link org.apache.doris.nereids.trees.plans
 * .commands.insert.BaseExternalTableInsertExecutor}.</p>
 */
public class PluginDrivenTransactionManager implements TransactionManager {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenTransactionManager.class);

    private final ConcurrentHashMap<Long, PluginDrivenTransaction> transactions =
            new ConcurrentHashMap<>();

    @Override
    public long begin() {
        long txnId = Env.getCurrentEnv().getNextId();
        PluginDrivenTransaction txn = new PluginDrivenTransaction(txnId);
        transactions.put(txnId, txn);
        LOG.debug("Plugin-driven transaction begun: {}", txnId);
        return txnId;
    }

    @Override
    public void commit(long id) throws UserException {
        PluginDrivenTransaction txn = transactions.remove(id);
        if (txn != null) {
            txn.commit();
            LOG.debug("Plugin-driven transaction committed: {}", id);
        }
    }

    @Override
    public void rollback(long id) {
        PluginDrivenTransaction txn = transactions.remove(id);
        if (txn != null) {
            txn.rollback();
            LOG.debug("Plugin-driven transaction rolled back: {}", id);
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
     * Simple transaction that tracks state. Actual connector-level commit/rollback
     * is performed by the insert executor via ConnectorWriteOps.
     */
    private static class PluginDrivenTransaction implements Transaction {
        private final long id;

        PluginDrivenTransaction(long id) {
            this.id = id;
        }

        @Override
        public void commit() {
            // No-op: actual commit is done via ConnectorWriteOps.finishInsert()
        }

        @Override
        public void rollback() {
            // No-op: actual rollback is done via ConnectorWriteOps.abortInsert()
        }
    }
}
