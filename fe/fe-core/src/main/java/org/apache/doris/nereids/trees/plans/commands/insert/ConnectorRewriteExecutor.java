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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.TransactionType;

import java.util.Optional;

/**
 * Rewrite executor for plugin-driven connector data file rewrite (compaction) operations — the neutral
 * counterpart of {@link IcebergRewriteExecutor}.
 *
 * <p>Like the iceberg one, the per-group INSERT-SELECT transaction is NOT managed here: the distributed
 * rewrite coordinator opens a single connector transaction and holds it across the N per-group writes,
 * binding it onto each group's sink session and committing once at the end. So {@code beforeExec} and
 * {@code doBeforeCommit} are no-ops, and the transaction is keyed neutrally (not
 * {@link TransactionType#ICEBERG}).</p>
 */
public class ConnectorRewriteExecutor extends BaseExternalTableInsertExecutor {

    /**
     * constructor
     */
    public ConnectorRewriteExecutor(ConnectContext ctx, ExternalTable table,
            String labelName, NereidsPlanner planner,
            Optional<InsertCommandContext> insertCtx,
            boolean emptyInsert, long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    protected void beforeExec() throws UserException {
        // do nothing, the transaction is held by the rewrite coordinator, not by ConnectorRewriteExecutor
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        // do nothing, the transaction is held by the rewrite coordinator, not by ConnectorRewriteExecutor
    }

    @Override
    protected TransactionType transactionType() {
        // Neutral key: the connector tags its own transaction with a profile label; rewrite opens no
        // executor-owned transaction here, so report UNKNOWN (mirrors PluginDrivenInsertExecutor's
        // no-transaction fallback) rather than the iceberg-specific TransactionType.ICEBERG.
        return TransactionType.UNKNOWN;
    }
}
