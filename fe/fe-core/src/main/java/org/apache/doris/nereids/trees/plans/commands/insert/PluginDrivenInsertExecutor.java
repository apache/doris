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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorWriteOps;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PluginDrivenTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.transaction.PluginDrivenTransactionManager;
import org.apache.doris.transaction.TransactionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Insert executor for plugin-driven connector catalogs.
 *
 * <p>Delegates the write lifecycle to the connector's {@link ConnectorWriteOps} SPI through a
 * single transaction model: {@link #beginTransaction()} opens a {@link ConnectorTransaction}
 * and registers it globally; {@link #finalizeSink} binds it onto the sink's session so the
 * connector's {@code planWrite} sees it; BE feeds commit fragments back through the transaction;
 * {@code onComplete} commits / {@code onFail} rolls back via the transaction manager. Connectors
 * whose writes are auto-committed by BE (e.g. jdbc) return a degenerate no-op transaction.</p>
 */
public class PluginDrivenInsertExecutor extends BaseExternalTableInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenInsertExecutor.class);

    private transient ConnectorSession connectorSession;
    private transient ConnectorWriteOps writeOps;
    // The connector transaction for this write: opened in beginTransaction(), bound onto the
    // sink's session in finalizeSink(), and committed / rolled back via the transaction manager
    // in onComplete() / onFail(). Null only on the empty-insert path, which skips beginTransaction.
    private transient ConnectorTransaction connectorTx;

    /**
     * constructor
     */
    public PluginDrivenInsertExecutor(ConnectContext ctx, ExternalTable table,
                                      String labelName, NereidsPlanner planner,
                                      Optional<InsertCommandContext> insertCtx,
                                      boolean emptyInsert, long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    public void beginTransaction() {
        ensureConnectorSetup();
        // Single transaction model: every plugin-driven write opens a ConnectorTransaction and
        // registers it globally, so the BE block-allocation RPC and commit-data feedback can look
        // it up by id. Connectors whose writes are auto-committed by BE (jdbc) return a degenerate
        // no-op transaction; maxcompute returns a real one. The connector-specific write session is
        // created later by planWrite (reached through finalizeSink -> bindDataSink).
        connectorTx = writeOps.beginTransaction(connectorSession);
        txnId = ((PluginDrivenTransactionManager) transactionManager).begin(connectorTx);
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        // Bind the connector transaction onto the SINK's session BEFORE super.finalizeSink ->
        // bindDataSink -> planWrite, which reads it via ConnectorSession.getCurrentTransaction().
        // Only plan-provider sinks (e.g. maxcompute) carry a connector session; config-bag sinks
        // (jdbc) have none and build their TDataSink without the transaction, so skip binding for
        // them (getConnectorSession() == null) — otherwise this would NPE.
        if (connectorTx != null && sink instanceof PluginDrivenTableSink) {
            ConnectorSession sinkSession = ((PluginDrivenTableSink) sink).getConnectorSession();
            if (sinkSession != null) {
                sinkSession.setCurrentTransaction(connectorTx);
            }
        }
        super.finalizeSink(fragment, sink, physicalSink);
    }

    @Override
    protected void beforeExec() throws UserException {
        // Single transaction model: the connector write session is created by planWrite
        // (in finalizeSink). There is no per-statement handle to open here.
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        if (connectorTx != null) {
            // BE reports the affected-row count either through the connector transaction's
            // commit-data (e.g. maxcompute TMCCommitData.row_count) or through the coordinator's
            // DPP_NORMAL_ALL load counter (e.g. jdbc). getUpdateCnt() == -1 means "no count from
            // the transaction; keep the coordinator counter" (NoOpConnectorTransaction); a value
            // >= 0 is authoritative and backfills loadedRows, which AbstractInsertExecutor otherwise
            // leaves at 0 for the transaction model. Mirrors legacy MCInsertExecutor.doBeforeCommit.
            long cnt = connectorTx.getUpdateCnt();
            if (cnt >= 0) {
                loadedRows = cnt;
            }
        }
    }

    /**
     * Post-commit refresh is best-effort for ALL connector write paths.
     *
     * <p>By the time this runs, the remote write is already durably committed and FE cannot roll
     * it back: for jdbc the BE commits directly via PreparedStatement; for the connector-transaction
     * path (maxcompute) the write session is committed by the transaction manager in onComplete,
     * before this step. {@code super.doAfterCommit()} only refreshes FE-side metadata cache and
     * writes an external-table refresh edit log (a cache-invalidation hint to followers); it never
     * touches the already-committed remote data.</p>
     *
     * <p>If that refresh fails (e.g., catalog dropped concurrently, edit log I/O error), reporting
     * the INSERT as failed would mislead the user into retrying and writing duplicate data. The
     * worst case of swallowing is transient cache staleness, which self-heals on the next refresh /
     * TTL. This intentionally diverges from legacy MCInsertExecutor (see deviations-log DV-018),
     * preserving the safer swallow-and-warn behavior of the old JdbcInsertExecutor.</p>
     */
    @Override
    protected void doAfterCommit() throws DdlException {
        try {
            super.doAfterCommit();
        } catch (Exception e) {
            LOG.warn("Post-commit cache refresh failed for table {}. "
                    + "Data was committed successfully; cache may be stale until next refresh.",
                    table.getName(), e);
        }
    }

    @Override
    protected TransactionType transactionType() {
        if (connectorTx == null) {
            // empty-insert path skips beginTransaction; no transaction was opened.
            return TransactionType.UNKNOWN;
        }
        // The connector tags its transaction with a profile label (e.g. "JDBC" / "MAXCOMPUTE");
        // map it to the profiling TransactionType. Unknown labels fall back to UNKNOWN.
        String label = connectorTx.profileLabel();
        for (TransactionType type : TransactionType.values()) {
            if (type.name().equals(label)) {
                return type;
            }
        }
        return TransactionType.UNKNOWN;
    }

    /**
     * Lazily builds the connector session and write-ops handle for this insert. Called from
     * {@link #beginTransaction()} before opening the connector transaction. Idempotent.
     */
    private void ensureConnectorSetup() {
        if (connectorSession != null) {
            return;
        }
        PluginDrivenExternalCatalog catalog =
                (PluginDrivenExternalCatalog) ((ExternalTable) table).getCatalog();
        Connector connector = catalog.getConnector();
        connectorSession = catalog.buildConnectorSession();
        writeOps = connector.getMetadata(connectorSession);
    }
}
