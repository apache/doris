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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorWriteOps;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.datasource.ConnectorColumnConverter;
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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Insert executor for plugin-driven connector catalogs.
 * Delegates the write lifecycle to the connector's ConnectorWriteOps SPI.
 */
public class PluginDrivenInsertExecutor extends BaseExternalTableInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenInsertExecutor.class);

    private transient ConnectorInsertHandle insertHandle;
    private transient ConnectorSession connectorSession;
    private transient ConnectorWriteOps writeOps;
    private transient ConnectorWriteType resolvedWriteType;
    // Non-null only for the SPI transaction model (e.g. maxcompute): opened in beginTransaction(),
    // bound onto the sink's session in finalizeSink(), and committed via the transaction manager
    // in onComplete(). Null for the JDBC / auto-commit insert-handle path.
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
        if (writeOps.usesConnectorTransaction()) {
            // SPI transaction model (e.g. maxcompute): open a connector transaction and let the
            // plugin-driven transaction manager register it globally, so the BE block-allocation
            // RPC and commit-data feedback can look it up by id. The ODPS write session that backs
            // it is created later by planWrite (reached through finalizeSink -> bindDataSink).
            connectorTx = writeOps.beginTransaction(connectorSession);
            txnId = ((PluginDrivenTransactionManager) transactionManager).begin(connectorTx);
        } else {
            // JDBC / auto-commit handle model: allocate a no-op engine txn id.
            super.beginTransaction();
        }
    }

    @Override
    protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        // Transaction model: bind the connector transaction onto the SINK's session BEFORE
        // super.finalizeSink -> bindDataSink -> planWrite, which reads it via
        // ConnectorSession.getCurrentTransaction() (fail-loud if absent). The sink carries its own
        // ConnectorSession built at translate time; the txn is shared with it by reference.
        if (connectorTx != null && sink instanceof PluginDrivenTableSink) {
            ((PluginDrivenTableSink) sink).getConnectorSession().setCurrentTransaction(connectorTx);
        }
        super.finalizeSink(fragment, sink, physicalSink);
    }

    @Override
    protected void beforeExec() throws UserException {
        if (connectorTx != null) {
            // Transaction model: the write session was already created by planWrite (in
            // finalizeSink). There is no per-statement insert handle to open here.
            return;
        }
        // JDBC / auto-commit handle model.
        ensureConnectorSetup();
        if (!writeOps.supportsInsert()) {
            throw new UserException("Connector does not support INSERT for table: "
                    + table.getName());
        }

        // Get table handle using remote names (not local/mapped names)
        ExternalTable extTable = (ExternalTable) table;
        String remoteDbName = extTable.getRemoteDbName();
        String remoteTableName = extTable.getRemoteName();
        Optional<ConnectorTableHandle> tableHandle = ((ConnectorMetadata) writeOps).getTableHandle(
                connectorSession, remoteDbName, remoteTableName);
        if (!tableHandle.isPresent()) {
            throw new UserException("Table not found via connector: "
                    + remoteDbName + "." + remoteTableName);
        }

        // Convert Doris columns to connector columns
        List<ConnectorColumn> columns = toConnectorColumns(extTable.getBaseSchema(true));

        // Resolve write type for transaction type decision
        resolvedWriteType = writeOps.getWriteConfig(
                connectorSession, tableHandle.get(), columns).getWriteType();

        // Begin insert
        insertHandle = writeOps.beginInsert(connectorSession, tableHandle.get(), columns);
        LOG.info("Plugin-driven insert started for table {}.{}, txnId={}",
                remoteDbName, remoteTableName, txnId);
    }

    @Override
    protected void doBeforeCommit() throws UserException {
        if (writeOps != null && insertHandle != null) {
            writeOps.finishInsert(connectorSession, insertHandle, Collections.emptyList());
        }
    }

    /**
     * Post-commit refresh is best-effort for connector writes.
     *
     * <p>For JDBC_WRITE, the remote write is committed directly by BE via
     * PreparedStatement — FE cannot roll it back. If the post-commit cache
     * refresh fails (e.g., catalog dropped concurrently, edit log I/O error),
     * reporting the INSERT as failed would mislead the user into retrying,
     * causing duplicate data. The old JdbcInsertExecutor avoided this by
     * not performing any post-commit work at all.</p>
     *
     * <p>We preserve that safety guarantee while still attempting the refresh
     * so that cache stays fresh in the common case.</p>
     */
    @Override
    protected void doAfterCommit() throws DdlException {
        try {
            super.doAfterCommit();
        } catch (Exception e) {
            LOG.warn("Post-commit cache refresh failed for table {} (write type: {}). "
                    + "Data was committed successfully; cache may be stale until next refresh.",
                    table.getName(), resolvedWriteType, e);
        }
    }

    @Override
    protected void onFail(Throwable t) {
        // Abort the connector-level write before the Doris-level transaction rollback
        if (writeOps != null && insertHandle != null) {
            try {
                writeOps.abortInsert(connectorSession, insertHandle);
            } catch (Exception e) {
                LOG.warn("Failed to abort connector insert for table {}: {}",
                        table.getName(), e.getMessage(), e);
            }
        }
        super.onFail(t);
    }

    @Override
    protected TransactionType transactionType() {
        if (connectorTx != null) {
            // SPI transaction model. maxcompute is currently the sole adopter; this value is
            // profiling-only. Revisit when a second transaction-model connector arrives.
            return TransactionType.MAXCOMPUTE;
        }
        if (resolvedWriteType == ConnectorWriteType.JDBC_WRITE) {
            return TransactionType.JDBC;
        }
        return TransactionType.HMS;
    }

    /**
     * Lazily builds the connector session and write-ops handle for this insert. Idempotent so
     * both {@link #beginTransaction()} and {@link #beforeExec()} can call it: the empty-insert
     * path skips beginTransaction, so beforeExec must still be able to set up on its own.
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

    /**
     * Converts a list of Doris {@link Column} to a list of {@link ConnectorColumn}.
     * This is the reverse of {@link org.apache.doris.datasource.ConnectorColumnConverter#convertColumns}.
     */
    private static List<ConnectorColumn> toConnectorColumns(List<Column> dorisColumns) {
        return dorisColumns.stream()
                .map(PluginDrivenInsertExecutor::toConnectorColumn)
                .collect(Collectors.toList());
    }

    private static ConnectorColumn toConnectorColumn(Column col) {
        return ConnectorColumnConverter.toConnectorColumn(col);
    }
}
