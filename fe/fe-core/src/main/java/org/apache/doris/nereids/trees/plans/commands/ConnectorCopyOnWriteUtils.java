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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.repository.ResultRow;

import java.util.List;
import java.util.stream.Collectors;

/** Safety checks shared by connector copy-on-write DML commands. */
final class ConnectorCopyOnWriteUtils {
    private ConnectorCopyOnWriteUtils() {
    }

    /** Pins both the target handle and the numeric version used by the separate affected-row query. */
    static CopyOnWriteSnapshot captureSnapshot(PluginDrivenExternalTable table, String operation) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        Connector connector = catalog.getConnector();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(connector.getClass().getClassLoader());
            ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
            ConnectorTableHandle handle = table.resolveConnectorTableHandle(session, metadata)
                    .orElseThrow(() -> new AnalysisException("Table not found while planning " + operation + ": "
                            + table.getRemoteDbName() + "." + table.getRemoteName()));
            ConnectorMvccSnapshot snapshot = metadata.beginQuerySnapshot(session, handle)
                    .filter(pin -> pin.getSnapshotId() >= 0)
                    .orElseThrow(() -> new AnalysisException("Connector " + operation
                            + " requires a numeric snapshot version for affected-row counting"));
            return new CopyOnWriteSnapshot(metadata.applySnapshot(session, handle, snapshot), snapshot.getSnapshotId());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    static final class CopyOnWriteSnapshot {
        final ConnectorTableHandle handle;
        final long version;

        private CopyOnWriteSnapshot(ConnectorTableHandle handle, long version) {
            this.handle = handle;
            this.version = version;
        }
    }

    static void requireUnrestrictedSource(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        if (ctx.getCurrentUserIdentity().isRootUser()
                || ctx.getCurrentUserIdentity().isAdminUser()) {
            return;
        }
        AccessControllerManager accessManager = ctx.getEnv().getAccessManager();
        String catalogName = table.getDatabase().getCatalog().getName();
        String databaseName = table.getDatabase().getFullName();
        String tableName = table.getName();
        if (!accessManager.evalRowFilterPolicies(ctx.getCurrentUserIdentity(),
                catalogName, databaseName, tableName).isEmpty()) {
            throw new AnalysisException(
                    "Connector copy-on-write DML does not support row filter policies");
        }
        boolean hasDataMask = table.getBaseSchema(true).stream().anyMatch(column ->
                accessManager.evalDataMaskPolicy(ctx.getCurrentUserIdentity(), catalogName,
                        databaseName, tableName, column.getName()).isPresent());
        if (hasDataMask) {
            throw new AnalysisException(
                    "Connector copy-on-write DML does not support data masking policies");
        }
    }

    static String quoteQualifiedName(List<String> nameParts) {
        return nameParts.stream()
                .map(SqlUtils::getIdentSql).collect(Collectors.joining("."));
    }

    static String quoteIdentifier(String name) {
        return SqlUtils.getIdentSql(name);
    }

    static long countRows(ConnectContext ctx, String sql, String operation) {
        try (AutoCloseConnectContext internalContext =
                new AutoCloseConnectContext(ctx.cloneContext())) {
            List<ResultRow> rows = new StmtExecutor(
                    internalContext.connectContext, sql).executeInternalQuery();
            if (rows.size() != 1 || rows.get(0).getValues().size() != 1) {
                throw new AnalysisException(
                        "Connector " + operation + " affected-row query returned an invalid result");
            }
            return Long.parseLong(rows.get(0).get(0));
        } catch (NumberFormatException e) {
            throw new AnalysisException(
                    "Connector " + operation + " affected-row query returned a non-numeric result", e);
        }
    }
}
