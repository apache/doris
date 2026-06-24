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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.datasource.ConnectorColumnConverter;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Engine-side {@link ExecuteAction} adapter that routes {@code ALTER TABLE t EXECUTE proc(...)} on a
 * {@link PluginDrivenExternalTable} to the connector's {@link ConnectorProcedureOps} (P6.4-T07).
 *
 * <p>The procedure-side analogue of the connector scan/write dispatch: it threads the catalog's
 * {@link ConnectorSession} and the resolved {@link ConnectorTableHandle} into
 * {@code getProcedureOps().execute(...)} (mirroring {@code PhysicalPlanTranslator.visitPhysicalConnectorTableSink}),
 * then wraps the engine-neutral {@link ConnectorProcedureResult} back into a {@code ResultSet}.</p>
 *
 * <p><b>Engine/connector split (D-062 §2).</b> The engine keeps the command shell — this adapter performs
 * the {@code ALTER} privilege check ({@link #validate}) and the single-row {@code CommonResultSet} wrapping
 * ({@link #execute}); {@code ExecuteActionCommand} keeps the edit-log refresh. The connector owns the
 * procedure body — per-argument validation (the {@code NamedArguments} framework is not reachable across the
 * import gate), the underlying SDK call and the result schema/rows. The connector signals failures with an
 * unchecked {@link DorisConnectorException}; this adapter converts it to a {@code UserException} so
 * {@code ExecuteActionCommand.run()} re-wraps it with the legacy {@code "Failed to execute action:"} prefix.</p>
 *
 * <p><b>Dormant pre-cutover.</b> Iceberg tables are {@code IcebergExternalTable} (not
 * {@code PluginDrivenExternalTable}) until they enter {@code SPI_READY_TYPES} at P6.6, so this adapter is
 * never constructed pre-flip — live {@code ALTER TABLE EXECUTE} still routes to the legacy fe-core actions.</p>
 */
public class ConnectorExecuteAction implements ExecuteAction {

    private final String actionType;
    private final Map<String, String> properties;
    private final Optional<PartitionNamesInfo> partitionNamesInfo;
    private final Optional<Expression> whereCondition;
    private final PluginDrivenExternalTable table;

    public ConnectorExecuteAction(String actionType, Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo, Optional<Expression> whereCondition,
            PluginDrivenExternalTable table) {
        this.actionType = actionType;
        this.properties = properties != null ? properties : Collections.emptyMap();
        this.partitionNamesInfo = partitionNamesInfo;
        this.whereCondition = whereCondition;
        this.table = table;
    }

    @Override
    public void validate(TableNameInfo tableNameInfo, UserIdentity currentUser) throws UserException {
        // Engine keeps the ALTER privilege check (D-062 §2); per-argument validation is connector-owned and
        // runs inside execute() (the NamedArguments framework is not reachable across the connector import gate).
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(),
                        tableNameInfo.getTbl(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER",
                    currentUser.getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getTbl());
        }
    }

    @Override
    public ResultSet execute(TableIf ignored) throws UserException {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        Connector connector = catalog.getConnector();
        ConnectorProcedureOps procedureOps = connector.getProcedureOps();
        if (procedureOps == null) {
            throw new DdlException("Connector '" + catalog.getName() + "' (type: " + catalog.getType()
                    + ") does not support EXECUTE actions");
        }
        // WHERE lowering is deferred to the rewrite_data_files write-path RFC (R-B): the only procedure that
        // accepts a WHERE is rewrite_data_files, which is not routed through this dispatch yet; the eight
        // pure-SDK procedures reject any WHERE. Until lowering lands, a present WHERE is rejected here (fail-loud
        // over silently dropping it). Pre-flip deviation logged with the T08 DV batch (DV-T07-where).
        if (whereCondition.isPresent()) {
            throw new DdlException("WHERE condition is not yet supported for connector EXECUTE actions");
        }

        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle tableHandle = metadata
                .getTableHandle(session, table.getRemoteDbName(), table.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found: " + table.getRemoteDbName()
                        + "." + table.getRemoteName() + " in catalog " + catalog.getName()));
        List<String> partitionNames = partitionNamesInfo
                .map(PartitionNamesInfo::getPartitionNames).orElse(Collections.emptyList());

        try {
            ConnectorProcedureResult result = procedureOps.execute(
                    session, tableHandle, actionType, properties, null, partitionNames);
            return wrapResult(result);
        } catch (DorisConnectorException e) {
            // Surface the connector's unchecked exception as a checked UserException so
            // ExecuteActionCommand.run() catches it and re-wraps it with the legacy "Failed to execute action:"
            // prefix. Use the plain UserException type the legacy action bodies threw (e.g.
            // IcebergRollbackToSnapshotAction.executeAction), so getMessage() formats identically; the message is
            // kept verbatim (the connector preserves the legacy text byte-for-byte — T08 byte-parity).
            throw new UserException(e.getMessage(), e);
        }
    }

    /**
     * Wraps the engine-neutral {@link ConnectorProcedureResult} into a {@link CommonResultSet}, enforcing the
     * legacy single-row contract (each row's width must equal the declared column count,
     * {@code BaseExecuteAction:106-108}). Mirrors {@code BaseExecuteAction.execute}, which returns {@code null}
     * when the metadata is absent OR the body row is {@code null}: the connector encodes a {@code null} body row
     * as {@code (schema, emptyRows)} ({@code BaseIcebergAction.execute}), so an empty schema OR zero rows maps to
     * a {@code null} ResultSet (the command sends nothing).
     */
    private ResultSet wrapResult(ConnectorProcedureResult result) {
        List<ConnectorColumn> resultSchema = result.getResultSchema();
        if (resultSchema == null || resultSchema.isEmpty() || result.getRows().isEmpty()) {
            return null;
        }
        List<Column> columns = ConnectorColumnConverter.convertColumns(resultSchema);
        ResultSetMetaData metaData = new CommonResultSet.CommonResultSetMetaData(columns);
        for (List<String> row : result.getRows()) {
            Preconditions.checkState(columns.size() == row.size(),
                    "Result row size does not match metadata column count");
        }
        return new CommonResultSet(metaData, result.getRows());
    }

    @Override
    public boolean isSupported(TableIf table) {
        // The connector rejects unknown procedure names inside execute() with its own faithful message
        // ("Unsupported <engine> procedure: ..."), so there is no engine-side support pre-filter here.
        return true;
    }

    @Override
    public String getDescription() {
        return "Connector procedure: " + actionType;
    }

    @Override
    public String getActionType() {
        return actionType;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<PartitionNamesInfo> getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    @Override
    public Optional<Expression> getWhereCondition() {
        return whereCondition;
    }
}
