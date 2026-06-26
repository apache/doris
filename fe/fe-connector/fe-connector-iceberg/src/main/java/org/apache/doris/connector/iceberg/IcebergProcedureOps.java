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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.iceberg.action.BaseIcebergAction;
import org.apache.doris.connector.iceberg.action.IcebergExecuteActionFactory;
import org.apache.doris.connector.spi.ConnectorContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Executes iceberg's {@code ALTER TABLE EXECUTE} procedures (the 9 legacy
 * {@code datasource/iceberg/action/*} actions) behind the {@link ConnectorProcedureOps} SPI.
 *
 * <p>Mirrors {@link IcebergWritePlanProvider}: a fresh instance per call over the lazily-built live
 * catalog, threading the same {@code properties} / {@link IcebergCatalogOps} / {@link ConnectorContext}
 * seams. The SDK table is loaded (inside {@code context.executeAuthenticated}) and the procedure body
 * runs in the connector; argument validation is connector-local (the engine cannot reach
 * {@code org.apache.doris.common.NamedArguments} across the import gate).</p>
 *
 * <p><b>T03 dispatch skeleton.</b> {@link #getSupportedProcedures()} exports the factory's name list and
 * {@link #execute} routes through {@link IcebergExecuteActionFactory} → {@link BaseIcebergAction}: validate
 * arguments, load the SDK table inside {@code context.executeAuthenticated}, run the body and wrap the
 * single row. The 9 procedure bodies (the factory's switch cases) are ported in T04 (the 8 pure-SDK
 * procedures) / T05–T06 ({@code rewrite_data_files}); until then a known name reaches the factory's faithful
 * "Unsupported Iceberg procedure" rejection. Inert pre-cutover regardless: iceberg tables are not
 * {@code PluginDrivenExternalTable} until P6.6, so {@code ExecuteActionCommand} still routes them to the
 * legacy fe-core actions and never reaches this class.</p>
 */
public class IcebergProcedureOps implements ConnectorProcedureOps {

    // Catalog-level properties seam, retained for structural symmetry with IcebergWritePlanProvider. The
    // per-procedure arguments arrive through execute()'s own {@code properties} parameter (the EXECUTE
    // properties), so this field is not read here.
    private final Map<String, String> properties;
    private final IcebergCatalogOps catalogOps;
    private final ConnectorContext context;

    public IcebergProcedureOps(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    @Override
    public List<String> getSupportedProcedures() {
        return Arrays.asList(IcebergExecuteActionFactory.getSupportedActions());
    }

    /**
     * {@code rewrite_data_files} is the one distributed procedure — it runs N per-group INSERT-SELECT writes
     * under one shared transaction (the engine-side rewrite driver), so the engine must orchestrate it rather
     * than dispatch it through {@link #execute}. Every other iceberg procedure is a synchronous SDK call
     * ({@link ProcedureExecutionMode#SINGLE_CALL}). Case-insensitive to mirror the factory's
     * {@code actionType.toLowerCase()} dispatch.
     */
    @Override
    public ProcedureExecutionMode getExecutionMode(String procedureName) {
        return IcebergExecuteActionFactory.REWRITE_DATA_FILES.equalsIgnoreCase(procedureName)
                ? ProcedureExecutionMode.DISTRIBUTED
                : ProcedureExecutionMode.SINGLE_CALL;
    }

    @Override
    public ConnectorProcedureResult execute(ConnectorSession session, ConnectorTableHandle table,
            String procedureName, Map<String, String> properties,
            ConnectorPredicate whereCondition, List<String> partitionNames) {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        // Build the procedure body (rejects unknown names) and validate its arguments before touching the
        // catalog — the engine has already performed the ALTER privilege check (D-062 §2).
        BaseIcebergAction action = IcebergExecuteActionFactory.createAction(
                procedureName, properties, partitionNames, whereCondition);
        action.validate();
        return runInAuthScope(handle, action, session);
    }

    /**
     * Loads the table and runs the procedure body within ONE authenticated scope. The body's SDK
     * manipulation and remote {@code commit()} must run under the catalog's auth context (Kerberized
     * catalogs), mirroring the write path — recon §7 "commit 裹 executeAuthenticated"; this is the auth fix
     * the legacy fe-core actions lacked (the snapshot mutators ran their commit unauthenticated).
     *
     * <p>The body's own {@link DorisConnectorException} (argument / procedure-body failures) surfaces
     * verbatim — the engine command shell re-wraps it with the user-facing "Failed to execute action:"
     * prefix when {@code ExecuteActionCommand} is rewired to this SPI at the iceberg cutover (T07).
     *
     * <p>On success the table's cached metadata is invalidated through {@code context.getMetaInvalidator()}
     * (default {@code NOOP}); this replaces the legacy per-action {@code ExtMetaCacheMgr.invalidateTableCache}
     * (fe-core-only, dropped from the bodies). Invalidation is unconditional on a normal return — including a
     * no-op short-circuit (e.g. {@code rollback_to_snapshot} already on the target) where legacy skipped it;
     * the extra invalidation is idempotent on an unchanged table (a pre-flip behavioural nuance logged with
     * the T08 DV batch). {@code session} carries the time zone the {@code rollback_to_timestamp} body needs.
     */
    private ConnectorProcedureResult runInAuthScope(IcebergTableHandle handle, BaseIcebergAction action,
            ConnectorSession session) {
        ConnectorProcedureResult result;
        if (context == null) {
            result = action.execute(catalogOps.loadTable(handle.getDbName(), handle.getTableName()), session);
        } else {
            try {
                result = context.executeAuthenticated(() ->
                        action.execute(catalogOps.loadTable(handle.getDbName(), handle.getTableName()), session));
            } catch (DorisConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new DorisConnectorException("Failed to load iceberg table "
                        + handle.getDbName() + "." + handle.getTableName() + ": " + e.getMessage(), e);
            }
            context.getMetaInvalidator().invalidateTable(handle.getDbName(), handle.getTableName());
        }
        return result;
    }
}
