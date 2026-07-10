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
import org.apache.doris.connector.api.procedure.ConnectorRewriteGroup;
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.iceberg.action.BaseIcebergAction;
import org.apache.doris.connector.iceberg.action.IcebergExecuteActionFactory;
import org.apache.doris.connector.iceberg.action.IcebergRewriteDataFilesAction;
import org.apache.doris.connector.iceberg.rewrite.RewriteDataFilePlanner;
import org.apache.doris.connector.iceberg.rewrite.RewriteDataGroup;
import org.apache.doris.connector.spi.ConnectorContext;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    // Per-request catalog-ops resolver: applied with the current ConnectorSession to obtain the IcebergCatalogOps
    // for that request. For a iceberg.rest.session=user catalog the connector passes this::newCatalogBackedOps so
    // ALTER TABLE ... EXECUTE loads the target table through the querying user's per-request delegated REST catalog
    // (fail-closed); every other catalog (and the offline-test ctor) resolves the single shared ops (s -> catalogOps).
    private final Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver;
    private final ConnectorContext context;

    public IcebergProcedureOps(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        // Constant resolver: this ctor (offline tests + the pre-session connector path) binds a single ops that
        // ignores the session, so existing behaviour/tests are byte-identical.
        this(properties, session -> catalogOps, context);
    }

    /**
     * Session-aware ctor used by {@link IcebergConnector#getProcedureOps()}: {@code catalogOpsResolver} is applied
     * per request with the current {@link ConnectorSession} so a {@code iceberg.rest.session=user} catalog resolves
     * the querying user's per-request delegated catalog (the connector passes {@code this::newCatalogBackedOps});
     * every other catalog resolves the single shared ops.
     */
    public IcebergProcedureOps(Map<String, String> properties,
            Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver, ConnectorContext context) {
        this.properties = properties;
        this.catalogOpsResolver = catalogOpsResolver;
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
     * Plans {@code rewrite_data_files} (the one {@link ProcedureExecutionMode#DISTRIBUTED} iceberg procedure)
     * into bin-packed groups for the engine rewrite driver (WS-REWRITE R3). Builds the rewrite action directly
     * — the factory rejects {@code rewrite_data_files} for the single-call {@link #execute} path — validates its
     * arguments, then runs the connector {@link RewriteDataFilePlanner} (the SDK-only planning half) and returns
     * each group's data-file paths + stats in engine-neutral form.
     */
    @Override
    public List<ConnectorRewriteGroup> planRewrite(ConnectorSession session, ConnectorTableHandle table,
            String procedureName, Map<String, String> properties,
            ConnectorPredicate whereCondition, List<String> partitionNames) {
        if (!IcebergExecuteActionFactory.REWRITE_DATA_FILES.equalsIgnoreCase(procedureName)) {
            // Only rewrite_data_files is DISTRIBUTED for iceberg; fail loud on a miswired caller.
            throw new DorisConnectorException("Unsupported distributed iceberg procedure: " + procedureName);
        }
        IcebergTableHandle handle = (IcebergTableHandle) table;
        IcebergRewriteDataFilesAction action = new IcebergRewriteDataFilesAction(
                properties, partitionNames, whereCondition);
        action.validate();
        return planInAuthScope(handle, action, session);
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
     * <p>This method does NOT invalidate any cache. Cache invalidation is the engine's responsibility: after the
     * procedure returns, {@code ConnectorExecuteAction} refreshes the mutated table through the standard
     * refresh-table path — the only path that correctly drops BOTH the engine meta cache (keyed by the table's
     * LOCAL names) and the connector's own per-table cache (keyed by the REMOTE names), resolving both name
     * spaces from the engine's {@code ExternalTable}. The connector alone has only the REMOTE names and cannot
     * reach the engine meta cache correctly, so it must not drive invalidation. {@code session} carries the time
     * zone the {@code rollback_to_timestamp} body needs.
     */
    private ConnectorProcedureResult runInAuthScope(IcebergTableHandle handle, BaseIcebergAction action,
            ConnectorSession session) {
        // Resolve the per-request ops before the auth scope so a session=user fail-closed surfaces the
        // DorisConnectorException verbatim (not wrapped by executeAuthenticated's catch).
        IcebergCatalogOps ops = catalogOpsResolver.apply(session);
        ConnectorProcedureResult result;
        if (context == null) {
            result = action.execute(ops.loadTable(handle.getDbName(), handle.getTableName()), session);
        } else {
            try {
                result = context.executeAuthenticated(() ->
                        action.execute(ops.loadTable(handle.getDbName(), handle.getTableName()), session));
            } catch (DorisConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new DorisConnectorException("Failed to load iceberg table "
                        + handle.getDbName() + "." + handle.getTableName() + ": " + e.getMessage(), e);
            }
        }
        return result;
    }

    /**
     * Loads the table and plans the rewrite groups within ONE authenticated scope (manifest reads need the
     * catalog's auth context on Kerberized catalogs), mirroring {@link #runInAuthScope}. Unlike the procedure
     * bodies, planning does NOT mutate the table or invalidate caches — it only reads the current snapshot's
     * file scan tasks.
     */
    private List<ConnectorRewriteGroup> planInAuthScope(IcebergTableHandle handle,
            IcebergRewriteDataFilesAction action, ConnectorSession session) {
        ZoneId sessionZone = IcebergTimeUtils.resolveSessionZone(session);
        RewriteDataFilePlanner planner = new RewriteDataFilePlanner(action.buildRewriteParameters(), sessionZone);
        // Resolve the per-request ops before the auth scope (see runInAuthScope): a session=user fail-closed
        // surfaces verbatim.
        IcebergCatalogOps ops = catalogOpsResolver.apply(session);
        List<RewriteDataGroup> groups;
        if (context == null) {
            groups = planner.planAndOrganizeTasks(
                    ops.loadTable(handle.getDbName(), handle.getTableName()));
        } else {
            try {
                groups = context.executeAuthenticated(() -> planner.planAndOrganizeTasks(
                        ops.loadTable(handle.getDbName(), handle.getTableName())));
            } catch (DorisConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new DorisConnectorException("Failed to plan rewrite for iceberg table "
                        + handle.getDbName() + "." + handle.getTableName() + ": " + e.getMessage(), e);
            }
        }
        return groups.stream().map(IcebergProcedureOps::toConnectorRewriteGroup).collect(Collectors.toList());
    }

    /**
     * Converts a connector {@link RewriteDataGroup} to the engine-neutral {@link ConnectorRewriteGroup}: the
     * data files become their RAW iceberg paths ({@code dataFile.path()}) — the SAME key the scan provider
     * filters a per-group file scope by (WS-REWRITE R2 [INV-M1]); the counts/size are carried verbatim so the
     * engine sums them into the procedure's result row. {@code LinkedHashSet} keeps a stable iteration order.
     */
    private static ConnectorRewriteGroup toConnectorRewriteGroup(RewriteDataGroup group) {
        Set<String> dataFilePaths = group.getDataFiles().stream()
                .map(dataFile -> dataFile.path().toString())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return new ConnectorRewriteGroup(dataFilePaths, group.getDataFiles().size(),
                group.getTotalSize(), group.getDeleteFileCount());
    }
}
