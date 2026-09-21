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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.connector.converter.WriteConstraintExtractor;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelDeleteSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Position-delete {@link RowLevelDmlTransform}: routes {@code DELETE}/{@code UPDATE}/{@code MERGE INTO}
 * through the generic {@link RowLevelDmlCommand} shell.
 *
 * <p>The plan-synthesis algebra lives in same-package neutral helpers: {@link #synthesize} constructs
 * the corresponding {@code ExternalRowLevel*PlanBuilder} and calls its (package-visible) synthesis method, so
 * the synthesized {@code LogicalExternalRowLevel{Delete,Merge}Sink} tree is the generic row-level DML sink.
 * The per-executor-only bits (conflict-filter stash, finalize) are routed here via
 * {@code instanceof}-free operation switches. Connector-owned metadata column names are obtained through
 * the write-provider SPI rather than embedded in engine code.</p>
 */
public class PositionDeleteRowLevelDmlTransform implements RowLevelDmlTransform {

    @Override
    public boolean handles(TableIf table) {
        return table instanceof PluginDrivenExternalTable
                && ((PluginDrivenExternalTable) table).getConnectorRowChangeStyle()
                        == ConnectorRowChangeStyle.POSITION_DELETE
                && pluginConnectorSupportsRowLevelDml((PluginDrivenExternalTable) table);
    }

    /**
     * A plugin-driven table is routed through position-delete row-level DML synthesis only if
     * its connector declares row-level DML support ({@code supportsDelete()} or {@code supportsMerge()}).
     * Mirrors the connector-capability probe in
     * {@code InsertOverwriteTableCommand.pluginConnectorSupportsInsertOverwrite}.
     *
     * <p>This gate is op-agnostic by design: {@code RowLevelDmlRegistry.find} carries no operation, so it
     * admits "supports any row-level DML"; per-op validity (e.g. UPDATE against a delete-only connector) is
     * enforced later in {@link #checkMode}.</p>
     *
     * <p>The representation check in {@link #handles} must precede this capability check: a connector
     * using changelog rows may support the same operations but cannot use the position-delete plan.</p>
     */
    private static boolean pluginConnectorSupportsRowLevelDml(PluginDrivenExternalTable table) {
        // Per-handle write-op probe lets a heterogeneous gateway select only qualifying tables.
        Set<WriteOperation> ops = table.connectorSupportedWriteOperations();
        return ops.contains(WriteOperation.DELETE) || ops.contains(WriteOperation.MERGE);
    }

    @Override
    public void checkMode(TableIf table, RowLevelDmlOp op) {
        PluginDrivenExternalTable connectorTable = (PluginDrivenExternalTable) table;
        WriteOperation operation = toWriteOperation(op);
        if (!connectorTable.connectorSupportedWriteOperations().contains(operation)) {
            throw new AnalysisException("Connector does not support " + operation + " operations");
        }
        checkPluginMode(connectorTable, op);
    }

    /**
     * {@link #checkMode} body: route the copy-on-write rejection through the connector's neutral
     * {@code validateRowLevelDmlMode} SPI, so format-specific properties and messages stay in the
     * connector. A connector {@link DorisConnectorException} is surfaced as the analysis-time
     * {@link AnalysisException} the legacy native path threw, preserving the user-facing message and the
     * exception type.
     */
    private static void checkPluginMode(PluginDrivenExternalTable table, RowLevelDmlOp op) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, catalog.getConnector());
        ConnectorTableHandle handle = metadata.getTableHandle(
                        session, table.getRemoteDbName(), table.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found: "
                        + table.getRemoteDbName() + "." + table.getRemoteName()
                        + " in catalog " + catalog.getName()));
        try {
            metadata.validateRowLevelDmlMode(session, handle, toWriteOperation(op));
        } catch (DorisConnectorException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    private static WriteOperation toWriteOperation(RowLevelDmlOp op) {
        switch (op) {
            case DELETE:
                return WriteOperation.DELETE;
            case UPDATE:
                return WriteOperation.UPDATE;
            default:
                return WriteOperation.MERGE;
        }
    }

    @Override
    public LogicalPlan synthesize(ConnectContext ctx, RowLevelDmlArgs args, RowLevelDmlOp op) {
        ExternalTable externalTable = (ExternalTable) args.getTable();
        switch (op) {
            case DELETE:
                return new ExternalRowLevelDeletePlanBuilder(
                        args.getNameParts(), args.getTableAlias(), args.isTempPart(),
                        args.getPartitions(), args.getLogicalQuery())
                        .completeQueryPlan(ctx, args.getLogicalQuery(), externalTable);
            case UPDATE:
                return new ExternalRowLevelUpdatePlanBuilder(
                        args.getNameParts(), args.getTableAlias(), args.getAssignments(),
                        args.getLogicalQuery())
                        .buildMergePlan(ctx, args.getLogicalQuery(), args.getAssignments(), externalTable);
            default:
                return new ExternalRowLevelMergePlanBuilder(
                        args.getTargetNameParts(), args.getTargetAlias(), args.getCte(),
                        args.getSource(), args.getOnClause(), args.getMatchedClauses(), args.getNotMatchedClauses())
                        .buildMergePlan(ctx, externalTable);
        }
    }

    @Override
    public BaseExternalTableInsertExecutor newExecutor(ConnectContext ctx, TableIf table, String label,
            NereidsPlanner planner, boolean emptyInsert, RowLevelDmlOp op) {
        // The connector-driven executor opens an SPI ConnectorTransaction (non-null), which activates the
        // neutral conflict path in RowLevelDmlCommand.applyWriteConstraintIfPresent. The op rides the
        // sink's WriteOperation (set by the translator), so one executor serves DELETE/MERGE; no
        // InsertCommandContext is needed for a row-level write.
        return new PluginDrivenInsertExecutor(ctx, (PluginDrivenExternalTable) table, label, planner,
                Optional.empty(), emptyInsert, -1L);
    }

    @Override
    public PhysicalSink<?> requirePhysicalSink(NereidsPlanner planner, RowLevelDmlOp op) {
        Optional<PhysicalSink<?>> plan = planner.getPhysicalPlan()
                .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance).stream().findAny();
        switch (op) {
            case DELETE:
                if (!plan.isPresent()) {
                    throw new AnalysisException("DELETE command must contain target table");
                }
                if (!(plan.get() instanceof PhysicalExternalRowLevelDeleteSink)) {
                    throw new AnalysisException("DELETE plan must use a position-delete sink");
                }
                return plan.get();
            case UPDATE:
                if (!plan.isPresent()) {
                    throw new AnalysisException("UPDATE command must contain target table");
                }
                if (!(plan.get() instanceof PhysicalExternalRowLevelMergeSink)) {
                    throw new AnalysisException("UPDATE plan must use a position-delete merge sink");
                }
                return plan.get();
            default:
                if (!plan.isPresent()) {
                    throw new AnalysisException("MERGE INTO command must contain target table");
                }
                if (!(plan.get() instanceof PhysicalExternalRowLevelMergeSink)) {
                    throw new AnalysisException("MERGE INTO plan must use a position-delete merge sink");
                }
                return plan.get();
        }
    }

    @Override
    public String labelPrefix(TableIf table, RowLevelDmlOp op) {
        return ((PluginDrivenExternalTable) table)
                .getConnectorRowLevelDmlLabelPrefix(toWriteOperation(op));
    }

    @Override
    public void setupConflictDetection(BaseExternalTableInsertExecutor executor, Plan analyzedPlan, TableIf table,
            RowLevelDmlOp op) {
        // No-op: the conflict filter is supplied through the neutral SPI path
        // (RowLevelDmlCommand.applyWriteConstraintIfPresent -> extractWriteConstraint ->
        // ConnectorTransaction.applyWriteConstraint). Running only the SPI path avoids applying the same
        // optimistic-conflict predicate twice.
    }

    @Override
    public void finalizeSink(BaseExternalTableInsertExecutor executor, RowLevelDmlOp op, PlanFragment fragment,
            DataSink sink, PhysicalSink<?> physicalSink) {
        // Finalize through the connector's single transaction model (bind tx -> bindDataSink -> planWrite),
        // which supplies rewritable_delete_file_sets itself via the scan-time stash -> exactly one finalize,
        // no double-overlay.
        ((PluginDrivenInsertExecutor) executor).finalizeRowLevelDmlSink(fragment, sink, physicalSink);
    }

    @Override
    public Optional<ConnectorPredicate> extractWriteConstraint(Plan analyzedPlan, TableIf table) {
        Set<String> excludedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        excludedColumns.addAll(((PluginDrivenExternalTable) table)
                .getConnectorRowLevelWriteConstraintExcludedColumns());
        Predicate<SlotReference> exclusion = slot -> excludedColumns.contains(slot.getName());
        return WriteConstraintExtractor.extract(analyzedPlan, table.getId(), exclusion);
    }
}
