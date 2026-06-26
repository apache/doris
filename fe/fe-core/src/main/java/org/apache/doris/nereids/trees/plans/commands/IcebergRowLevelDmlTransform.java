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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.WriteConstraintExtractor;
import org.apache.doris.datasource.iceberg.IcebergConflictDetectionFilterUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataColumn;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergDeleteExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergMergeExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Iceberg {@link RowLevelDmlTransform}: routes {@code DELETE}/{@code UPDATE}/{@code MERGE INTO} on iceberg
 * tables through the generic {@link RowLevelDmlCommand} shell.
 *
 * <p>Per the T07c "delegated synthesis" decision, the iceberg plan-synthesis algebra is <b>not</b> relocated:
 * {@link #synthesize} constructs the corresponding {@code Iceberg*Command} (same package) and calls its
 * (now package-visible) synthesis method, so the synthesized {@code LogicalIceberg{Delete,Merge}Sink} tree is
 * byte-identical to legacy. The per-executor-only bits (conflict-filter stash, finalize) are routed here via
 * {@code instanceof}-free op switches; the O5-2 exclusion predicate mirrors legacy
 * {@code IcebergConflictDetectionFilterUtils} (note the {@code equalsIgnoreCase} vs {@code equals} asymmetry).</p>
 */
public class IcebergRowLevelDmlTransform implements RowLevelDmlTransform {

    /**
     * Slots excluded from the O5-2 target-only write constraint: the synthetic {@code $row_id} column and
     * iceberg metadata columns. Mirrors legacy {@code IcebergConflictDetectionFilterUtils.isTargetOnlyPredicate}
     * exactly — keep the {@code equalsIgnoreCase} (rowid) vs {@code equals} (metadata) asymmetry.
     */
    private static final Predicate<SlotReference> ICEBERG_EXCLUSION =
            slot -> Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slot.getName())
                    || IcebergMetadataColumn.isMetadataColumn(slot.getName());

    @Override
    public boolean handles(TableIf table) {
        return table instanceof IcebergExternalTable
                || (table instanceof PluginDrivenExternalTable
                        && pluginConnectorSupportsRowLevelDml((PluginDrivenExternalTable) table));
    }

    /**
     * A plugin-driven (SPI connector) table is routed through the iceberg row-level DML synthesis only if
     * its connector declares row-level DML support ({@code supportsDelete()} or {@code supportsMerge()}).
     * Mirrors the connector-capability probe in
     * {@code InsertOverwriteTableCommand.pluginConnectorSupportsInsertOverwrite}.
     *
     * <p>This gate is op-agnostic by design: {@code RowLevelDmlRegistry.find} carries no operation, so it
     * admits "supports any row-level DML"; per-op validity (e.g. UPDATE against a delete-only connector) is
     * enforced later in {@link #checkMode}.</p>
     *
     * <p>Dormant until the C5 cutover: today only the iceberg connector declares these capabilities (every
     * other SPI connector inherits the {@code ConnectorWriteOps} default {@code false}), and iceberg is not
     * yet in {@code SPI_READY_TYPES} — so no live table presents as a {@link PluginDrivenExternalTable} here.</p>
     */
    private static boolean pluginConnectorSupportsRowLevelDml(PluginDrivenExternalTable table) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorMetadata metadata = catalog.getConnector().getMetadata(catalog.buildConnectorSession());
        return metadata.supportsDelete() || metadata.supportsMerge();
    }

    @Override
    public void checkMode(TableIf table, RowLevelDmlOp op) {
        if (table instanceof PluginDrivenExternalTable) {
            checkPluginMode((PluginDrivenExternalTable) table, op);
            return;
        }
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        switch (op) {
            case DELETE:
                IcebergDmlCommandUtils.checkDeleteMode(icebergTable);
                break;
            case UPDATE:
                IcebergDmlCommandUtils.checkUpdateMode(icebergTable);
                break;
            default:
                IcebergDmlCommandUtils.checkMergeMode(icebergTable);
                break;
        }
    }

    /**
     * Post-cutover {@link #checkMode}: route the copy-on-write rejection through the connector's neutral
     * {@code validateRowLevelDmlMode} SPI, so the iceberg property knowledge and the message stay in the
     * connector. A connector {@link DorisConnectorException} is surfaced as the analysis-time
     * {@link AnalysisException} the legacy native path threw, preserving the user-facing message and the
     * exception type.
     *
     * <p>Dormant until the C5 cutover: today no live table presents as a {@link PluginDrivenExternalTable}
     * here (iceberg is not yet in {@code SPI_READY_TYPES}), so the legacy {@code IcebergDmlCommandUtils} arm
     * still runs.</p>
     */
    private static void checkPluginMode(PluginDrivenExternalTable table, RowLevelDmlOp op) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = catalog.getConnector().getMetadata(session);
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
        IcebergExternalTable icebergTable = (IcebergExternalTable) args.getTable();
        switch (op) {
            case DELETE:
                return new IcebergDeleteCommand(args.getNameParts(), args.getTableAlias(), args.isTempPart(),
                        args.getPartitions(), args.getLogicalQuery(), args.getDeleteCtx())
                        .completeQueryPlan(ctx, args.getLogicalQuery(), icebergTable);
            case UPDATE:
                return new IcebergUpdateCommand(args.getNameParts(), args.getTableAlias(), args.getAssignments(),
                        args.getLogicalQuery(), args.getDeleteCtx())
                        .buildMergePlan(ctx, args.getLogicalQuery(), args.getAssignments(), icebergTable);
            default:
                return new IcebergMergeCommand(args.getTargetNameParts(), args.getTargetAlias(), args.getCte(),
                        args.getSource(), args.getOnClause(), args.getMatchedClauses(), args.getNotMatchedClauses())
                        .buildMergePlan(ctx, icebergTable);
        }
    }

    @Override
    public BaseExternalTableInsertExecutor newExecutor(ConnectContext ctx, TableIf table, String label,
            NereidsPlanner planner, boolean emptyInsert, RowLevelDmlOp op) {
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        if (op == RowLevelDmlOp.DELETE) {
            return new IcebergDeleteExecutor(ctx, icebergTable, label, planner, emptyInsert, -1L);
        }
        return new IcebergMergeExecutor(ctx, icebergTable, label, planner, emptyInsert, -1L);
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
                if (!(plan.get() instanceof PhysicalIcebergDeleteSink)) {
                    throw new AnalysisException("DELETE plan must use Iceberg delete sink");
                }
                return plan.get();
            case UPDATE:
                if (!plan.isPresent()) {
                    throw new AnalysisException("UPDATE command must contain target table");
                }
                if (!(plan.get() instanceof PhysicalIcebergMergeSink)) {
                    throw new AnalysisException("UPDATE merge plan must use Iceberg merge sink");
                }
                return plan.get();
            default:
                if (!plan.isPresent()) {
                    throw new AnalysisException("MERGE INTO command must contain target table");
                }
                if (!(plan.get() instanceof PhysicalIcebergMergeSink)) {
                    throw new AnalysisException("MERGE INTO plan must use Iceberg merge sink");
                }
                return plan.get();
        }
    }

    @Override
    public String labelPrefix(RowLevelDmlOp op) {
        switch (op) {
            case DELETE:
                return "iceberg_delete";
            case UPDATE:
                return "iceberg_update_merge";
            default:
                return "iceberg_merge_into";
        }
    }

    @Override
    public void setupConflictDetection(BaseExternalTableInsertExecutor executor, Plan analyzedPlan, TableIf table,
            RowLevelDmlOp op) {
        Optional<org.apache.iceberg.expressions.Expression> conflictFilter =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(
                        analyzedPlan, (IcebergExternalTable) table);
        if (op == RowLevelDmlOp.DELETE) {
            ((IcebergDeleteExecutor) executor).setConflictDetectionFilter(conflictFilter);
        } else {
            ((IcebergMergeExecutor) executor).setConflictDetectionFilter(conflictFilter);
        }
    }

    @Override
    public void finalizeSink(BaseExternalTableInsertExecutor executor, RowLevelDmlOp op, PlanFragment fragment,
            DataSink sink, PhysicalSink<?> physicalSink) {
        if (op == RowLevelDmlOp.DELETE) {
            ((IcebergDeleteExecutor) executor).finalizeSinkForDelete(fragment, sink, physicalSink);
        } else {
            ((IcebergMergeExecutor) executor).finalizeSinkForMerge(fragment, sink, physicalSink);
        }
    }

    @Override
    public Optional<ConnectorPredicate> extractWriteConstraint(Plan analyzedPlan, TableIf table) {
        return WriteConstraintExtractor.extract(analyzedPlan, table.getId(), ICEBERG_EXCLUSION);
    }
}
