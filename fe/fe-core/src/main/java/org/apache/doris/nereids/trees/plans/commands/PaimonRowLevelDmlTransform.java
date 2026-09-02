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

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Paimon {@link RowLevelDmlTransform}: routes {@code DELETE} on paimon tables through the generic
 * {@link RowLevelDmlCommand} shell.
 *
 * <p>Paimon expresses a delete in one of two shapes, and which applies is a property of the TABLE:
 *
 * <ul>
 * <li><b>Primary-key table</b>: the delete is a {@code RowKind.DELETE} record carrying the key, which the
 *     merge engine cancels against the existing row. No per-row physical address is needed, so this shape
 *     needs no synthetic row-id column at all.</li>
 * <li><b>Append-only table with deletion vectors</b>: the deleted row positions are recorded in a
 *     deletion-vector index file. This shape DOES need a per-row address, which the connector declares as a
 *     synthetic write column.</li>
 * </ul>
 *
 * <p>An append-only table with neither is rejected up front by the connector's
 * {@code validateRowLevelDmlMode} (reached through {@link #checkMode}) — that is a Paimon engine limitation,
 * not a Doris one: with no key to cancel against and no deletion vector to mark positions, there is nowhere
 * to record the deletion.
 *
 * <p>All three row-level operations route through the same shape gate, because they all have to REMOVE the
 * matched row: an UPDATE is planned as a merge (remove the old row, write the new one) and a MERGE INTO
 * combines both. So an append-only table without deletion vectors rejects UPDATE and MERGE for exactly the
 * reason it rejects DELETE.
 */
public class PaimonRowLevelDmlTransform implements RowLevelDmlTransform {

    /**
     * Position-delete metadata column names — the connector-declared row-id STRUCT field names,
     * {@code $}-prefixed. Paimon addresses a row inside an append-only table by data-file name plus the
     * row's ordinal within that file, which is what a deletion vector indexes.
     */
    private static final Set<String> PAIMON_METADATA_COLUMN_NAMES = ImmutableSet.of(
            "$file_path", "$row_position");

    /**
     * Slots excluded from the target-only write constraint: the synthetic row-id column and the paimon
     * metadata columns. These are write plumbing, not user predicates, so a constraint derived for conflict
     * detection must not reference them.
     */
    private static final Predicate<SlotReference> PAIMON_EXCLUSION =
            slot -> PaimonRowLevelDmlColumns.ROWID_COL.equalsIgnoreCase(slot.getName())
                    || PAIMON_METADATA_COLUMN_NAMES.contains(slot.getName());

    @Override
    public boolean handles(TableIf table) {
        // Identity FIRST, capability second — see IcebergRowLevelDmlTransform.handles: the
        // synthesized plan shape is connector-specific, so the claim keys on the connector type.
        return table instanceof PluginDrivenExternalTable
                && "paimon".equalsIgnoreCase(
                        ((PluginDrivenExternalTable) table).getCatalog().getType())
                && pluginConnectorSupportsRowLevelDml((PluginDrivenExternalTable) table);
    }

    /**
     * A plugin-driven table is routed here if its connector declares any row-level write capability.
     *
     * <p>Op-agnostic by design: {@code RowLevelDmlRegistry.find} carries no operation, so this admits
     * "supports row-level DML at all". Whether a PARTICULAR table can carry a PARTICULAR op is decided
     * later by {@link #checkMode}, which consults the connector's per-table shape gate.
     */
    private static boolean pluginConnectorSupportsRowLevelDml(PluginDrivenExternalTable table) {
        Set<WriteOperation> ops = table.connectorSupportedWriteOperations();
        return ops.contains(WriteOperation.DELETE) || ops.contains(WriteOperation.MERGE);
    }

    @Override
    public void checkMode(TableIf table, RowLevelDmlOp op) {
        PluginDrivenExternalTable paimonTable = (PluginDrivenExternalTable) table;
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) paimonTable.getCatalog();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, catalog.getConnector());
        ConnectorTableHandle handle = metadata.getTableHandle(
                        session, paimonTable.getRemoteDbName(), paimonTable.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found: "
                        + paimonTable.getRemoteDbName() + "." + paimonTable.getRemoteName()
                        + " in catalog " + catalog.getName()));
        try {
            // The table-shape knowledge (primary key? deletion vectors?) and its actionable message live in
            // the connector, so the rejection is raised there and surfaced here as the analysis-time
            // exception the user sees. Every row-level op goes through the SAME gate: an UPDATE and a
            // MERGE both have to remove the old row, so they need exactly what a DELETE needs.
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
        ExternalTable paimonTable = (ExternalTable) args.getTable();
        switch (op) {
            case DELETE:
                return new PaimonRowLevelDeletePlanBuilder(
                        args.getNameParts(), args.getTableAlias(), args.isTempPart(),
                        args.getPartitions(), args.getLogicalQuery())
                        .completeQueryPlan(ctx, args.getLogicalQuery(), paimonTable);
            case UPDATE:
                // An UPDATE is synthesized as a merge: remove the matched row, write the new one. On a
                // primary-key table both halves collapse into one keyed upsert record; on an append-only
                // table the removal is a deletion-vector mark plus an append.
                return new ExternalRowLevelUpdatePlanBuilder(
                        args.getNameParts(), args.getTableAlias(), args.getAssignments(),
                        args.getLogicalQuery())
                        .buildMergePlan(ctx, args.getLogicalQuery(), args.getAssignments(), paimonTable);
            default:
                return new PaimonRowLevelMergePlanBuilder(
                        args.getTargetNameParts(), args.getTargetAlias(), args.getCte(),
                        args.getSource(), args.getOnClause(), args.getMatchedClauses(),
                        args.getNotMatchedClauses())
                        .buildMergePlan(ctx, paimonTable);
        }
    }

    @Override
    public BaseExternalTableInsertExecutor newExecutor(ConnectContext ctx, TableIf table, String label,
            NereidsPlanner planner, boolean emptyInsert, RowLevelDmlOp op) {
        // Same connector-driven executor as the insert path: it opens the SPI ConnectorTransaction, which
        // is what activates the neutral conflict path in RowLevelDmlCommand.applyWriteConstraintIfPresent.
        // The op rides the sink's WriteOperation, so no InsertCommandContext is needed.
        return new PluginDrivenInsertExecutor(ctx, (PluginDrivenExternalTable) table, label, planner,
                Optional.empty(), emptyInsert, -1L);
    }

    @Override
    public PhysicalSink<?> requirePhysicalSink(NereidsPlanner planner, RowLevelDmlOp op) {
        Optional<PhysicalSink<?>> plan = planner.getPhysicalPlan()
                .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance).stream().findAny();
        String label = op == RowLevelDmlOp.MERGE ? "MERGE INTO" : op.toString();
        if (!plan.isPresent()) {
            throw new AnalysisException(label + " command must contain target table");
        }
        // DELETE synthesizes a delete sink; UPDATE and MERGE both synthesize a merge sink (an UPDATE is
        // planned as a merge). Checking the sink type here catches a synthesize/plan mismatch before the
        // write reaches the connector.
        Class<?> expected = op == RowLevelDmlOp.DELETE
                ? PhysicalExternalRowLevelDeleteSink.class
                : PhysicalExternalRowLevelMergeSink.class;
        if (!expected.isInstance(plan.get())) {
            throw new AnalysisException(label + " plan must use the row-level "
                    + (op == RowLevelDmlOp.DELETE ? "delete" : "merge") + " sink");
        }
        return plan.get();
    }

    @Override
    public String labelPrefix(RowLevelDmlOp op) {
        switch (op) {
            case DELETE:
                return "paimon_delete";
            case UPDATE:
                return "paimon_update_merge";
            default:
                return "paimon_merge_into";
        }
    }

    @Override
    public void setupConflictDetection(BaseExternalTableInsertExecutor executor, Plan analyzedPlan,
            TableIf table, RowLevelDmlOp op) {
        // No-op: the write constraint is supplied through the neutral SPI path
        // (RowLevelDmlCommand.applyWriteConstraintIfPresent -> extractWriteConstraint ->
        // ConnectorTransaction.applyWriteConstraint). Paimon has no second, native filter path to run.
    }

    @Override
    public void finalizeSink(BaseExternalTableInsertExecutor executor, RowLevelDmlOp op,
            PlanFragment fragment, DataSink sink, PhysicalSink<?> physicalSink) {
        // Finalize through the connector's single transaction model (bind tx -> bindDataSink -> planWrite),
        // exactly as the iceberg path does; the paimon write plan provider supplies the delete write mode.
        ((PluginDrivenInsertExecutor) executor).finalizeRowLevelDmlSink(fragment, sink, physicalSink);
    }

    @Override
    public Optional<ConnectorPredicate> extractWriteConstraint(Plan analyzedPlan, TableIf table) {
        return WriteConstraintExtractor.extract(analyzedPlan, table.getId(), PAIMON_EXCLUSION);
    }
}
