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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.connector.spi.write.ConnectorRowLevelDmlRequest;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.ConnectorChangelogRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.physical.PhysicalConnectorTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Plans row-level changes as an operation column followed by a complete table row. */
public class ChangelogRowLevelDmlTransform implements RowLevelDmlTransform {

    @Override
    public boolean handles(TableIf table) {
        if (!(table instanceof PluginDrivenExternalTable)) {
            return false;
        }
        PluginDrivenExternalTable connectorTable = (PluginDrivenExternalTable) table;
        if (connectorTable.getConnectorRowChangeStyle() != ConnectorRowChangeStyle.CHANGELOG) {
            return false;
        }
        return RowLevelDmlRegistry.supportsAnyRowLevelDml(
                connectorTable.connectorSupportedWriteOperations());
    }

    @Override
    public void checkMode(TableIf table, RowLevelDmlOp op) {
        WriteOperation operation = op.toWriteOperation();
        if (!((PluginDrivenExternalTable) table).connectorSupportedWriteOperations().contains(operation)) {
            throw new AnalysisException("Connector does not support " + operation + " operations");
        }
        // Statement-specific validation runs in synthesize, where assignments and MERGE clauses are available.
    }

    @Override
    public LogicalPlan synthesize(ConnectContext ctx, RowLevelDmlArgs args, RowLevelDmlOp op) {
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) args.getTable();
        if (op == RowLevelDmlOp.DELETE && (args.isTempPart() || !args.getPartitions().isEmpty())) {
            throw new AnalysisException(
                    "Connector changelog DELETE does not support partition name lists; use a WHERE predicate");
        }
        validate(ctx, table, args, op);
        switch (op) {
            case DELETE:
                return deletePlan(ctx, args);
            case UPDATE:
                return updatePlan(ctx, args);
            default:
                return mergePlan(ctx, args);
        }
    }

    private LogicalPlan deletePlan(ConnectContext ctx, RowLevelDmlArgs args) {
        List<String> target = args.getTableAlias() != null
                ? ImmutableList.of(args.getTableAlias())
                : RelationUtil.getQualifierName(ctx, args.getNameParts());
        return new UnboundConnectorTableSink<>(args.getNameParts(), args.getLogicalQuery(),
                new ConnectorChangelogRowChangeSpec.Delete(target, args.shouldDeduplicateTargetRows()));
    }

    private LogicalPlan updatePlan(ConnectContext ctx, RowLevelDmlArgs args) {
        for (EqualTo assignment : args.getAssignments()) {
            UpdateCommand.checkAssignmentColumn(ctx,
                    ((UnboundSlot) assignment.left()).getNameParts(),
                    args.getNameParts(), args.getTableAlias());
        }
        List<String> target = args.getTableAlias() != null
                ? ImmutableList.of(args.getTableAlias())
                : RelationUtil.getQualifierName(ctx, args.getNameParts());
        LogicalPlan sink = new UnboundConnectorTableSink<>(args.getNameParts(), args.getLogicalQuery(),
                new ConnectorChangelogRowChangeSpec.Update(target, args.getAssignments()));
        return args.getCte().isPresent() ? (LogicalPlan) args.getCte().get().withChildren(sink) : sink;
    }

    private LogicalPlan mergePlan(ConnectContext ctx, RowLevelDmlArgs args) {
        for (MergeMatchedClause clause : args.getMatchedClauses()) {
            for (EqualTo assignment : clause.getAssignments()) {
                UpdateCommand.checkAssignmentColumn(ctx,
                        ((UnboundSlot) assignment.left()).getNameParts(),
                        args.getTargetNameParts(), args.getTargetAlias().orElse(null));
            }
        }
        List<String> targetName = args.getTargetAlias().isPresent()
                ? ImmutableList.of(args.getTargetAlias().get())
                : RelationUtil.getQualifierName(ctx, args.getTargetNameParts());
        ConnectorChangelogRowChangeSpec.Merge spec = new ConnectorChangelogRowChangeSpec.Merge(
                targetName, args.getMatchedClauses(), args.getNotMatchedClauses());
        LogicalPlan target = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), args.getTargetNameParts()));
        if (args.getTargetAlias().isPresent()) {
            target = new LogicalSubQueryAlias<>(args.getTargetAlias().get(), target);
        }
        LogicalPlan join = MergeUtils.buildMergeJoin(target, args.getSource(), args.getOnClause(),
                !args.getNotMatchedClauses().isEmpty());
        LogicalPlan sink = new UnboundConnectorTableSink<>(args.getTargetNameParts(), join, spec);
        return args.getCte().isPresent() ? (LogicalPlan) args.getCte().get().withChildren(sink) : sink;
    }

    private void validate(ConnectContext ctx, PluginDrivenExternalTable table,
            RowLevelDmlArgs args, RowLevelDmlOp op) {
        requireNoDataMask(ctx, table, op);
        Set<String> updatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean containsUpdate = op == RowLevelDmlOp.UPDATE;
        boolean containsDelete = op == RowLevelDmlOp.DELETE;
        if (op == RowLevelDmlOp.UPDATE) {
            addUpdatedColumns(updatedColumns, args.getAssignments());
        } else if (op == RowLevelDmlOp.MERGE) {
            for (MergeMatchedClause clause : args.getMatchedClauses()) {
                containsDelete |= clause.isDelete();
                containsUpdate |= !clause.isDelete();
                addUpdatedColumns(updatedColumns, clause.getAssignments());
            }
        }
        try {
            table.validateConnectorRowLevelDml(new ConnectorRowLevelDmlRequest(
                    op.toWriteOperation(), updatedColumns, containsUpdate, containsDelete));
        } catch (DorisConnectorException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    static void requireNoDataMask(ConnectContext ctx, PluginDrivenExternalTable table, RowLevelDmlOp op) {
        UserIdentity user = ctx.getCurrentUserIdentity();
        if (user.isRootUser() || user.isAdminUser()) {
            return;
        }
        DatabaseIf<?> database = table.getDatabase();
        CatalogIf<?> catalog = database.getCatalog();
        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getFullSchema()) {
            columns.add(column.getName());
        }
        AccessControllerManager accessManager = ctx.getEnv().getAccessManager();
        if (!accessManager.evalDataMaskPolicies(
                user, catalog.getName(), database.getFullName(), table.getName(), columns).isEmpty()) {
            throw new AnalysisException("Connector " + op
                    + " is not supported when data masking policies apply to the target table");
        }
    }

    private void addUpdatedColumns(Set<String> columns, List<EqualTo> assignments) {
        for (EqualTo assignment : assignments) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            columns.add(parts.get(parts.size() - 1));
        }
    }

    @Override
    public BaseExternalTableInsertExecutor newExecutor(ConnectContext ctx, TableIf table, String label,
            NereidsPlanner planner, boolean emptyInsert, RowLevelDmlOp op) {
        return new PluginDrivenInsertExecutor(ctx, (PluginDrivenExternalTable) table, label,
                planner, Optional.empty(), emptyInsert, -1L);
    }

    @Override
    public PhysicalSink<?> requirePhysicalSink(NereidsPlanner planner, RowLevelDmlOp op) {
        return planner.getPhysicalPlan().<PhysicalSink<?>>collect(PhysicalSink.class::isInstance)
                .stream().filter(PhysicalConnectorTableSink.class::isInstance).findAny()
                .orElseThrow(() -> new AnalysisException(op + " plan must use connector table sink"));
    }

    @Override
    public String labelPrefix(TableIf table, RowLevelDmlOp op) {
        return ((PluginDrivenExternalTable) table).getConnectorRowLevelDmlLabelPrefix(op.toWriteOperation());
    }

    @Override
    public void finalizeSink(BaseExternalTableInsertExecutor executor, RowLevelDmlOp op,
            PlanFragment fragment, DataSink sink, PhysicalSink<?> physicalSink) {
        ((PluginDrivenInsertExecutor) executor).finalizeRowLevelDmlSink(fragment, sink, physicalSink);
    }

    @Override
    public Optional<ConnectorPredicate> extractWriteConstraint(Plan analyzedPlan, TableIf table) {
        return Optional.empty();
    }
}
