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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.iceberg.IcebergConflictDetectionFilterUtils;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergNereidsUtils;
import org.apache.doris.datasource.iceberg.IcebergRowId;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergMergeExecutor;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * MERGE INTO command for Iceberg tables.
 */
public class IcebergMergeCommand extends Command implements ForwardWithSync, Explainable {
    private static final String BRANCH_LABEL = "__DORIS_ICEBERG_MERGE_INTO_BRANCH_LABEL__";

    private final List<String> targetNameParts;
    private final Optional<String> targetAlias;
    private final List<String> targetNameInPlan;
    private final Optional<LogicalPlan> cte;
    private final LogicalPlan source;
    private final Expression onClause;
    private final List<MergeMatchedClause> matchedClauses;
    private final List<MergeNotMatchedClause> notMatchedClauses;
    private final DeleteCommandContext deleteCtx;

    /**
     * constructor.
     */
    public IcebergMergeCommand(List<String> targetNameParts, Optional<String> targetAlias,
            Optional<LogicalPlan> cte, LogicalPlan source, Expression onClause,
            List<MergeMatchedClause> matchedClauses, List<MergeNotMatchedClause> notMatchedClauses) {
        super(PlanType.MERGE_INTO_COMMAND);
        this.targetNameParts = Utils.copyRequiredList(targetNameParts);
        this.targetAlias = Objects.requireNonNull(targetAlias, "targetAlias should not be null");
        if (targetAlias.isPresent()) {
            this.targetNameInPlan = ImmutableList.of(targetAlias.get());
        } else {
            this.targetNameInPlan = ImmutableList.copyOf(targetNameParts);
        }
        this.cte = Objects.requireNonNull(cte, "cte should not be null");
        this.source = Objects.requireNonNull(source, "source should not be null");
        this.onClause = Objects.requireNonNull(onClause, "onClause should not be null");
        this.matchedClauses = Utils.fastToImmutableList(
                Objects.requireNonNull(matchedClauses, "matchedClauses should not be null"));
        this.notMatchedClauses = Utils.fastToImmutableList(
                Objects.requireNonNull(notMatchedClauses, "notMatchedClauses should not be null"));
        this.deleteCtx = new DeleteCommandContext();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        TableIf table = getTargetTable(ctx);
        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("MERGE INTO can only be used on Iceberg tables. "
                    + "Table " + Util.getTempTableDisplayName(table.getName()) + " is not an Iceberg table.");
        }
        IcebergExternalTable icebergTable = (IcebergExternalTable) table;
        long previousTargetTableId = ctx.getIcebergRowIdTargetTableId();
        ctx.setIcebergRowIdTargetTableId(icebergTable.getId());
        try {
            LogicalPlan mergePlan = buildMergePlan(ctx, icebergTable);
            executeMergePlan(ctx, executor, icebergTable, mergePlan);
        } finally {
            ctx.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        TableIf table = getTargetTable(ctx);
        if (!(table instanceof IcebergExternalTable)) {
            throw new AnalysisException("MERGE INTO can only be used on Iceberg tables. "
                    + "Table " + Util.getTempTableDisplayName(table.getName()) + " is not an Iceberg table.");
        }
        long previousTargetTableId = ctx.getIcebergRowIdTargetTableId();
        ctx.setIcebergRowIdTargetTableId(((IcebergExternalTable) table).getId());
        try {
            return buildMergePlan(ctx, (IcebergExternalTable) table);
        } finally {
            ctx.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.MERGE_INTO;
    }

    private TableIf getTargetTable(ConnectContext ctx) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, targetNameParts);
        return RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());
    }

    private LogicalPlan generateBasePlan() {
        LogicalPlan targetPlan = LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(
                        StatementScopeIdGenerator.newRelationId(),
                        targetNameParts
                )
        );
        if (targetAlias.isPresent()) {
            targetPlan = new LogicalSubQueryAlias<>(targetAlias.get(), targetPlan);
        }
        // Use INNER JOIN when there are no WHEN NOT MATCHED clauses, since unmatched
        // source rows are not needed. This allows early filtering for better performance.
        JoinType joinType = notMatchedClauses.isEmpty()
                ? JoinType.INNER_JOIN : JoinType.LEFT_OUTER_JOIN;
        return new LogicalJoin<>(joinType,
                ImmutableList.of(), ImmutableList.of(onClause),
                source, targetPlan, JoinReorderContext.EMPTY);
    }

    private NamedExpression generateBranchLabel(Expression rowIdExpr) {
        Expression matchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = matchedClauses.size() - 1; i >= 0; i--) {
            MergeMatchedClause clause = matchedClauses.get(i);
            if (i != matchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last matched clause could without case predicate.");
            }
            Expression currentResult = new IntegerLiteral(i);
            if (clause.getCasePredicate().isPresent()) {
                matchedLabel = new If(clause.getCasePredicate().get(), currentResult, matchedLabel);
            } else {
                matchedLabel = currentResult;
            }
        }

        Expression notMatchedLabel = new NullLiteral(IntegerType.INSTANCE);
        for (int i = notMatchedClauses.size() - 1; i >= 0; i--) {
            MergeNotMatchedClause clause = notMatchedClauses.get(i);
            if (i != notMatchedClauses.size() - 1 && !clause.getCasePredicate().isPresent()) {
                throw new AnalysisException("Only the last not matched clause could without case predicate.");
            }
            Expression currentResult = new IntegerLiteral(i + matchedClauses.size());
            if (clause.getCasePredicate().isPresent()) {
                notMatchedLabel = new If(clause.getCasePredicate().get(), currentResult, notMatchedLabel);
            } else {
                notMatchedLabel = currentResult;
            }
        }

        return new UnboundAlias(new If(new Not(new IsNull(rowIdExpr)), matchedLabel, notMatchedLabel),
                BRANCH_LABEL);
    }

    private List<Expression> buildDeleteProjection(Expression rowIdExpr, List<Column> columns) {
        List<Expression> projection = new ArrayList<>();
        projection.add(new TinyIntLiteral(IcebergMergeOperation.DELETE_OPERATION_NUMBER));
        projection.add(rowIdExpr);
        for (Column column : columns) {
            if (!column.isVisible()) {
                continue;
            }
            List<String> nameParts = Lists.newArrayList(targetNameInPlan);
            nameParts.add(column.getName());
            projection.add(new UnboundSlot(nameParts));
        }
        return projection;
    }

    private List<Expression> buildUpdateProjection(MergeMatchedClause clause, Expression rowIdExpr,
            List<Column> columns, ConnectContext ctx) {
        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo equalTo : clause.getAssignments()) {
            List<String> nameParts = ((UnboundSlot) equalTo.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(ctx, nameParts, targetNameParts, targetAlias.orElse(null));
            String columnName = nameParts.get(nameParts.size() - 1);
            if (colNameToExpression.put(columnName, equalTo.right()) != null) {
                throw new AnalysisException("Duplicate column name in update: " + columnName);
            }
        }
        List<Expression> projection = new ArrayList<>();
        projection.add(new TinyIntLiteral(IcebergMergeOperation.UPDATE_OPERATION_NUMBER));
        projection.add(rowIdExpr);
        for (Column column : columns) {
            if (!column.isVisible()) {
                continue;
            }
            if (column.isGeneratedColumn()) {
                throw new AnalysisException("The value specified for generated column '"
                        + column.getName() + "' in table '" + getTargetTable(ctx).getName() + "' is not allowed.");
            }
            if (colNameToExpression.containsKey(column.getName())) {
                projection.add(colNameToExpression.remove(column.getName()));
            } else {
                List<String> nameParts = Lists.newArrayList(targetNameInPlan);
                nameParts.add(column.getName());
                projection.add(new UnboundSlot(nameParts));
            }
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in assignment list: "
                    + String.join(", ", colNameToExpression.keySet()));
        }
        return projection;
    }

    private List<Expression> buildInsertProjection(MergeNotMatchedClause clause,
            List<Column> columns, ConnectContext ctx, DataType rowIdType) {
        Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (!clause.getColNames().isEmpty()) {
            if (clause.getColNames().size() != clause.getRow().size()) {
                throw new AnalysisException("Column count doesn't match value count");
            }
            for (int i = 0; i < clause.getColNames().size(); i++) {
                String targetColumnName = clause.getColNames().get(i);
                NamedExpression rowItem = clause.getRow().get(i);
                Expression value = rowItem instanceof UnboundAlias ? rowItem.child(0) : rowItem;
                if (rowItem instanceof Alias) {
                    value = rowItem.child(0);
                }
                if (colNameToExpression.put(targetColumnName, value) != null) {
                    throw new AnalysisException("insert has duplicate column names");
                }
            }
        } else {
            long visibleColumnCount = columns.stream().filter(Column::isVisible).count();
            if (visibleColumnCount != clause.getRow().size()) {
                throw new AnalysisException("Column count doesn't match value count");
            }
        }

        List<Expression> projection = new ArrayList<>();
        projection.add(new TinyIntLiteral(IcebergMergeOperation.INSERT_OPERATION_NUMBER));
        projection.add(new NullLiteral(rowIdType));

        int visibleIndex = 0;
        for (Column column : columns) {
            if (!column.isVisible()) {
                continue;
            }
            if (column.isGeneratedColumn()) {
                throw new AnalysisException("The value specified for generated column '"
                        + column.getName() + "' in table '" + getTargetTable(ctx).getName() + "' is not allowed.");
            }
            Expression value = null;
            if (!clause.getColNames().isEmpty()) {
                value = colNameToExpression.remove(column.getName());
            } else {
                NamedExpression rowItem = clause.getRow().get(visibleIndex++);
                value = rowItem instanceof UnboundAlias ? rowItem.child(0) : rowItem;
                if (rowItem instanceof Alias) {
                    value = rowItem.child(0);
                }
            }
            if (value == null) {
                if (column.getDefaultValueSql() != null) {
                    Expression unboundDefaultValue = new NereidsParser()
                            .parseExpression(column.getDefaultValueSql());
                    if (unboundDefaultValue instanceof UnboundAlias) {
                        unboundDefaultValue = unboundDefaultValue.child(0);
                    }
                    value = unboundDefaultValue;
                } else if (column.isAllowNull()) {
                    value = new NullLiteral(DataType.fromCatalogType(column.getType()));
                } else {
                    throw new AnalysisException("Column has no default value, column=" + column.getName());
                }
            }
            projection.add(value);
        }
        if (!colNameToExpression.isEmpty()) {
            throw new AnalysisException("unknown column in target table: "
                    + String.join(", ", colNameToExpression.keySet()));
        }
        return projection;
    }

    private List<NamedExpression> generateFinalProjections(List<String> colNames,
            List<List<Expression>> finalProjections) {
        for (List<Expression> projection : finalProjections) {
            if (projection.size() != finalProjections.get(0).size()) {
                throw new AnalysisException("Column count doesn't match each other");
            }
        }
        List<NamedExpression> output = new ArrayList<>();
        for (int i = 0; i < finalProjections.get(0).size(); i++) {
            Expression project = new NullLiteral();
            for (int j = 0; j < finalProjections.size(); j++) {
                project = new If(new EqualTo(new UnboundSlot(BRANCH_LABEL), new IntegerLiteral(j)),
                        finalProjections.get(j).get(i), project);
            }
            output.add(new UnboundAlias(project, colNames.get(i)));
        }
        return output;
    }

    private LogicalPlan buildMergeProjectPlan(ConnectContext ctx, IcebergExternalTable icebergTable) {
        List<Column> columns = icebergTable.getBaseSchema(true);

        LogicalPlan plan = generateBasePlan();
        plan = injectRowIdColumn(plan, icebergTable);

        Expression rowIdExpr = getTargetRowIdSlot();
        if (!IcebergNereidsUtils.hasUnboundPlan(plan)) {
            Optional<Slot> rowIdSlot = IcebergNereidsUtils.findRowIdSlot(plan.getOutput());
            if (rowIdSlot.isPresent()) {
                rowIdExpr = rowIdSlot.get();
            }
        }
        List<NamedExpression> outputProjections = new ArrayList<>();
        outputProjections.add(new UnboundStar(ImmutableList.of()));
        if (!Util.showHiddenColumns()) {
            outputProjections.add((NamedExpression) rowIdExpr);
        }
        outputProjections.add(generateBranchLabel(rowIdExpr));
        plan = new LogicalProject<>(outputProjections, plan);

        plan = new LogicalFilter<>(ImmutableSet.of(new Not(new IsNull(new UnboundSlot(BRANCH_LABEL)))), plan);

        List<List<Expression>> finalProjections = new ArrayList<>();
        for (MergeMatchedClause clause : matchedClauses) {
            if (clause.isDelete()) {
                finalProjections.add(buildDeleteProjection(rowIdExpr, columns));
            } else {
                finalProjections.add(buildUpdateProjection(clause, rowIdExpr, columns, ctx));
            }
        }

        DataType rowIdType = DataType.fromCatalogType(IcebergRowId.getRowIdType());
        for (MergeNotMatchedClause clause : notMatchedClauses) {
            finalProjections.add(buildInsertProjection(clause, columns, ctx, rowIdType));
        }

        List<String> colNames = new ArrayList<>();
        colNames.add(IcebergMergeOperation.OPERATION_COLUMN);
        colNames.add(Column.ICEBERG_ROWID_COL);
        for (Column column : columns) {
            if (column.isVisible()) {
                colNames.add(column.getName());
            }
        }
        plan = new LogicalProject<>(generateFinalProjections(colNames, finalProjections), plan);

        if (cte.isPresent()) {
            plan = (LogicalPlan) cte.get().withChildren(plan);
        }
        return plan;
    }

    private LogicalPlan buildMergePlan(ConnectContext ctx, IcebergExternalTable icebergTable) {
        LogicalPlan projectPlan = buildMergeProjectPlan(ctx, icebergTable);

        List<NamedExpression> outputExprs;
        if (!IcebergNereidsUtils.hasUnboundPlan(projectPlan)) {
            outputExprs = projectPlan.getOutput().stream()
                    .map(NamedExpression.class::cast)
                    .collect(ImmutableList.toImmutableList());
        } else if (projectPlan instanceof LogicalProject) {
            outputExprs = ((LogicalProject<?>) projectPlan).getProjects();
        } else {
            outputExprs = ImmutableList.of();
        }

        return new LogicalIcebergMergeSink<>(
                (IcebergExternalDatabase) icebergTable.getDatabase(),
                icebergTable,
                icebergTable.getBaseSchema(true),
                outputExprs,
                deleteCtx,
                Optional.empty(),
                Optional.empty(),
                projectPlan);
    }

    private boolean executeMergePlan(ConnectContext ctx, StmtExecutor executor,
                                     IcebergExternalTable icebergTable,
                                     LogicalPlan logicalPlan) throws Exception {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalPlan, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.setPlanner(planner);
        executor.checkBlockRules();
        Optional<org.apache.iceberg.expressions.Expression> conflictFilter =
                IcebergConflictDetectionFilterUtils.buildConflictDetectionFilter(
                        planner.getAnalyzedPlan(), icebergTable);

        PhysicalSink<?> physicalSink = getPhysicalMergeSink(planner);
        PlanFragment fragment = planner.getFragments().get(0);
        DataSink dataSink = fragment.getSink();
        boolean emptyInsert = childIsEmptyRelation(physicalSink);
        String label = String.format("iceberg_merge_into_%x_%x", ctx.queryId().hi, ctx.queryId().lo);

        IcebergMergeExecutor insertExecutor =
                new IcebergMergeExecutor(ctx, icebergTable, label, planner, emptyInsert, -1L);
        insertExecutor.setConflictDetectionFilter(conflictFilter);

        if (insertExecutor.isEmptyInsert()) {
            return true;
        }

        insertExecutor.beginTransaction();
        insertExecutor.finalizeSinkForMerge(fragment, dataSink, physicalSink);
        insertExecutor.getCoordinator().setTxnId(insertExecutor.getTxnId());
        executor.setCoord(insertExecutor.getCoordinator());
        insertExecutor.executeSingleInsert(executor);
        return ctx.getState().getStateType() != QueryState.MysqlStateType.ERR;
    }

    private PhysicalSink<?> getPhysicalMergeSink(NereidsPlanner planner) {
        Optional<PhysicalSink<?>> plan = planner.getPhysicalPlan()
                .<PhysicalSink<?>>collect(PhysicalSink.class::isInstance).stream().findAny();
        if (!plan.isPresent()) {
            throw new AnalysisException("MERGE INTO command must contain target table");
        }
        PhysicalSink<?> sink = plan.get();
        if (!(sink instanceof PhysicalIcebergMergeSink)) {
            throw new AnalysisException("MERGE INTO plan must use Iceberg merge sink");
        }
        return sink;
    }

    private boolean childIsEmptyRelation(PhysicalSink<?> sink) {
        return sink.children() != null && sink.children().size() == 1
                && sink.child(0) instanceof PhysicalEmptyRelation;
    }

    private LogicalPlan injectRowIdColumn(LogicalPlan plan, IcebergExternalTable targetTable) {
        if (IcebergNereidsUtils.hasUnboundPlan(plan)) {
            return plan;
        }
        return IcebergNereidsUtils.injectRowIdColumn(plan, targetTable);
    }

    private Expression getTargetRowIdSlot() {
        return new UnboundSlot(Column.ICEBERG_ROWID_COL);
    }

    private static Column getRowIdColumn(IcebergExternalTable table) {
        return IcebergNereidsUtils.getRowIdColumn(table);
    }

}
