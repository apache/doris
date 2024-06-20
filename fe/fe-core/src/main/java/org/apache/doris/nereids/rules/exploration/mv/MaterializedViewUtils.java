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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The common util for materialized view
 */
public class MaterializedViewUtils {

    /**
     * Get related base table info which materialized view plan column reference,
     * input param plan should be rewritten plan that sub query should be eliminated
     *
     * @param materializedViewPlan this should be rewritten or analyzed plan, should not be physical plan.
     * @param column ref column name.
     */
    public static RelatedTableInfo getRelatedTableInfo(String column, String timeUnit,
            Plan materializedViewPlan, CascadesContext cascadesContext) {

        List<Slot> outputExpressions = materializedViewPlan.getOutput();
        NamedExpression columnExpr = null;
        // get column slot
        for (Slot outputSlot : outputExpressions) {
            if (outputSlot.getName().equalsIgnoreCase(column)) {
                columnExpr = outputSlot;
                break;
            }
        }
        if (columnExpr == null) {
            return RelatedTableInfo.failWith("partition column can not find from sql select column");
        }
        if (timeUnit != null) {
            Expression dateTrunc = new DateTrunc(columnExpr, new VarcharLiteral(timeUnit));
            columnExpr = new Alias(dateTrunc);
            materializedViewPlan = new LogicalProject<>(ImmutableList.of(columnExpr), materializedViewPlan);
        }
        // Collect table relation map which is used to identify self join
        List<CatalogRelation> catalogRelationObjs =
                materializedViewPlan.collectToList(CatalogRelation.class::isInstance);
        ImmutableMultimap.Builder<TableIdentifier, CatalogRelation> tableCatalogRelationMultimapBuilder =
                ImmutableMultimap.builder();
        for (CatalogRelation catalogRelation : catalogRelationObjs) {
            tableCatalogRelationMultimapBuilder.put(new TableIdentifier(catalogRelation.getTable()), catalogRelation);
        }
        // Check sql pattern
        IncrementCheckerContext checkContext =
                new IncrementCheckerContext(columnExpr, tableCatalogRelationMultimapBuilder.build(), cascadesContext);
        materializedViewPlan.accept(MaterializedViewIncrementChecker.INSTANCE, checkContext);
        Multimap<TableIf, Column> partitionRelatedTableAndColumnMap =
                checkContext.getPartitionRelatedTableAndColumnMap();
        if (partitionRelatedTableAndColumnMap.isEmpty()) {
            return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                            String.join(",", checkContext.getFailReasons())));
        }
        // TODO support to return only one related table info, support multi later
        for (Map.Entry<TableIf, Column> entry : partitionRelatedTableAndColumnMap.entries()) {
            return RelatedTableInfo.successWith(new BaseTableInfo(entry.getKey()), true,
                    entry.getValue().getName(), checkContext.getPartitionExpression().orElseGet(() -> null));
        }
        return RelatedTableInfo.failWith("can't not find valid partition track column finally");
    }

    /**
     * This method check the select query plan is contain the stmt as following or not
     * <p>
     * SELECT
     * [hint_statement, ...]
     * [ALL | DISTINCT | DISTINCTROW | ALL EXCEPT ( col_name1 [, col_name2, col_name3, ...] )]
     * select_expr [, select_expr ...]
     * [FROM table_references
     * [PARTITION partition_list]
     * [TABLET tabletid_list]
     * [TABLESAMPLE sample_value [ROWS | PERCENT]
     * [REPEATABLE pos_seek]]
     * [WHERE where_condition]
     * [GROUP BY [GROUPING SETS | ROLLUP | CUBE] {col_name | expr | position}]
     * [HAVING where_condition]
     * [ORDER BY {col_name | expr | position}
     * [ASC | DESC], ...]
     * [LIMIT {[offset,] row_count | row_count OFFSET offset}]
     * [INTO OUTFILE 'file_name']
     * <p>
     * if analyzedPlan contains the stmt as following:
     * [PARTITION partition_list]
     * [TABLET tabletid_list] or
     * [TABLESAMPLE sample_value [ROWS | PERCENT]
     * *         [REPEATABLE pos_seek]]
     * this method will return true.
     */
    public static boolean containTableQueryOperator(Plan analyzedPlan) {
        return analyzedPlan.accept(TableQueryOperatorChecker.INSTANCE, null);
    }

    /**
     * Extract struct info from plan, support to get struct info from logical plan or plan in group.
     */
    public static List<StructInfo> extractStructInfo(Plan plan, CascadesContext cascadesContext,
            BitSet materializedViewTableSet) {
        // If plan belong to some group, construct it with group struct info
        if (plan.getGroupExpression().isPresent()) {
            Group ownerGroup = plan.getGroupExpression().get().getOwnerGroup();
            StructInfoMap structInfoMap = ownerGroup.getstructInfoMap();
            // Refresh struct info in current level plan from top to bottom
            structInfoMap.refresh(ownerGroup, cascadesContext);
            structInfoMap.setRefreshVersion(cascadesContext.getMemo().getRefreshVersion());

            Set<BitSet> queryTableSets = structInfoMap.getTableMaps();
            ImmutableList.Builder<StructInfo> structInfosBuilder = ImmutableList.builder();
            if (!queryTableSets.isEmpty()) {
                for (BitSet queryTableSet : queryTableSets) {
                    // TODO As only support MatchMode.COMPLETE, so only get equaled query table struct info
                    if (!materializedViewTableSet.isEmpty()
                            && !materializedViewTableSet.equals(queryTableSet)) {
                        continue;
                    }
                    StructInfo structInfo = structInfoMap.getStructInfo(cascadesContext,
                            queryTableSet, ownerGroup, plan);
                    if (structInfo != null) {
                        structInfosBuilder.add(structInfo);
                    }
                }
                return structInfosBuilder.build();
            }
        }
        // if plan doesn't belong to any group, construct it directly
        return ImmutableList.of(StructInfo.of(plan, cascadesContext));
    }

    /**
     * Generate scan plan for materialized view
     * if MaterializationContext is already rewritten by materialized view, then should generate in real time
     * when query rewrite, because one plan may hit the materialized view repeatedly and the mv scan output
     * should be different
     */
    public static Plan generateMvScanPlan(MTMV materializedView, CascadesContext cascadesContext) {
        return new LogicalOlapScan(
                cascadesContext.getStatementContext().getNextRelationId(),
                materializedView,
                materializedView.getFullQualifiers(),
                ImmutableList.of(),
                materializedView.getBaseIndexId(),
                PreAggStatus.on(),
                // this must be empty, or it will be used to sample
                ImmutableList.of(),
                Optional.empty());
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext,
            Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan) {
        if (originPlan == null || rewrittenPlan == null) {
            return null;
        }
        if (originPlan.getOutputSet().size() != rewrittenPlan.getOutputSet().size()) {
            return rewrittenPlan;
        }
        // After RBO, slot order may change, so need originSlotToRewrittenExprId which record
        // origin plan slot order
        List<ExprId> originalRewrittenPlanExprIds =
                rewrittenPlan.getOutput().stream().map(Slot::getExprId).collect(Collectors.toList());
        // run rbo job on mv rewritten plan
        CascadesContext rewrittenPlanContext = CascadesContext.initContext(
                cascadesContext.getStatementContext(), rewrittenPlan,
                cascadesContext.getCurrentJobContext().getRequiredProperties());
        rewrittenPlan = planRewriter.apply(rewrittenPlanContext);
        Map<ExprId, Slot> exprIdToNewRewrittenSlot = Maps.newLinkedHashMap();
        for (Slot slot : rewrittenPlan.getOutput()) {
            exprIdToNewRewrittenSlot.put(slot.getExprId(), slot);
        }
        List<ExprId> rewrittenPlanExprIds = rewrittenPlan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toList());
        // If project order doesn't change, return rewrittenPlan directly
        if (originalRewrittenPlanExprIds.equals(rewrittenPlanExprIds)) {
            return rewrittenPlan;
        }
        // If project order change, return rewrittenPlan with reordered projects
        return new LogicalProject<>(originalRewrittenPlanExprIds.stream()
                .map(exprId -> (NamedExpression) exprIdToNewRewrittenSlot.get(exprId)).collect(Collectors.toList()),
                rewrittenPlan);
    }

    private static final class TableQueryOperatorChecker extends DefaultPlanVisitor<Boolean, Void> {
        public static final TableQueryOperatorChecker INSTANCE = new TableQueryOperatorChecker();

        @Override
        public Boolean visitLogicalRelation(LogicalRelation relation, Void context) {
            if (relation instanceof LogicalFileScan && ((LogicalFileScan) relation).getTableSample().isPresent()) {
                return true;
            }
            if (relation instanceof LogicalOlapScan) {
                LogicalOlapScan logicalOlapScan = (LogicalOlapScan) relation;
                if (logicalOlapScan.getTableSample().isPresent()) {
                    return true;
                }
                if (!logicalOlapScan.getSelectedTabletIds().isEmpty()) {
                    return true;
                }
                if (!logicalOlapScan.getManuallySpecifiedPartitions().isEmpty()) {
                    return true;
                }
            }
            return visit(relation, context);
        }

        @Override
        public Boolean visit(Plan plan, Void context) {
            for (Plan child : plan.children()) {
                Boolean checkResult = child.accept(this, context);
                if (checkResult) {
                    return checkResult;
                }
            }
            return false;
        }
    }

    private static final class MaterializedViewIncrementChecker extends
            DefaultPlanVisitor<Void, IncrementCheckerContext> {

        public static final MaterializedViewIncrementChecker INSTANCE = new MaterializedViewIncrementChecker();
        public static final Set<Class<? extends Expression>> SUPPORT_EXPRESSION_TYPES =
                ImmutableSet.of(DateTrunc.class, SlotReference.class, Literal.class);

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project, IncrementCheckerContext context) {
            NamedExpression mvPartitionColumn = context.getMvPartitionColumn();
            List<Slot> output = project.getOutput();
            if (context.getMvPartitionColumn().isColumnFromTable()) {
                return visit(project, context);
            }
            for (Slot projectSlot : output) {
                if (!projectSlot.equals(mvPartitionColumn.toSlot())) {
                    continue;
                }
                if (projectSlot.isColumnFromTable()) {
                    context.setMvPartitionColumn(projectSlot);
                } else {
                    // should be only use date_trunc
                    Expression shuttledExpression =
                            ExpressionUtils.shuttleExpressionWithLineage(projectSlot, project, new BitSet());
                    // merge date_trunc
                    shuttledExpression = new ExpressionNormalization().rewrite(shuttledExpression,
                            new ExpressionRewriteContext(context.getCascadesContext()));

                    List<Expression> expressions = shuttledExpression.collectToList(Expression.class::isInstance);
                    for (Expression expression : expressions) {
                        if (SUPPORT_EXPRESSION_TYPES.stream().noneMatch(
                                supportExpression -> supportExpression.isAssignableFrom(expression.getClass()))) {
                            context.addFailReason(
                                    String.format("partition column use invalid implicit expression, invalid "
                                                    + "expression is %s", expression));
                            return null;
                        }
                    }
                    List<DateTrunc> dataTruncExpressions =
                            shuttledExpression.collectToList(DateTrunc.class::isInstance);
                    if (dataTruncExpressions.size() != 1) {
                        // mv time unit level is little then query
                        context.addFailReason("partition column time unit level should be "
                                + "greater than sql select column");
                        return null;
                    }
                    Optional<Slot> columnExpr =
                            shuttledExpression.getArgument(0).collectFirst(Slot.class::isInstance);
                    if (!columnExpr.isPresent() || !columnExpr.get().isColumnFromTable()) {
                        context.addFailReason(String.format("partition reference column should be direct column "
                                + "rather then expression except date_trunc, columnExpr is %s", columnExpr));
                        return null;
                    }
                    context.setPartitionExpression(shuttledExpression);
                    context.setMvPartitionColumn(columnExpr.get());
                }
                return visit(project, context);
            }
            context.addFailReason(String.format("partition reference column should be direct column "
                    + "rather then expression except date_trunc, current project is %s", project));
            return null;
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, IncrementCheckerContext context) {
            return visit(filter, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                IncrementCheckerContext context) {
            if (join.isMarkJoin()) {
                context.addFailReason("partition track doesn't support mark join");
                return null;
            }
            Plan left = join.child(0);
            Set<Column> leftColumnSet = left.getOutputSet().stream()
                    .filter(slot -> slot instanceof SlotReference
                            && slot.isColumnFromTable())
                    .map(slot -> ((SlotReference) slot).getColumn().get())
                    .collect(Collectors.toSet());
            SlotReference contextPartitionColumn = getContextPartitionColumn(context);
            if (contextPartitionColumn == null) {
                return null;
            }
            boolean useLeft = leftColumnSet.contains(contextPartitionColumn.getColumn().get());
            JoinType joinType = join.getJoinType();
            if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
                return visit(join, context);
            } else if ((joinType.isLeftJoin()
                    || joinType.isLefSemiJoin()
                    || joinType.isLeftAntiJoin()) && useLeft) {
                return visit(join.left(), context);
            } else if ((joinType.isRightJoin()
                    || joinType.isRightAntiJoin()
                    || joinType.isRightSemiJoin()) && !useLeft) {
                return visit(join.right(), context);
            }
            context.addFailReason(String.format("partition column is in un supported join null generate side, "
                    + "current join type is %s", joinType));
            return null;
        }

        @Override
        public Void visitLogicalRelation(LogicalRelation relation, IncrementCheckerContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                context.addFailReason(String.format("relation should be LogicalCatalogRelation, "
                        + "but now is %s", relation.getClass().getSimpleName()));
                return null;
            }
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            // if self join, self join can not partition track now, remove the partition column correspondingly
            if (context.getRelationByTable(table).size() > 1) {
                context.getPartitionRelatedTableAndColumnMap().removeAll(table);
                context.addFailReason(String.format("self join doesn't support partition update, "
                        + "self join table name is %s", table.getName()));
                return null;
            }
            // TODO: 2024/1/31 support only one partition referenced column, support multi later
            if (!context.getPartitionRelatedTableAndColumnMap().isEmpty()) {
                context.addFailReason(String.format("partition track already has an related base table column,"
                        + "track info is %s", context.getPartitionRelatedTableAndColumnMap()));
                return null;
            }
            if (!(table instanceof MTMVRelatedTableIf)) {
                context.addFailReason(String.format("relation base table is not MTMVRelatedTableIf, the table is %s",
                        table.getName()));
                return null;
            }
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            PartitionType type = relatedTable.getPartitionType();
            if (PartitionType.UNPARTITIONED.equals(type)) {
                context.addFailReason(String.format("related base table is not partition table, the table is %s",
                        table.getName()));
                return null;
            }
            Set<Column> partitionColumnSet = new HashSet<>(relatedTable.getPartitionColumns());
            SlotReference contextPartitionColumn = getContextPartitionColumn(context);
            if (contextPartitionColumn == null) {
                return null;
            }
            Column mvReferenceColumn = contextPartitionColumn.getColumn().get();
            if (partitionColumnSet.contains(mvReferenceColumn)
                    && (!mvReferenceColumn.isAllowNull() || relatedTable.isPartitionColumnAllowNull())) {
                context.addTableColumn(table, mvReferenceColumn);
            } else {
                context.addFailReason(String.format("related base table partition column doesn't contain the mv"
                                + " partition or partition nullable check fail, the mvReferenceColumn is %s",
                        mvReferenceColumn));
            }
            return visit(relation, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                IncrementCheckerContext context) {
            Set<Expression> groupByExprSet = new HashSet<>(aggregate.getGroupByExpressions());
            if (groupByExprSet.isEmpty()) {
                context.addFailReason("group by sets is empty, doesn't contain the target partition");
                return null;
            }
            Set<Column> originalGroupbyExprSet = new HashSet<>();
            groupByExprSet.forEach(groupExpr -> {
                if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                    originalGroupbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                }
            });
            SlotReference contextPartitionColumn = getContextPartitionColumn(context);
            if (contextPartitionColumn == null) {
                return null;
            }
            if (!originalGroupbyExprSet.contains(contextPartitionColumn.getColumn().get())) {
                context.addFailReason("group by sets doesn't contain the target partition");
                return null;
            }
            return visit(aggregate, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, IncrementCheckerContext context) {
            List<NamedExpression> windowExpressions = window.getWindowExpressions();
            if (windowExpressions.isEmpty()) {
                return visit(window, context);
            }
            for (NamedExpression namedExpression : windowExpressions) {
                if (!checkWindowPartition(namedExpression, context)) {
                    context.addFailReason("window partition sets doesn't contain the target partition");
                    return null;
                }
            }
            return super.visitLogicalWindow(window, context);
        }

        @Override
        public Void visit(Plan plan, IncrementCheckerContext context) {
            if (plan instanceof LogicalProject
                    || plan instanceof LogicalFilter
                    || plan instanceof LogicalJoin
                    || plan instanceof LogicalAggregate
                    || plan instanceof LogicalCatalogRelation
                    || plan instanceof LogicalResultSink
                    || plan instanceof LogicalWindow) {
                return super.visit(plan, context);
            }
            return null;
        }

        private boolean checkWindowPartition(Expression expression, IncrementCheckerContext context) {
            List<Object> windowExpressions =
                    expression.collectToList(expressionTreeNode -> expressionTreeNode instanceof WindowExpression);
            for (Object windowExpressionObj : windowExpressions) {
                WindowExpression windowExpression = (WindowExpression) windowExpressionObj;
                List<Expression> partitionKeys = windowExpression.getPartitionKeys();
                Set<Column> originalPartitionbyExprSet = new HashSet<>();
                partitionKeys.forEach(groupExpr -> {
                    if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                        originalPartitionbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                    }
                });
                SlotReference contextPartitionColumn = getContextPartitionColumn(context);
                if (contextPartitionColumn == null) {
                    return false;
                }
                if (!originalPartitionbyExprSet.contains(contextPartitionColumn.getColumn().get())) {
                    return false;
                }
            }
            return true;
        }

        private SlotReference getContextPartitionColumn(IncrementCheckerContext context) {
            if (!context.getMvPartitionColumn().isColumnFromTable()) {
                context.addFailReason(String.format("context partition column should be slot from column, "
                                + "context column is %s",
                        context.getMvPartitionColumn()));
                return null;
            }
            return (SlotReference) context.getMvPartitionColumn();
        }
    }

    private static final class IncrementCheckerContext {
        private NamedExpression mvPartitionColumn;
        private Optional<Expression> partitionExpression = Optional.empty();
        private final Multimap<TableIdentifier, CatalogRelation> tableAndCatalogRelationMap;
        private final Multimap<TableIf, Column> partitionRelatedTableAndColumnMap = HashMultimap.create();
        private final Set<String> failReasons = new HashSet<>();
        private final CascadesContext cascadesContext;

        public IncrementCheckerContext(NamedExpression mvPartitionColumn,
                Multimap<TableIdentifier, CatalogRelation> tableAndCatalogRelationMap,
                CascadesContext cascadesContext) {
            this.mvPartitionColumn = mvPartitionColumn;
            this.tableAndCatalogRelationMap = tableAndCatalogRelationMap;
            this.cascadesContext = cascadesContext;
        }

        public NamedExpression getMvPartitionColumn() {
            return mvPartitionColumn;
        }

        public void setMvPartitionColumn(NamedExpression mvPartitionColumn) {
            this.mvPartitionColumn = mvPartitionColumn;
        }

        public void addTableColumn(TableIf relatedTable, Column partitionColumn) {
            partitionRelatedTableAndColumnMap.put(relatedTable, partitionColumn);
        }

        public Multimap<TableIf, Column> getPartitionRelatedTableAndColumnMap() {
            return partitionRelatedTableAndColumnMap;
        }

        public Collection<CatalogRelation> getRelationByTable(TableIf tableIf) {
            return tableAndCatalogRelationMap.get(new TableIdentifier(tableIf));
        }

        public void addTableAndRelation(TableIf tableIf, CatalogRelation relation) {
            tableAndCatalogRelationMap.put(new TableIdentifier(tableIf), relation);
        }

        public Set<String> getFailReasons() {
            return failReasons;
        }

        public void addFailReason(String failReason) {
            this.failReasons.add(failReason);
        }

        public CascadesContext getCascadesContext() {
            return cascadesContext;
        }

        public Optional<Expression> getPartitionExpression() {
            return partitionExpression;
        }

        public void setPartitionExpression(Expression partitionExpression) {
            this.partitionExpression = Optional.ofNullable(partitionExpression);
        }
    }

    /**
     * The related table info that mv relate
     */
    public static final class RelatedTableInfo {
        private final BaseTableInfo tableInfo;
        private final boolean pctPossible;
        private final String column;
        private final Set<String> failReasons = new HashSet<>();
        // This records the partition expression if exist
        private Optional<Expression> partitionExpression;

        public RelatedTableInfo(BaseTableInfo tableInfo, boolean pctPossible, String column, String failReason,
                Expression partitionExpression) {
            this.tableInfo = tableInfo;
            this.pctPossible = pctPossible;
            this.column = column;
            this.failReasons.add(failReason);
            this.partitionExpression = Optional.ofNullable(partitionExpression);
        }

        public static RelatedTableInfo failWith(String failReason) {
            return new RelatedTableInfo(null, false, null, failReason,
                    null);
        }

        public static RelatedTableInfo successWith(BaseTableInfo tableInfo, boolean pctPossible, String column,
                Expression partitionExpression) {
            return new RelatedTableInfo(tableInfo, pctPossible, column, "", partitionExpression);
        }

        public BaseTableInfo getTableInfo() {
            return tableInfo;
        }

        public boolean isPctPossible() {
            return pctPossible;
        }

        public String getColumn() {
            return column;
        }

        public void addFailReason(String failReason) {
            this.failReasons.add(failReason);
        }

        public String getFailReason() {
            return String.join(",", failReasons);
        }

        public Optional<Expression> getPartitionExpression() {
            return partitionExpression;
        }
    }
}
