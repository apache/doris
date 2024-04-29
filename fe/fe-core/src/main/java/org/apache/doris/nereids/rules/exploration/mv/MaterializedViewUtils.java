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
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.BitSet;
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
    public static Optional<RelatedTableInfo> getRelatedTableInfo(String column, Plan materializedViewPlan) {
        List<Slot> outputExpressions = materializedViewPlan.getOutput();
        Slot columnExpr = null;
        // get column slot
        for (Slot outputSlot : outputExpressions) {
            if (outputSlot.getName().equalsIgnoreCase(column)) {
                columnExpr = outputSlot;
                break;
            }
        }
        if (columnExpr == null) {
            return Optional.empty();
        }
        if (!(columnExpr instanceof SlotReference)) {
            return Optional.empty();
        }
        SlotReference columnSlot = (SlotReference) columnExpr;
        if (!columnSlot.isColumnFromTable()) {
            return Optional.empty();
        }
        // check sql pattern
        IncrementCheckerContext context = new IncrementCheckerContext(columnSlot);
        materializedViewPlan.accept(MaterializedViewIncrementChecker.INSTANCE, context);
        if (context.getPartitionRelatedTableAndColumnList().isEmpty() || !context.isPctPossible()) {
            return Optional.empty();
        }
        // TODO support to return only one related table info, support multi later
        Pair<TableIf, Column> tableIfColumnPair = context.getPartitionRelatedTableAndColumnList().get(0);
        return Optional.of(new RelatedTableInfo(new BaseTableInfo(tableIfColumnPair.key()),
                context.isPctPossible(),
                tableIfColumnPair.value().getName()));
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
            structInfoMap.refresh(ownerGroup, cascadesContext.getMemo().getRefreshVersion());
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
                    StructInfo structInfo = structInfoMap.getStructInfo(cascadesContext.getMemo(),
                            queryTableSet, ownerGroup, plan);
                    if (structInfo != null) {
                        structInfosBuilder.add(structInfo);
                    }
                }
                return structInfosBuilder.build();
            }
        }
        // if plan doesn't belong to any group, construct it directly
        return ImmutableList.of(StructInfo.of(plan));
    }

    /**
     * Generate scan plan for materialized view
     * if MaterializationContext is already rewritten by materialized view, then should generate in real time
     * when query rewrite, because one plan may hit the materialized view repeatedly and the mv scan output
     * should be different
     */
    public static Plan generateMvScanPlan(MTMV materializedView, CascadesContext cascadesContext) {
        LogicalOlapScan mvScan = new LogicalOlapScan(
                cascadesContext.getStatementContext().getNextRelationId(),
                materializedView,
                ImmutableList.of(materializedView.getQualifiedDbName()),
                // this must be empty, or it will be used to sample
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
        mvScan = mvScan.withMaterializedIndexSelected(PreAggStatus.on(), materializedView.getBaseIndexId());
        List<NamedExpression> mvProjects = mvScan.getOutput().stream().map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        return new LogicalProject<Plan>(mvProjects, mvScan);
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext,
            Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan) {
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

        @Override
        public Void visitLogicalProject(LogicalProject<? extends Plan> project, IncrementCheckerContext context) {
            return visit(project, context);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, IncrementCheckerContext context) {
            return visit(filter, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                IncrementCheckerContext context) {
            if (join.isMarkJoin()) {
                context.setPctPossible(false);
                return null;
            }
            Plan left = join.child(0);
            Set<Column> leftColumnSet = left.getOutputSet().stream()
                    .filter(slot -> slot instanceof SlotReference
                            && slot.isColumnFromTable())
                    .map(slot -> ((SlotReference) slot).getColumn().get())
                    .collect(Collectors.toSet());
            boolean useLeft = leftColumnSet.contains(context.getMvPartitionColumn().getColumn().get());
            JoinType joinType = join.getJoinType();
            if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
                context.setPctPossible(true);
            } else if (joinType.isLeftJoin()
                    || joinType.isLefSemiJoin()
                    || joinType.isLeftAntiJoin()) {
                context.setPctPossible(useLeft);
            } else if (joinType.isRightJoin()
                    || joinType.isRightAntiJoin()
                    || joinType.isRightSemiJoin()) {
                context.setPctPossible(!useLeft);
            } else {
                // un supported join type
                context.setPctPossible(false);
            }
            return visit(join, context);
        }

        @Override
        public Void visitLogicalRelation(LogicalRelation relation, IncrementCheckerContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                return null;
            }
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            // if self join, can't infer partition column
            if (!context.getTableIdAndRelationMapping().get(table.getId()).isEmpty()) {
                context.setPctPossible(false);
                return null;
            }
            // record tableId and relation, to check the self join
            context.addTableIdAndRelation(((LogicalCatalogRelation) relation).getTable().getId(), relation);
            // TODO: 2024/1/31 support only one partition referenced column, support multi later
            if (!context.getPartitionRelatedTableAndColumnList().isEmpty()) {
                return null;
            }
            if (!(table instanceof MTMVRelatedTableIf)) {
                return null;
            }
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            PartitionType type = relatedTable.getPartitionType();
            if (PartitionType.UNPARTITIONED.equals(type)) {
                return null;
            }
            Set<Column> partitionColumnSet = new HashSet<>(relatedTable.getPartitionColumns());
            Column mvReferenceColumn = context.getMvPartitionColumn().getColumn().get();
            if (partitionColumnSet.contains(mvReferenceColumn)) {
                context.addTableColumn(table, mvReferenceColumn);
                context.setPctPossible(!mvReferenceColumn.isAllowNull() || relatedTable.isPartitionColumnAllowNull());
            }
            return visit(relation, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                IncrementCheckerContext context) {
            Set<Expression> groupByExprSet = new HashSet<>(aggregate.getGroupByExpressions());
            if (groupByExprSet.isEmpty()) {
                context.setPctPossible(false);
                return null;
            }
            Set<Column> originalGroupbyExprSet = new HashSet<>();
            groupByExprSet.forEach(groupExpr -> {
                if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                    originalGroupbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                }
            });
            if (!originalGroupbyExprSet.contains(context.getMvPartitionColumn().getColumn().get())) {
                context.setPctPossible(false);
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
            windowExpressions.forEach(expr -> checkWindowPartition(expr, context));
            return super.visitLogicalWindow(window, context);
        }

        @Override
        public Void visit(Plan plan, IncrementCheckerContext context) {
            if (!context.isPctPossible()) {
                return null;
            }
            if (plan instanceof LogicalProject
                    || plan instanceof LogicalFilter
                    || plan instanceof LogicalJoin
                    || plan instanceof LogicalAggregate
                    || plan instanceof LogicalCatalogRelation
                    || plan instanceof LogicalResultSink
                    || plan instanceof LogicalWindow) {
                return super.visit(plan, context);
            }
            context.setPctPossible(false);
            return null;
        }

        private void checkWindowPartition(Expression expression, IncrementCheckerContext context) {
            expression.collectToList(expressionTreeNode -> expressionTreeNode instanceof WindowExpression)
                    .forEach(windowObj -> {
                        WindowExpression windowExpression = (WindowExpression) windowObj;
                        List<Expression> partitionKeys = windowExpression.getPartitionKeys();
                        Set<Column> originalPartitionbyExprSet = new HashSet<>();
                        partitionKeys.forEach(groupExpr -> {
                            if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                                originalPartitionbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                            }
                        });
                        if (!originalPartitionbyExprSet.contains(context.getMvPartitionColumn().getColumn().get())) {
                            context.setPctPossible(false);
                        }
                    });
        }
    }

    private static final class IncrementCheckerContext {
        private final SlotReference mvPartitionColumn;
        private boolean pctPossible = true;
        private final List<Pair<TableIf, Column>> partitionRelatedTableAndColumnList = new ArrayList<>();
        // This record the table id and relation mapping, because a table maybe used repeatedly.
        private final Multimap<Long, LogicalRelation> tableIdAndRelationMapping = HashMultimap.create();

        public IncrementCheckerContext(SlotReference mvPartitionColumn) {
            this.mvPartitionColumn = mvPartitionColumn;
        }

        public SlotReference getMvPartitionColumn() {
            return mvPartitionColumn;
        }

        public boolean isPctPossible() {
            return pctPossible;
        }

        public void setPctPossible(boolean pctPossible) {
            this.pctPossible = pctPossible;
        }

        public void addTableColumn(TableIf relatedTable, Column partitionColumn) {
            partitionRelatedTableAndColumnList.add(Pair.of(relatedTable, partitionColumn));
        }

        public List<Pair<TableIf, Column>> getPartitionRelatedTableAndColumnList() {
            return partitionRelatedTableAndColumnList;
        }

        public Multimap<Long, LogicalRelation> getTableIdAndRelationMapping() {
            return tableIdAndRelationMapping;
        }

        public void addTableIdAndRelation(Long tableId, LogicalRelation relation) {
            tableIdAndRelationMapping.put(tableId, relation);
        }
    }

    /**
     * The related table info that mv relate
     */
    public static final class RelatedTableInfo {
        private BaseTableInfo tableInfo;
        private boolean pctPossible;
        private String column;

        public RelatedTableInfo(BaseTableInfo tableInfo, boolean pctPossible, String column) {
            this.tableInfo = tableInfo;
            this.pctPossible = pctPossible;
            this.column = column;
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
    }
}
