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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
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
        // Collect table relation map which is used to identify self join
        List<Object> catalogRelationObjs = materializedViewPlan.collectToList(
                planTreeNode -> planTreeNode instanceof CatalogRelation);
        ImmutableMultimap.Builder<TableIdentifier, CatalogRelation> tableCatalogRelationMultimapBuilder =
                ImmutableMultimap.builder();
        for (Object catalogRelationObj : catalogRelationObjs) {
            CatalogRelation catalogRelation = (CatalogRelation) catalogRelationObj;
            tableCatalogRelationMultimapBuilder.put(new TableIdentifier(catalogRelation.getTable()),
                    catalogRelation);
        }
        // Check sql pattern
        IncrementCheckerContext checkContext =
                new IncrementCheckerContext(columnSlot, tableCatalogRelationMultimapBuilder.build());
        materializedViewPlan.accept(MaterializedViewIncrementChecker.INSTANCE, checkContext);
        Multimap<TableIf, Column> partitionRelatedTableAndColumnMap =
                checkContext.getPartitionRelatedTableAndColumnMap();
        if (partitionRelatedTableAndColumnMap.isEmpty()) {
            return Optional.empty();
        }
        // TODO support to return only one related table info, support multi later
        for (Map.Entry<TableIf, Column> entry : partitionRelatedTableAndColumnMap.entries()) {
            return Optional.of(new RelatedTableInfo(new BaseTableInfo(entry.getKey()), true,
                    entry.getValue().getName()));
        }
        return Optional.empty();
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
        LogicalOlapScan mvScan = new LogicalOlapScan(
                cascadesContext.getStatementContext().getNextRelationId(),
                materializedView,
                materializedView.getFullQualifiers(),
                ImmutableList.of(),
                materializedView.getBaseIndexId(),
                PreAggStatus.on(),
                // this must be empty, or it will be used to sample
                ImmutableList.of(),
                Optional.empty());
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
            return null;
        }

        @Override
        public Void visitLogicalRelation(LogicalRelation relation, IncrementCheckerContext context) {
            if (!(relation instanceof LogicalCatalogRelation)) {
                return null;
            }
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            // if self join, self join can not partition track now, remove the partition column correspondingly
            if (context.getRelationByTable(table).size() > 1) {
                context.getPartitionRelatedTableAndColumnMap().removeAll(table);
                return null;
            }
            // TODO: 2024/1/31 support only one partition referenced column, support multi later
            if (!context.getPartitionRelatedTableAndColumnMap().isEmpty()) {
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
            if (partitionColumnSet.contains(mvReferenceColumn)
                    && (!mvReferenceColumn.isAllowNull() || relatedTable.isPartitionColumnAllowNull())) {
                context.addTableColumn(table, mvReferenceColumn);
            }
            return visit(relation, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                IncrementCheckerContext context) {
            Set<Expression> groupByExprSet = new HashSet<>(aggregate.getGroupByExpressions());
            if (groupByExprSet.isEmpty()) {
                return null;
            }
            Set<Column> originalGroupbyExprSet = new HashSet<>();
            groupByExprSet.forEach(groupExpr -> {
                if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                    originalGroupbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                }
            });
            if (!originalGroupbyExprSet.contains(context.getMvPartitionColumn().getColumn().get())) {
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
                if (!originalPartitionbyExprSet.contains(context.getMvPartitionColumn().getColumn().get())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class IncrementCheckerContext {
        private final SlotReference mvPartitionColumn;
        private final Multimap<TableIdentifier, CatalogRelation> tableAndCatalogRelationMap;
        private final Multimap<TableIf, Column> partitionRelatedTableAndColumnMap = HashMultimap.create();

        public IncrementCheckerContext(SlotReference mvPartitionColumn,
                Multimap<TableIdentifier, CatalogRelation> tableAndCatalogRelationMap) {
            this.mvPartitionColumn = mvPartitionColumn;
            this.tableAndCatalogRelationMap = tableAndCatalogRelationMap;
        }

        public SlotReference getMvPartitionColumn() {
            return mvPartitionColumn;
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
