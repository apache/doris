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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
        if (context.getRelatedTable() == null
                || context.getRelatedTableColumn() == null
                || !context.isPctPossible()) {
            return Optional.empty();
        }
        return Optional.of(new RelatedTableInfo(new BaseTableInfo(context.getRelatedTable()),
                context.isPctPossible(),
                context.getRelatedTableColumn().getName()));
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
            if (!(relation instanceof LogicalCatalogRelation) || context.getRelatedTable() != null) {
                return visit(relation, context);
            }
            LogicalCatalogRelation logicalCatalogRelation = (LogicalCatalogRelation) relation;
            TableIf table = logicalCatalogRelation.getTable();
            if (!(table instanceof OlapTable)) {
                return visit(relation, context);
            }
            OlapTable olapTable = (OlapTable) table;
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            Set<Column> partitionColumnSet = new HashSet<>(partitionInfo.getPartitionColumns());
            if (PartitionType.UNPARTITIONED.equals(partitionInfo.getType())) {
                return visit(relation, context);
            }
            Column mvReferenceColumn = context.getMvPartitionColumn().getColumn().get();
            if (partitionColumnSet.contains(mvReferenceColumn)) {
                context.setRelatedTable(table);
                context.setRelatedTableColumn(mvReferenceColumn);
                context.setPctPossible(!mvReferenceColumn.isAllowNull());
            }
            return visit(relation, context);
        }

        @Override
        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                IncrementCheckerContext context) {
            Set<Expression> groupByExprSet = new HashSet<>(aggregate.getGroupByExpressions());
            if (groupByExprSet.isEmpty()) {
                return visit(aggregate, context);
            }
            Set<Column> originalGroupbyExprSet = new HashSet<>();
            groupByExprSet.forEach(groupExpr -> {
                if (groupExpr instanceof SlotReference && groupExpr.isColumnFromTable()) {
                    originalGroupbyExprSet.add(((SlotReference) groupExpr).getColumn().get());
                }
            });
            if (!originalGroupbyExprSet.contains(context.getMvPartitionColumn().getColumn().get())) {
                context.setPctPossible(false);
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
        private TableIf relatedTable;
        private Column relatedTableColumn;
        private boolean joinNullGenerateSide;

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

        public TableIf getRelatedTable() {
            return relatedTable;
        }

        public void setRelatedTable(TableIf relatedTable) {
            this.relatedTable = relatedTable;
        }

        public Column getRelatedTableColumn() {
            return relatedTableColumn;
        }

        public void setRelatedTableColumn(Column relatedTableColumn) {
            this.relatedTableColumn = relatedTableColumn;
        }

        public boolean isJoinNullGenerateSide() {
            return joinNullGenerateSide;
        }

        public void setJoinNullGenerateSide(boolean joinNullGenerateSide) {
            this.joinNullGenerateSide = joinNullGenerateSide;
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
