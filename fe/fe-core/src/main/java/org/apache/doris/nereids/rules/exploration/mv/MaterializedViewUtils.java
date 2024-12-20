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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
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
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
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
        List<CatalogRelation> catalogRelations = materializedViewPlan.collectToList(CatalogRelation.class::isInstance);
        ImmutableMultimap.Builder<TableIdentifier, CatalogRelation> tableCatalogRelationMultimapBuilder =
                ImmutableMultimap.builder();
        for (CatalogRelation catalogRelation : catalogRelations) {
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
     * @param plan maybe remove unnecessary plan node, and the logical output maybe wrong
     * @param originalPlan original plan, the output is right
     */
    public static List<StructInfo> extractStructInfo(Plan plan, Plan originalPlan, CascadesContext cascadesContext,
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
                            queryTableSet, ownerGroup, originalPlan);
                    if (structInfo != null) {
                        structInfosBuilder.add(structInfo);
                    }
                }
            }
            return structInfosBuilder.build();
        }
        // if plan doesn't belong to any group, construct it directly
        return ImmutableList.of(StructInfo.of(plan, originalPlan, cascadesContext));
    }

    /**
     * Generate scan plan for materialized view
     * if MaterializationContext is already rewritten by materialized view, then should generate in real time
     * when query rewrite, because one plan may hit the materialized view repeatedly and the mv scan output
     * should be different
     */
    public static Plan generateMvScanPlan(OlapTable table, long indexId,
            List<Long> partitionIds,
            PreAggStatus preAggStatus,
            CascadesContext cascadesContext) {
        LogicalOlapScan olapScan = new LogicalOlapScan(
                cascadesContext.getStatementContext().getNextRelationId(),
                table,
                ImmutableList.of(table.getQualifiedDbName()),
                ImmutableList.of(),
                partitionIds,
                indexId,
                preAggStatus,
                ImmutableList.of(),
                // this must be empty, or it will be used to sample
                ImmutableList.of(),
                Optional.empty());
        return BindRelation.checkAndAddDeleteSignFilter(olapScan, cascadesContext.getConnectContext(),
                olapScan.getTable());
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules, this method is only for materialized view rewrite
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
        // Tmp old disable rule variable
        Set<String> oldDisableRuleNames = rewrittenPlanContext.getStatementContext().getConnectContext()
                .getSessionVariable()
                .getDisableNereidsRuleNames();
        rewrittenPlanContext.getStatementContext().getConnectContext().getSessionVariable()
                .setDisableNereidsRules(String.join(",", ImmutableSet.of(RuleType.ADD_DEFAULT_LIMIT.name())));
        rewrittenPlanContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        try {
            rewrittenPlanContext.getConnectContext().setSkipAuth(true);
            rewrittenPlan = planRewriter.apply(rewrittenPlanContext);
        } finally {
            rewrittenPlanContext.getConnectContext().setSkipAuth(false);
            // Recover old disable rules variable
            rewrittenPlanContext.getStatementContext().getConnectContext().getSessionVariable()
                    .setDisableNereidsRules(String.join(",", oldDisableRuleNames));
            rewrittenPlanContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        }
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

    /**
     * Extract nondeterministic function form plan, if the function is in whiteExpressionSet,
     * the function would be considered as deterministic function and will not return
     * in the result expression result
     */
    public static List<Expression> extractNondeterministicFunction(Plan plan) {
        List<Expression> nondeterministicFunctions = new ArrayList<>();
        plan.accept(NondeterministicFunctionCollector.INSTANCE, nondeterministicFunctions);
        return nondeterministicFunctions;
    }

    /**
     * createMTMVCache from querySql
     */
    public static MTMVCache createMTMVCache(String querySql, ConnectContext connectContext) {
        LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(querySql);
        StatementContext mvSqlStatementContext = new StatementContext(connectContext,
                new OriginStatement(querySql, 0));
        mvSqlStatementContext.setNeedLockTables(false);
        NereidsPlanner planner = new NereidsPlanner(mvSqlStatementContext);
        if (mvSqlStatementContext.getConnectContext().getStatementContext() == null) {
            mvSqlStatementContext.getConnectContext().setStatementContext(mvSqlStatementContext);
        }
        // Can not convert to table sink, because use the same column from different table when self join
        // the out slot is wrong
        planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainCommand.ExplainLevel.ALL_PLAN);
        Plan originPlan = planner.getRewrittenPlan();
        // Eliminate result sink because sink operator is useless in query rewrite by materialized view
        // and the top sort can also be removed
        Plan mvPlan = originPlan.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, Object context) {
                return logicalResultSink.child().accept(this, context);
            }
        }, null);
        // Optimize by rules to remove top sort
        CascadesContext parentCascadesContext = CascadesContext.initContext(mvSqlStatementContext, mvPlan,
                PhysicalProperties.ANY);
        mvPlan = MaterializedViewUtils.rewriteByRules(parentCascadesContext, childContext -> {
            Rewriter.getCteChildrenRewriter(childContext,
                    ImmutableList.of(Rewriter.custom(RuleType.ELIMINATE_SORT, EliminateSort::new))).execute();
            return childContext.getRewritePlan();
        }, mvPlan, originPlan);
        return new MTMVCache(mvPlan, originPlan, planner.getAnalyzedPlan(),
                planner.getCascadesContext().getMemo().getRoot().getStatistics(), null);
    }

    /**
     * Check the query if Contains query operator
     * Such sql as following should return true
     * select * from orders TABLET(10098) because TABLET(10098) should return true
     * select * from orders_partition PARTITION (day_2) because PARTITION (day_2)
     * select * from orders index query_index_test because index query_index_test
     * select * from orders TABLESAMPLE(20 percent) because TABLESAMPLE(20 percent)
     * */
    public static final class TableQueryOperatorChecker extends DefaultPlanVisitor<Boolean, Void> {
        public static final TableQueryOperatorChecker INSTANCE = new TableQueryOperatorChecker();

        @Override
        public Boolean visitLogicalRelation(LogicalRelation relation, Void context) {
            if (relation instanceof LogicalFileScan && ((LogicalFileScan) relation).getTableSample().isPresent()) {
                return true;
            }
            if (relation instanceof LogicalOlapScan) {
                LogicalOlapScan logicalOlapScan = (LogicalOlapScan) relation;
                if (logicalOlapScan.getTableSample().isPresent()) {
                    // Contain sample, select * from orders TABLESAMPLE(20 percent)
                    return true;
                }
                if (!logicalOlapScan.getManuallySpecifiedTabletIds().isEmpty()) {
                    // Contain tablets, select * from orders TABLET(10098) because TABLET(10098)
                    return true;
                }
                if (!logicalOlapScan.getManuallySpecifiedPartitions().isEmpty()) {
                    // Contain specified partitions, select * from orders_partition PARTITION (day_2)
                    return true;
                }
                if (logicalOlapScan.getSelectedIndexId() != logicalOlapScan.getTable().getBaseIndexId()) {
                    // Contains select index or use sync mv in rbo rewrite
                    // select * from orders index query_index_test
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
            List<Slot> output = project.getOutput();
            boolean isValid = checkPartition(output, project, context);
            if (!isValid) {
                return null;
            }
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
                return join.left().accept(this, context);
            } else if ((joinType.isRightJoin()
                    || joinType.isRightAntiJoin()
                    || joinType.isRightSemiJoin()) && !useLeft) {
                return join.right().accept(this, context);
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
            SlotReference contextPartitionColumn = getContextPartitionColumn(context);
            if (contextPartitionColumn == null) {
                context.addFailReason(String.format("mv partition column is not from table when relation check, "
                        + "mv partition column is %s", context.getMvPartitionColumn()));
                return null;
            }
            // Check the table which mv partition column belonged to is same as the current check relation or not
            if (!((LogicalCatalogRelation) relation).getTable().getFullQualifiers().equals(
                    contextPartitionColumn.getTable().map(TableIf::getFullQualifiers).orElse(ImmutableList.of()))) {
                context.addFailReason(String.format("mv partition column name is not belonged to current check , "
                                + "table, current table is %s",
                        ((LogicalCatalogRelation) relation).getTable().getFullQualifiers()));
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
            PartitionType type = relatedTable.getPartitionType(Optional.empty());
            if (PartitionType.UNPARTITIONED.equals(type)) {
                context.addFailReason(String.format("related base table is not partition table, the table is %s",
                        table.getName()));
                return null;
            }
            Set<Column> partitionColumnSet = new HashSet<>(relatedTable.getPartitionColumns(Optional.empty()));
            Column mvReferenceColumn = contextPartitionColumn.getColumn().get();
            Expr definExpr = mvReferenceColumn.getDefineExpr();
            if (definExpr instanceof SlotRef) {
                Column referenceRollupColumn = ((SlotRef) definExpr).getColumn();
                if (referenceRollupColumn != null) {
                    mvReferenceColumn = referenceRollupColumn;
                }
            }
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
            boolean isValid = checkPartition(groupByExprSet, aggregate, context);
            if (!isValid) {
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
                    || plan instanceof LogicalLimit
                    || plan instanceof LogicalFilter
                    || plan instanceof LogicalJoin
                    || plan instanceof LogicalAggregate
                    || plan instanceof LogicalCatalogRelation
                    || plan instanceof LogicalResultSink
                    || plan instanceof LogicalWindow) {
                return super.visit(plan, context);
            }
            context.addFailReason(String.format("Unsupported plan operate in track partition, "
                    + "the invalid plan node is %s", plan.getClass().getSimpleName()));
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

        /**
         * Given a partition named expression and expressionsToCheck, check the partition is valid
         * example 1:
         * partition expression is date_trunc(date_alias#25, 'hour') AS `date_trunc(date_alias, 'hour')`#30
         * expressionsToCheck is date_trunc(date_alias, 'hour')#30
         * expressionsToCheck is the slot to partition expression, but they are expression
         * example 2:
         * partition expression is L_SHIPDATE#10
         * expressionsToCheck isL_SHIPDATE#10
         * both of them are slot
         * example 3:
         * partition expression is date_trunc(L_SHIPDATE#10, 'hour')#30
         * expressionsToCheck is L_SHIPDATE#10
         * all above should check successfully
         * */
        private static boolean checkPartition(Collection<? extends Expression> expressionsToCheck, Plan plan,
                IncrementCheckerContext context) {
            NamedExpression partitionColumn = context.getMvPartitionColumn();

            OUTER_CHECK: for (Expression projectSlot : expressionsToCheck) {
                if (projectSlot.isColumnFromTable() && projectSlot.equals(partitionColumn.toSlot())) {
                    continue;
                }
                // check the expression which use partition column
                Expression expressionToCheck =
                        ExpressionUtils.shuttleExpressionWithLineage(projectSlot, plan, new BitSet());
                // merge date_trunc
                expressionToCheck = new ExpressionNormalization().rewrite(expressionToCheck,
                        new ExpressionRewriteContext(context.getCascadesContext()));

                Expression partitionExpression = context.getPartitionExpression().isPresent()
                        ? context.getPartitionExpression().get() :
                        ExpressionUtils.shuttleExpressionWithLineage(partitionColumn, plan, new BitSet());
                // merge date_trunc
                partitionExpression = new ExpressionNormalization().rewrite(partitionExpression,
                        new ExpressionRewriteContext(context.getCascadesContext()));

                Set<SlotReference> expressionToCheckColumns =
                        expressionToCheck.collectToSet(SlotReference.class::isInstance);
                Set<SlotReference> partitionColumns =
                        partitionExpression.collectToSet(SlotReference.class::isInstance);
                if (Sets.intersection(expressionToCheckColumns, partitionColumns).isEmpty()
                        || expressionToCheckColumns.isEmpty() || partitionColumns.isEmpty()) {
                    // this expression doesn't use partition column
                    continue;
                }
                if (expressionToCheckColumns.size() > 1 || partitionColumns.size() > 1) {
                    context.addFailReason(
                            String.format("partition expression use more than one slot reference, invalid "
                                            + "expressionToCheckColumns is %s, partitionColumnDateColumns is %s",
                                    expressionToCheckColumns, partitionColumns));
                    continue;
                }
                List<Expression> expressions = expressionToCheck.collectToList(Expression.class::isInstance);
                for (Expression expression : expressions) {
                    if (SUPPORT_EXPRESSION_TYPES.stream().noneMatch(
                            supportExpression -> supportExpression.isAssignableFrom(expression.getClass()))) {
                        context.addFailReason(
                                String.format("column to check use invalid implicit expression, invalid "
                                        + "expression is %s", expression));
                        continue OUTER_CHECK;
                    }
                }
                List<Expression> partitionExpressions = partitionExpression.collectToList(
                        Expression.class::isInstance);
                for (Expression expression : partitionExpressions) {
                    if (SUPPORT_EXPRESSION_TYPES.stream().noneMatch(
                            supportExpression -> supportExpression.isAssignableFrom(expression.getClass()))) {
                        context.addFailReason(
                                String.format("partition column use invalid implicit expression, invalid "
                                        + "expression is %s", expression));
                        continue OUTER_CHECK;
                    }
                }
                List<DateTrunc> expressionToCheckDataTruncList =
                        expressionToCheck.collectToList(DateTrunc.class::isInstance);
                List<DateTrunc> partitionColumnDateTrucList =
                        partitionExpression.collectToList(DateTrunc.class::isInstance);
                if (expressionToCheckDataTruncList.size() > 1 || partitionColumnDateTrucList.size() > 1) {
                    // mv time unit level is little then query
                    context.addFailReason("partition column time unit level should be "
                            + "greater than sql select column");
                    continue;
                }
                if (!partitionColumn.isColumnFromTable()) {
                    context.setMvPartitionColumn(partitionColumns.iterator().next());
                }
                if (!context.getPartitionExpression().isPresent()) {
                    context.setPartitionExpression(partitionExpression);
                }
                return true;
            }
            return context.getMvPartitionColumn().isColumnFromTable();
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
        private final Optional<Expression> partitionExpression;

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
