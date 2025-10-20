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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementChecker;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.rules.rewrite.QueryPartitionCollector;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
    @Deprecated
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
        materializedViewPlan = PartitionIncrementMaintainer.removeSink(materializedViewPlan);
        Expression dateTrunc = null;
        if (timeUnit != null) {
            dateTrunc = new DateTrunc(columnExpr, new VarcharLiteral(timeUnit));
            columnExpr = new Alias(dateTrunc);
            materializedViewPlan = new LogicalProject<>(ImmutableList.of(columnExpr), materializedViewPlan);
        }
        // Check sql pattern
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, cascadesContext);
        checkContext.getPartitionAndRefExpressionMap().put(columnExpr,
                RelatedTableColumnInfo.of(columnExpr, null, true, false));
        materializedViewPlan.accept(PartitionIncrementChecker.INSTANCE, checkContext);
        List<RelatedTableColumnInfo> checkedTableColumnInfos =
                PartitionIncrementMaintainer.getRelatedTableColumnInfosWithCheck(checkContext, tableColumnInfo ->
                        tableColumnInfo.isOriginalPartition() && tableColumnInfo.isFromTablePartitionColumn());
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (!checkedTableColumnInfos.isEmpty()) {
            return RelatedTableInfo.successWith(checkedTableColumnInfos);
        }
        return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                String.join(",", checkContext.getFailReasons())));
    }

    /**
     * Get related base table info which materialized view plan column reference,
     * input param plan should be rewritten plan that sub query should be eliminated
     *
     * @param materializedViewPlan this should be rewritten or analyzed plan, should not be physical plan.
     * @param column ref column name.
     */
    public static RelatedTableInfo getRelatedTableInfos(String column, String timeUnit,
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
        materializedViewPlan = PartitionIncrementMaintainer.removeSink(materializedViewPlan);
        Expression dateTrunc = null;
        if (timeUnit != null) {
            dateTrunc = new DateTrunc(columnExpr, new VarcharLiteral(timeUnit));
            columnExpr = new Alias(dateTrunc);
            materializedViewPlan = new LogicalProject<>(ImmutableList.of(columnExpr), materializedViewPlan);
        }
        // Check sql pattern
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, cascadesContext);
        checkContext.getPartitionAndRefExpressionMap().put(columnExpr,
                RelatedTableColumnInfo.of(columnExpr, null, true, false));
        materializedViewPlan.accept(PartitionIncrementChecker.INSTANCE, checkContext);
        if (!checkPartitionRefExpression(checkContext.getPartitionAndRefExpressionMap().values())) {
            return RelatedTableInfo.failWith(String.format(
                    "partition ref expressions is not consistent, partition ref expressions map is %s",
                    checkContext.getPartitionAndRefExpressionMap()));
        }
        List<RelatedTableColumnInfo> checkedTableColumnInfos =
                PartitionIncrementMaintainer.getRelatedTableColumnInfosWithCheck(checkContext,
                        RelatedTableColumnInfo::isReachRelationCheck);
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (!checkedTableColumnInfos.isEmpty()) {
            return RelatedTableInfo.successWith(checkedTableColumnInfos);
        }
        return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                String.join(",", checkContext.getFailReasons())));
    }

    /**
     * Check the partition expression date_trunc num is valid or not
     */
    private static boolean checkPartitionRefExpression(Collection<RelatedTableColumnInfo> refExpressions) {
        for (RelatedTableColumnInfo tableColumnInfo : refExpressions) {
            if (tableColumnInfo.getPartitionExpression().isPresent()) {
                // If partition ref up expression is empty, return false directly
                List<DateTrunc> dateTruncs =
                        tableColumnInfo.getPartitionExpression().get().collectToList(DateTrunc.class::isInstance);
                if (dateTruncs.size() > 1) {
                    return false;
                }
            }
        }
        return true;
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
     * Transform to common table id, this is used by get query struct info, maybe little err when same table occur
     * more than once, this is not a problem because the process of query rewrite by mv would consider more
     */
    public static BitSet transformToCommonTableId(BitSet relationIdSet, Map<Integer, Integer> relationIdToTableIdMap) {
        BitSet transformedBitset = new BitSet();
        for (int i = relationIdSet.nextSetBit(0); i >= 0; i = relationIdSet.nextSetBit(i + 1)) {
            Integer commonTableId = relationIdToTableIdMap.get(i);
            if (commonTableId != null) {
                transformedBitset.set(commonTableId);
            }
        }
        return transformedBitset;
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
            StructInfoMap structInfoMap = ownerGroup.getStructInfoMap();
            // Refresh struct info in current level plan from top to bottom
            SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
            structInfoMap.refresh(ownerGroup, cascadesContext, new BitSet(), new HashSet<>(),
                    sessionVariable.isEnableMaterializedViewNestRewrite());
            structInfoMap.setRefreshVersion(cascadesContext.getMemo().getRefreshVersion());
            Set<BitSet> queryTableSets = structInfoMap.getTableMaps();
            ImmutableList.Builder<StructInfo> structInfosBuilder = ImmutableList.builder();
            if (!queryTableSets.isEmpty()) {
                for (BitSet queryTableSet : queryTableSets) {
                    // TODO As only support MatchMode.COMPLETE, so only get equaled query table struct info
                    BitSet queryCommonTableSet = MaterializedViewUtils.transformToCommonTableId(queryTableSet,
                            cascadesContext.getStatementContext().getRelationIdToCommonTableIdMap());
                    // compare relation id corresponding table id
                    if (!materializedViewTableSet.isEmpty()
                            && !materializedViewTableSet.equals(queryCommonTableSet)) {
                        continue;
                    }
                    StructInfo structInfo = structInfoMap.getStructInfo(cascadesContext, queryTableSet, ownerGroup,
                            originalPlan, sessionVariable.isEnableMaterializedViewNestRewrite());
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
                Optional.empty(),
                ImmutableList.of());
        return BindRelation.checkAndAddDeleteSignFilter(olapScan, cascadesContext.getConnectContext(),
                olapScan.getTable());
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules, this method is only for materialized view rewrite
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext, Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan, boolean mvRewrite) {
        if (originPlan == null || rewrittenPlan == null) {
            return null;
        }
        if (originPlan.getOutputSet().size() != rewrittenPlan.getOutputSet().size()) {
            return rewrittenPlan;
        }
        Plan tmpRewrittenPlan = rewrittenPlan;
        // After RBO, slot order may change, so need originSlotToRewrittenExprId which record
        // origin plan slot order
        List<ExprId> rewrittenPlanOutputsBeforeOptimize =
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
        List<PlannerHook> removedMaterializedViewHooks = new ArrayList<>();
        try {
            if (!mvRewrite) {
                removedMaterializedViewHooks = removeMaterializedViewHooks(rewrittenPlanContext.getStatementContext());
            } else {
                // Add MaterializationContext for new cascades context
                cascadesContext.getMaterializationContexts().forEach(rewrittenPlanContext::addMaterializationContext);
            }
            rewrittenPlanContext.getConnectContext().setSkipAuth(true);
            AtomicReference<Plan> rewriteResult = new AtomicReference<>();
            rewrittenPlanContext.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                rewriteResult.set(planRewriter.apply(rewrittenPlanContext));
            });
            cascadesContext.addPlanProcesses(rewrittenPlanContext.getPlanProcesses());
            rewrittenPlan = rewriteResult.get();
        } finally {
            rewrittenPlanContext.getConnectContext().setSkipAuth(false);
            // Recover old disable rules variable
            rewrittenPlanContext.getStatementContext().getConnectContext().getSessionVariable()
                    .setDisableNereidsRules(String.join(",", oldDisableRuleNames));
            rewrittenPlanContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            rewrittenPlanContext.getStatementContext().getPlannerHooks().addAll(removedMaterializedViewHooks);
        }
        if (rewrittenPlan == null) {
            return null;
        }
        if (rewrittenPlan instanceof Sink) {
            // can keep the right column order, no need to adjust
            return rewrittenPlan;
        }
        Map<ExprId, Slot> rewrittenPlanAfterOptimizedExprIdToOutputMap = Maps.newLinkedHashMap();
        for (Slot slot : rewrittenPlan.getOutput()) {
            rewrittenPlanAfterOptimizedExprIdToOutputMap.put(slot.getExprId(), slot);
        }
        List<ExprId> rewrittenPlanOutputsAfterOptimized = rewrittenPlan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toList());
        // If project order doesn't change, return rewrittenPlan directly
        if (rewrittenPlanOutputsBeforeOptimize.equals(rewrittenPlanOutputsAfterOptimized)) {
            return rewrittenPlan;
        }
        // the expr id would change for some rule, once happened, not check result column order
        List<NamedExpression> adjustedOrderProjects = new ArrayList<>();
        for (ExprId exprId : rewrittenPlanOutputsBeforeOptimize) {
            Slot output = rewrittenPlanAfterOptimizedExprIdToOutputMap.get(exprId);
            if (output == null) {
                // some rule change the output slot id, would cause error, so not optimize and return tmpRewrittenPlan
                return tmpRewrittenPlan;
            }
            adjustedOrderProjects.add(output);
        }
        // If project order change, return rewrittenPlan with reordered projects
        return new LogicalProject<>(adjustedOrderProjects, rewrittenPlan);
    }

    /**
     * Normalize expression such as nullable property and output slot id
     */
    public static Plan normalizeExpressions(Plan rewrittenPlan, Plan originPlan) {
        if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
            return null;
        }
        // normalize nullable
        List<NamedExpression> normalizeProjects = new ArrayList<>();
        for (int i = 0; i < originPlan.getOutput().size(); i++) {
            normalizeProjects.add(normalizeExpression(originPlan.getOutput().get(i), rewrittenPlan.getOutput().get(i)));
        }
        return new LogicalProject<>(normalizeProjects, rewrittenPlan);
    }

    /**
     * Normalize expression with query, keep the consistency of exprId and nullable props with
     * query
     * Keep the replacedExpression slot property is the same as the sourceExpression
     */
    public static NamedExpression normalizeExpression(
            NamedExpression sourceExpression, NamedExpression replacedExpression) {
        Expression innerExpression = replacedExpression;
        if (replacedExpression.nullable() != sourceExpression.nullable()) {
            // if enable join eliminate, query maybe inner join and mv maybe outer join.
            // If the slot is at null generate side, the nullable maybe different between query and view
            // So need to force to consistent.
            innerExpression = sourceExpression.nullable()
                    ? new Nullable(replacedExpression) : new NonNullable(replacedExpression);
        }
        return new Alias(sourceExpression.getExprId(), innerExpression, sourceExpression.getName());
    }

    /**
     * removeMaterializedViewHooks
     *
     * @return removed materialized view hooks
     */
    public static List<PlannerHook> removeMaterializedViewHooks(StatementContext statementContext) {
        List<PlannerHook> tmpMaterializedViewHooks = new ArrayList<>();
        Set<PlannerHook> otherHooks = new HashSet<>();
        for (PlannerHook hook : statementContext.getPlannerHooks()) {
            if (hook instanceof InitMaterializationContextHook) {
                tmpMaterializedViewHooks.add(hook);
            } else {
                otherHooks.add(hook);
            }
        }
        statementContext.clearMaterializedHooksBy(otherHooks);
        return tmpMaterializedViewHooks;
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
     * Collect table used partitions, this is used for mv rewrite partition union
     * can not cumulative, if called multi times, should clean firstly
     */
    public static void collectTableUsedPartitions(Plan plan, CascadesContext cascadesContext) {
        // the recorded partition is based on relation id
        plan.accept(new QueryPartitionCollector(), cascadesContext);
    }

    /**
     * Decide the statementContext if contain materialized view hook or not
     */
    public static boolean containMaterializedViewHook(StatementContext statementContext) {
        for (PlannerHook plannerHook : statementContext.getPlannerHooks()) {
            // only collect when InitMaterializationContextHook exists in planner hooks
            if (plannerHook instanceof InitMaterializationContextHook) {
                return true;
            }
        }
        return false;
    }

    /**
     * Calc the chosen materialization context and all table used by final physical plan
     */
    public static Pair<Map<List<String>, MaterializationContext>, BitSet> getChosenMaterializationAndUsedTable(
            Plan physicalPlan, Map<List<String>, MaterializationContext> materializationContexts) {
        final Map<List<String>, MaterializationContext> chosenMaterializationMap = new HashMap<>();
        BitSet usedRelation = new BitSet();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Map<List<String>, MaterializationContext>>() {
            @Override
            public Void visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation,
                    Map<List<String>, MaterializationContext> chosenMaterializationMap) {
                usedRelation.set(catalogRelation.getRelationId().asInt());
                if (!(catalogRelation instanceof PhysicalOlapScan)) {
                    return null;
                }
                PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) catalogRelation;
                OlapTable table = physicalOlapScan.getTable();
                List<String> materializationIdentifier
                        = MaterializationContext.generateMaterializationIdentifierByIndexId(table,
                        physicalOlapScan.getSelectedIndexId() == table.getBaseIndexId()
                                ? null : physicalOlapScan.getSelectedIndexId());
                MaterializationContext materializationContext = materializationContexts.get(materializationIdentifier);
                if (materializationContext == null) {
                    return null;
                }
                if (materializationContext.isFinalChosen(catalogRelation)) {
                    chosenMaterializationMap.put(materializationIdentifier, materializationContext);
                }
                return null;
            }

            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation,
                    Map<List<String>, MaterializationContext> context) {
                usedRelation.set(physicalRelation.getRelationId().asInt());
                return null;
            }
        }, chosenMaterializationMap);
        return Pair.of(chosenMaterializationMap, usedRelation);
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
}
