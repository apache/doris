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
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementChecker;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.rewrite.QueryPartitionCollector;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        Map<CTEId, Plan> producerCteIdToPlanMap = collectProducerCtePlans(materializedViewPlan);
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, producerCteIdToPlanMap, materializedViewPlan, cascadesContext);
        checkContext.getPartitionAndRefExpressionMap().put(columnExpr,
                RelatedTableColumnInfo.of(columnExpr, null, true, false));
        materializedViewPlan.accept(PartitionIncrementChecker.INSTANCE, checkContext);
        List<RelatedTableColumnInfo> checkedTableColumnInfos =
                PartitionIncrementMaintainer.getRelatedTableColumnInfosWithCheck(checkContext, tableColumnInfo ->
                        tableColumnInfo.isOriginalPartition() && tableColumnInfo.isFromTablePartitionColumn());
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (checkContext.isFailFast()) {
            return RelatedTableInfo.failWith("partition column is not in group by or window partition by, "
                    + checkContext.getFailReasons());
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
        Map<CTEId, Plan> producerCteIdToPlanMap = collectProducerCtePlans(materializedViewPlan);
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, producerCteIdToPlanMap, materializedViewPlan, cascadesContext);
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
                        RelatedTableColumnInfo::isFromTablePartitionColumn);
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (checkContext.isFailFast()) {
            return RelatedTableInfo.failWith("partition column is not in group by or window partition by, "
                    + checkContext.getFailReasons());
        }
        if (!checkedTableColumnInfos.isEmpty()) {
            return RelatedTableInfo.successWith(checkedTableColumnInfos);
        }
        return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                String.join(",", checkContext.getFailReasons())));
    }

    private static Map<CTEId, Plan> collectProducerCtePlans(Plan plan) {
        Map<CTEId, Plan> collectProducerCtePlans = new HashMap<>();
        plan.accept(new DefaultPlanVisitor<Void, Map<CTEId, Plan>>() {
            @Override
            public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
                    Map<CTEId, Plan> context) {
                context.put(cteProducer.getCteId(), cteProducer);
                return super.visitLogicalCTEProducer(cteProducer, context);
            }
        }, collectProducerCtePlans);
        return collectProducerCtePlans;
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
     * @param cascadesContext the cascadesContext when extractStructInfo
     * @param targetTableIdSet the target relation id set which used to filter struct info,
     *                            empty means no struct info match
     */
    public static List<StructInfo> extractStructInfoFuzzy(Plan plan, Plan originalPlan,
                                                          CascadesContext cascadesContext, BitSet targetTableIdSet) {
        // If plan belong to some group, construct it with group struct info
        if (plan.getGroupExpression().isPresent()) {
            Group ownerGroup = plan.getGroupExpression().get().getOwnerGroup();
            StructInfoMap structInfoMap = ownerGroup.getStructInfoMap();
            // Refresh struct info in current level plan from top to bottom
            SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
            int memoVersion = StructInfoMap.getMemoVersion(targetTableIdSet,
                    cascadesContext.getMemo().getRefreshVersion());
            structInfoMap.refresh(ownerGroup, cascadesContext, targetTableIdSet, new HashSet<>(),
                    sessionVariable.isEnableMaterializedViewNestRewrite(), memoVersion, true);
            structInfoMap.setRefreshVersion(targetTableIdSet, cascadesContext.getMemo().getRefreshVersion());
            Set<BitSet> queryTableIdSets = structInfoMap.getTableMaps(true);
            ImmutableList.Builder<StructInfo> structInfosBuilder = ImmutableList.builder();
            if (!queryTableIdSets.isEmpty()) {
                for (BitSet queryTableIdSet : queryTableIdSets) {
                    // compare relation id corresponding table id
                    if (!containsAll(targetTableIdSet, queryTableIdSet)) {
                        continue;
                    }
                    StructInfo structInfo = structInfoMap.getStructInfo(cascadesContext, queryTableIdSet, ownerGroup,
                            originalPlan, sessionVariable.isEnableMaterializedViewNestRewrite(), true);
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
    public static LogicalOlapScan generateMvOlapScanPlan(OlapTable table, long indexId,
            List<Long> partitionIds,
            PreAggStatus preAggStatus,
            CascadesContext cascadesContext) {
        return new LogicalOlapScan(
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
    }

    public static LogicalOlapScan withImpliedPredicates(
            LogicalOlapScan olapScan, StructInfo structInfo,
            List<? extends Expression> planOutputShuttledExpressions) {
        ExpressionMapping shuttledExprToScanExprMapping =
                ExpressionMapping.generate(planOutputShuttledExpressions, olapScan.getOutput());
        return withImpliedPredicates(olapScan, structInfo, shuttledExprToScanExprMapping);
    }

    static LogicalOlapScan withImpliedPredicates(
            LogicalOlapScan olapScan, StructInfo structInfo, ExpressionMapping shuttledExprToScanExprMapping) {
        List<Map<Expression, Expression>> flattenExpressionMap = shuttledExprToScanExprMapping.flattenMap();
        Map<Expression, Expression> lineageExpressionToScanExpressionMap = flattenExpressionMap.isEmpty()
                ? ImmutableMap.of()
                : flattenExpressionMap.get(0);
        Set<Expression> scanPredicates = collectScanPredicates(
                structInfo, lineageExpressionToScanExpressionMap);
        return olapScan.withRelationImpliedPredicates(scanPredicates);
    }

    private static Set<Expression> collectScanPredicates(
            StructInfo structInfo, Map<Expression, Expression> lineageExpressionToScanExpressionMap) {
        Set<Expression> scanPredicateCandidates = structInfo.getPredicates().getSemanticPredicates();
        Set<Expression> scanPredicates = new HashSet<>();
        if (scanPredicateCandidates.isEmpty()) {
            return scanPredicates;
        }

        List<? extends Expression> lineagePredicates = ExpressionUtils.shuttleExpressionWithLineage(
                new ArrayList<>(scanPredicateCandidates), structInfo.getTopPlan());

        Map<NonOutputSlotKey, SlotReference> nonOutputSlots = new HashMap<>();
        for (Expression predicate : lineagePredicates) {
            if (ExpressionUtils.isInferred(predicate) || BooleanLiteral.TRUE.equals(predicate)) {
                continue;
            }
            if (!canRepresentAsScanPredicate(predicate, lineageExpressionToScanExpressionMap)) {
                continue;
            }
            Expression scanPredicate = rewritePredicateToScan(predicate,
                    lineageExpressionToScanExpressionMap, nonOutputSlots);
            scanPredicates.add(scanPredicate);
        }
        return scanPredicates;
    }

    // StructInfo semantic predicates may contain top-plan predicates that are not tied to a base-table column,
    // such as mark-join slots generated for NOT IN subqueries. Only predicates whose every leaf can be mapped
    // to an MV scan output expression or to an identifiable original table column can be carried by the scan.
    private static boolean canRepresentAsScanPredicate(Expression predicate,
            Map<Expression, Expression> lineageExpressionToScanExpressionMap) {
        if (lineageExpressionToScanExpressionMap.containsKey(predicate)) {
            return true;
        }
        if (predicate instanceof SlotReference) {
            // Mark-join slots like $c$1 are semantic predicates of the upper plan,
            // but they are not relation columns and cannot be carried by an MV scan.
            SlotReference slot = (SlotReference) predicate;
            return slot.getOriginalTable().isPresent() && slot.getOriginalColumn().isPresent();
        }
        for (Expression child : predicate.children()) {
            if (!canRepresentAsScanPredicate(child, lineageExpressionToScanExpressionMap)) {
                return false;
            }
        }
        return true;
    }

    // Rewrite a semantic predicate from the MV definition side into the expression form
    // that can be carried by the MV scan. For example, if the MV outputs `a + 1 AS x`,
    // lineageExpressionToScanExpressionMap contains `a + 1 -> mv.x`, so `a + 1 > 10`
    // is rewritten to `mv.x > 10`. If the predicate references a non-output column like `c > 10`,
    // create a non-output slot for `c`, and reuse the same slot through nonOutputSlots.
    private static Expression rewritePredicateToScan(Expression predicate,
            Map<Expression, Expression> lineageExpressionToScanExpressionMap,
            Map<NonOutputSlotKey, SlotReference> nonOutputSlots) {
        return predicate.rewriteDownShortCircuit(expression -> {
            Expression scanExpression = lineageExpressionToScanExpressionMap.get(expression);
            if (scanExpression != null) {
                // When an MV output expression is matched, replace it with the MV scan output slot,
                // for example `a + 1` -> `mv.x`.
                return scanExpression;
            }
            if (expression instanceof SlotReference) {
                SlotReference slot = (SlotReference) expression;
                Preconditions.checkState(slot.getOriginalTable().isPresent()
                        && slot.getOriginalColumn().isPresent(),
                        "slot can not be rewritten to mv scan predicate: %s", slot);
                NonOutputSlotKey key = NonOutputSlotKey.of(slot);
                // When a non-output base-table column is still identifiable, create a scan-side slot for it.
                return nonOutputSlots.computeIfAbsent(key,
                        ignored -> slot.withExprId(StatementScopeIdGenerator.newExprId()));
            }
            return expression;
        });
    }

    // Reuse one non-output slot for the same original table column/sub-path in all rewritten predicates.
    private static class NonOutputSlotKey {
        private final TableIdentifier tableIdentifier;
        private final String columnName;
        private final List<String> subPath;

        private NonOutputSlotKey(TableIdentifier tableIdentifier, String columnName, List<String> subPath) {
            this.tableIdentifier = tableIdentifier;
            this.columnName = columnName;
            this.subPath = subPath;
        }

        private static NonOutputSlotKey of(SlotReference slot) {
            return new NonOutputSlotKey(new TableIdentifier(slot.getOriginalTable().get()),
                    slot.getOriginalColumn().get().getName(), slot.getSubPath());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof NonOutputSlotKey)) {
                return false;
            }
            NonOutputSlotKey that = (NonOutputSlotKey) o;
            return Objects.equals(tableIdentifier, that.tableIdentifier)
                    && Objects.equals(columnName, that.columnName)
                    && Objects.equals(subPath, that.subPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableIdentifier, columnName, subPath);
        }
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules, this method is only for materialized view rewrite
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext, Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan, boolean mvRewrite) {
        return rewriteByRules(cascadesContext, planRewriter, rewrittenPlan, originPlan, mvRewrite, false);
    }

    /**
     * Optimize by rules, this support optimize by custom rules by define different rewriter according to different
     * rules, this method is only for materialized view rewrite
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext, Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan, boolean mvRewrite, boolean materializedViewRewritePlanFragment) {
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
        rewrittenPlanContext.setMaterializedViewRewritePlanFragment(materializedViewRewritePlanFragment);
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
     * Normalize expression such as nullable property and output slot id when plan in the plan tree
     */
    public static Plan normalizeExpressions(Plan rewrittenPlan, Plan originPlan) {
        if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
            return null;
        }
        // normalize nullable
        List<NamedExpression> normalizeProjects = new ArrayList<>();
        for (int i = 0; i < originPlan.getOutput().size(); i++) {
            normalizeProjects.add(normalizeExpression(originPlan.getOutput().get(i),
                    rewrittenPlan.getOutput().get(i), false));
        }
        return new LogicalProject<>(normalizeProjects, rewrittenPlan);
    }

    /**
     * Normalize expression such as nullable property and output slot id when plan is on the top of tree
     */
    public static Plan normalizeSinkExpressions(Plan rewrittenPlan, Plan originPlan) {
        return rewrittenPlan.accept(new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visitLogicalSink(LogicalSink<? extends Plan> rewrittenPlan, Void context) {
                if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
                    return null;
                }
                if (rewrittenPlan.getLogicalProperties().equals(originPlan.getLogicalProperties())) {
                    return rewrittenPlan;
                }
                List<NamedExpression> rewrittenPlanOutputExprList = rewrittenPlan.getOutputExprs();
                List<? extends NamedExpression> originPlanOutputExprList = originPlan.getOutput();
                if (rewrittenPlanOutputExprList.size() != originPlanOutputExprList.size()) {
                    return null;
                }
                List<NamedExpression> normalizedOutputExprList = new ArrayList<>();
                for (int i = 0; i < rewrittenPlanOutputExprList.size(); i++) {
                    NamedExpression rewrittenExpression = rewrittenPlanOutputExprList.get(i);
                    NamedExpression originalExpression = originPlanOutputExprList.get(i);
                    normalizedOutputExprList.add(normalizeExpression(originalExpression,
                            rewrittenExpression, true));
                }
                LogicalProject<Plan> project = new LogicalProject<>(normalizedOutputExprList,
                        rewrittenPlan.child());
                return rewrittenPlan.withChildren(project);
            }
        }, null);

    }

    /**
     * Normalize expression with query, keep the consistency of exprId and nullable props with
     * query
     * Keep the replacedExpression slot property is the same as the sourceExpression
     */
    public static NamedExpression normalizeExpression(
            NamedExpression sourceExpression, NamedExpression replacedExpression, boolean isSink) {
        Expression innerExpression = replacedExpression;
        boolean isExprEquals = replacedExpression.getExprId().equals(sourceExpression.getExprId());
        boolean isNullableEquals = replacedExpression.nullable() == sourceExpression.nullable();
        if (isExprEquals && isNullableEquals) {
            return replacedExpression;
        }
        if (isExprEquals && isSink && replacedExpression instanceof SlotReference) {
            // for sink, if expr id is the same, but nullable is different, should keep the same expr id
            return ((SlotReference) replacedExpression).withNullable(sourceExpression.nullable());
        }
        if (!isNullableEquals) {
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
     * Checks if the superset contains all of the set bits from the subset.
     *
     * @param superset The BitSet expected to contain the bits.
     * @param subset   The BitSet whose set bits are to be checked.
     * @return true if all bits set in the subset are also set in the superset, false otherwise.
     */
    public static boolean containsAll(BitSet superset, BitSet subset) {
        // Clone the subset to avoid modifying the original instance.
        BitSet temp = (BitSet) subset.clone();
        // Remove all bits from temp that are also present in the superset.
        // temp.andNot(superset) is equivalent to the operation: temp = temp AND (NOT superset)
        temp.andNot(superset);
        return temp.isEmpty();
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

    /**
     * Check the prefix of two order key list is same from start
     */
    public static boolean isPrefixSameFromStart(List<OrderKey> queryShuttledOrderKeys,
                                                 List<OrderKey> viewShuttledOrderKeys) {
        if (queryShuttledOrderKeys == null || viewShuttledOrderKeys == null) {
            return false;
        }
        if (queryShuttledOrderKeys.size() > viewShuttledOrderKeys.size()) {
            return false;
        }
        for (int i = 0; i < queryShuttledOrderKeys.size(); i++) {
            if (!java.util.Objects.equals(queryShuttledOrderKeys.get(i), viewShuttledOrderKeys.get(i))) {
                return false;
            }
        }
        return true;
    }
}
