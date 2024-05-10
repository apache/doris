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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRewriteUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.InvalidPartitionRemover;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.QueryScanPartitionsCollector;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule implements ExplorationRuleFactory {
    public static final Set<JoinType> SUPPORTED_JOIN_TYPE_SET = ImmutableSet.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.RIGHT_ANTI_JOIN);

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * multi materialization every time. if exception it will catch the exception and record it to
     * materialization context.
     */
    public List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<Plan> rewrittenPlans = new ArrayList<>();
        // if available materialization list is empty, bail out
        if (cascadesContext.getMaterializationContexts().isEmpty()) {
            return rewrittenPlans;
        }
        for (MaterializationContext context : cascadesContext.getMaterializationContexts()) {
            if (checkIfRewritten(queryPlan, context)) {
                continue;
            }
            // check mv plan is valid or not
            boolean valid = checkPattern(context.getStructInfo()) && context.getStructInfo().isValid();
            if (!valid) {
                context.recordFailReason(context.getStructInfo(),
                        "View struct info is invalid", () -> String.format(", view plan is %s",
                                context.getStructInfo().getOriginalPlan().treeString()));
                continue;
            }
            // get query struct infos according to the view strut info, if valid query struct infos is empty, bail out
            List<StructInfo> queryStructInfos = getValidQueryStructInfos(queryPlan, cascadesContext,
                    context.getStructInfo().getTableBitSet());
            if (queryStructInfos.isEmpty()) {
                continue;
            }
            for (StructInfo queryStructInfo : queryStructInfos) {
                try {
                    if (rewrittenPlans.size() < cascadesContext.getConnectContext()
                            .getSessionVariable().getMaterializedViewRewriteSuccessCandidateNum()) {
                        rewrittenPlans.addAll(doRewrite(queryStructInfo, cascadesContext, context));
                    }
                } catch (Exception exception) {
                    context.recordFailReason(queryStructInfo,
                            "Materialized view rule exec fail", exception::toString);
                }
            }
        }
        return rewrittenPlans;
    }

    /**
     * Get valid query struct infos, if invalid record the invalid reason
     */
    protected List<StructInfo> getValidQueryStructInfos(Plan queryPlan, CascadesContext cascadesContext,
            BitSet materializedViewTableSet) {
        List<StructInfo> validStructInfos = new ArrayList<>();
        // For every materialized view we should trigger refreshing struct info map
        List<StructInfo> uncheckedStructInfos = MaterializedViewUtils.extractStructInfo(queryPlan, cascadesContext,
                materializedViewTableSet);
        uncheckedStructInfos.forEach(queryStructInfo -> {
            boolean valid = checkPattern(queryStructInfo) && queryStructInfo.isValid();
            if (!valid) {
                cascadesContext.getMaterializationContexts().forEach(ctx ->
                        ctx.recordFailReason(queryStructInfo, "Query struct info is invalid",
                                () -> String.format("query table bitmap is %s, plan is %s",
                                        queryStructInfo.getTableBitSet(), queryPlan.treeString())
                        ));
            } else {
                validStructInfos.add(queryStructInfo);
            }
        });
        return validStructInfos;
    }

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * only one materialization every time. Different query pattern should override the sub logic.
     */
    protected List<Plan> doRewrite(StructInfo queryStructInfo, CascadesContext cascadesContext,
            MaterializationContext materializationContext) {
        List<Plan> rewriteResults = new ArrayList<>();
        StructInfo viewStructInfo = materializationContext.getStructInfo();
        MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations());
        if (MatchMode.COMPLETE != matchMode) {
            materializationContext.recordFailReason(queryStructInfo, "Match mode is invalid",
                    () -> String.format("matchMode is %s", matchMode));
            return rewriteResults;
        }
        List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(queryStructInfo.getRelations(),
                viewStructInfo.getRelations());
        // if any relation in query and view can not map, bail out.
        if (queryToViewTableMappings == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Query to view table mapping is null", () -> "");
            return rewriteResults;
        }
        for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
            SlotMapping queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
            if (queryToViewSlotMapping == null) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Query to view slot mapping is null", () -> "");
                continue;
            }
            SlotMapping viewToQuerySlotMapping = queryToViewSlotMapping.inverse();
            LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                    queryToViewTableMapping, queryToViewSlotMapping, queryStructInfo, viewStructInfo);
            ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo,
                    compatibilityContext);
            if (comparisonResult.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "The graph logic between query and view is not consistent",
                        comparisonResult::getErrorMessage);
                continue;
            }
            SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                    viewToQuerySlotMapping, comparisonResult, cascadesContext);
            // Can not compensate, bail out
            if (compensatePredicates.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Predicate compensate fail",
                        () -> String.format("query predicates = %s,\n query equivalenceClass = %s, \n"
                                        + "view predicates = %s,\n query equivalenceClass = %s\n"
                                        + "comparisonResult = %s ", queryStructInfo.getPredicates(),
                                queryStructInfo.getEquivalenceClass(), viewStructInfo.getPredicates(),
                                viewStructInfo.getEquivalenceClass(), comparisonResult));
                continue;
            }
            Plan rewrittenPlan;
            Plan mvScan = materializationContext.getMvScanPlan();
            Plan queryPlan = queryStructInfo.getTopPlan();
            if (compensatePredicates.isAlwaysTrue()) {
                rewrittenPlan = mvScan;
            } else {
                // Try to rewrite compensate predicates by using mv scan
                List<Expression> rewriteCompensatePredicates = rewriteExpression(compensatePredicates.toList(),
                        queryPlan, materializationContext.getMvExprToMvScanExprMapping(),
                        viewToQuerySlotMapping, true, queryStructInfo.getTableBitSet());
                if (rewriteCompensatePredicates.isEmpty()) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Rewrite compensate predicate by view fail",
                            () -> String.format("compensatePredicates = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                            + "viewToQuerySlotMapping = %s",
                                    compensatePredicates, materializationContext.getMvExprToMvScanExprMapping(),
                                    viewToQuerySlotMapping));
                    continue;
                }
                rewrittenPlan = new LogicalFilter<>(Sets.newLinkedHashSet(rewriteCompensatePredicates), mvScan);
            }
            // Rewrite query by view
            rewrittenPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    rewrittenPlan, materializationContext);
            if (rewrittenPlan == null) {
                continue;
            }
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getWholeTreeRewriter(childContext).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan);
            // check the partitions used by rewritten plan is valid or not
            Multimap<Pair<MTMVPartitionInfo, PartitionInfo>, Partition> invalidPartitionsQueryUsed =
                    calcUsedInvalidMvPartitions(rewrittenPlan, materializationContext, cascadesContext);
            // All partition used by query is valid
            if (!invalidPartitionsQueryUsed.isEmpty() && !cascadesContext.getConnectContext().getSessionVariable()
                    .isEnableMaterializedViewUnionRewrite()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Check partition query used validation fail",
                        () -> String.format("the partition used by query is invalid by materialized view,"
                                        + "invalid partition info query used is %s",
                                invalidPartitionsQueryUsed.values().stream()
                                        .map(Partition::getName)
                                        .collect(Collectors.toSet())));
                continue;
            }
            boolean partitionValid = invalidPartitionsQueryUsed.isEmpty();
            if (checkCanUnionRewrite(invalidPartitionsQueryUsed, queryPlan, cascadesContext)) {
                // construct filter on originalPlan
                Map<TableIf, Set<Expression>> filterOnOriginPlan;
                try {
                    filterOnOriginPlan = Predicates.constructFilterByPartitions(invalidPartitionsQueryUsed,
                            queryToViewSlotMapping);
                    if (filterOnOriginPlan.isEmpty()) {
                        materializationContext.recordFailReason(queryStructInfo,
                                "construct invalid partition filter on query fail",
                                () -> String.format("the invalid partitions used by query is %s, query plan is %s",
                                        invalidPartitionsQueryUsed.values().stream().map(Partition::getName)
                                                .collect(Collectors.toSet()),
                                        queryStructInfo.getOriginalPlan().treeString()));
                        continue;
                    }
                } catch (org.apache.doris.common.AnalysisException e) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "construct invalid partition filter on query analysis fail",
                            () -> String.format("the invalid partitions used by query is %s, query plan is %s",
                                    invalidPartitionsQueryUsed.values().stream().map(Partition::getName)
                                            .collect(Collectors.toSet()),
                                    queryStructInfo.getOriginalPlan().treeString()));
                    continue;
                }
                // For rewrittenPlan which contains materialized view should remove invalid partition ids
                List<Plan> children = Lists.newArrayList(
                        rewrittenPlan.accept(new InvalidPartitionRemover(), Pair.of(materializationContext.getMTMV(),
                                invalidPartitionsQueryUsed.values().stream()
                                        .map(Partition::getId).collect(Collectors.toSet()))),
                        StructInfo.addFilterOnTableScan(queryPlan, filterOnOriginPlan, cascadesContext));
                // Union query materialized view and source table
                rewrittenPlan = new LogicalUnion(Qualifier.ALL,
                        queryPlan.getOutput().stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                        children.stream()
                                .map(plan -> plan.getOutput().stream()
                                        .map(slot -> (SlotReference) slot.toSlot()).collect(Collectors.toList()))
                                .collect(Collectors.toList()),
                        ImmutableList.of(),
                        false,
                        children);
                partitionValid = true;
            }
            if (!partitionValid) {
                materializationContext.recordFailReason(queryStructInfo,
                        "materialized view partition is invalid union fail",
                        () -> String.format("invalidPartitionsQueryUsed =  %s,\n query plan = %s",
                                invalidPartitionsQueryUsed, queryPlan.treeString()));
                continue;
            }
            rewrittenPlan = normalizeExpressions(rewrittenPlan, queryPlan);
            if (!isOutputValid(queryPlan, rewrittenPlan)) {
                LogicalProperties logicalProperties = rewrittenPlan.getLogicalProperties();
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("planOutput logical"
                                        + " properties = %s,\n groupOutput logical properties = %s",
                                logicalProperties, queryPlan.getLogicalProperties()));
                continue;
            }
            recordIfRewritten(queryStructInfo.getOriginalPlan(), materializationContext);
            // if rewrite successfully, try to regenerate mv scan because it maybe used again
            materializationContext.tryReGenerateMvScanPlan(cascadesContext);
            rewriteResults.add(rewrittenPlan);
        }
        return rewriteResults;
    }

    private boolean checkCanUnionRewrite(Multimap<Pair<MTMVPartitionInfo, PartitionInfo>, Partition>
            invalidPartitionsQueryUsed, Plan queryPlan, CascadesContext cascadesContext) {
        if (invalidPartitionsQueryUsed.isEmpty()
                || !cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewUnionRewrite()) {
            return false;
        }
        // if mv can not offer valid partition data for query, bail out union rewrite
        Map<Long, Set<PartitionItem>> mvRelatedTablePartitionMap = new LinkedHashMap<>();
        invalidPartitionsQueryUsed.keySet().forEach(invalidPartition ->
                mvRelatedTablePartitionMap.put(invalidPartition.key().getRelatedTableInfo().getTableId(),
                        new HashSet<>()));
        queryPlan.accept(new QueryScanPartitionsCollector(), mvRelatedTablePartitionMap);
        Set<PartitionKeyDesc> partitionKeyDescSetQueryUsed = mvRelatedTablePartitionMap.values().stream()
                .flatMap(Collection::stream)
                .map(PartitionItem::toPartitionKeyDesc)
                .collect(Collectors.toSet());
        Set<PartitionKeyDesc> mvInvalidPartitionKeyDescSet = new HashSet<>();
        for (Map.Entry<Pair<MTMVPartitionInfo, PartitionInfo>, Collection<Partition>> entry :
                invalidPartitionsQueryUsed.asMap().entrySet()) {
            entry.getValue().forEach(invalidPartition -> mvInvalidPartitionKeyDescSet.add(
                    entry.getKey().value().getItem(invalidPartition.getId()).toPartitionKeyDesc()));
        }
        return !mvInvalidPartitionKeyDescSet.containsAll(partitionKeyDescSetQueryUsed);
    }

    // Normalize expression such as nullable property and output slot id
    protected Plan normalizeExpressions(Plan rewrittenPlan, Plan originPlan) {
        // normalize nullable
        List<NamedExpression> normalizeProjects = new ArrayList<>();
        for (int i = 0; i < originPlan.getOutput().size(); i++) {
            normalizeProjects.add(normalizeExpression(originPlan.getOutput().get(i), rewrittenPlan.getOutput().get(i)));
        }
        return new LogicalProject<>(normalizeProjects, rewrittenPlan);
    }

    /**
     * Check the logical properties of rewritten plan by mv is the same with source plan
     * if same return true, if different return false
     */
    protected boolean isOutputValid(Plan sourcePlan, Plan rewrittenPlan) {
        if (sourcePlan.getGroupExpression().isPresent() && !rewrittenPlan.getLogicalProperties()
                .equals(sourcePlan.getGroupExpression().get().getOwnerGroup().getLogicalProperties())) {
            return false;
        }
        return sourcePlan.getLogicalProperties().equals(rewrittenPlan.getLogicalProperties());
    }

    /**
     * Partition will be pruned in query then add the pruned partitions to select partitions field of
     * catalog relation.
     * Maybe only just some partitions is valid in materialized view, so we should check if the mv can
     * offer the partitions which query used or not.
     *
     * @return the invalid partition name set
     */
    protected Multimap<Pair<MTMVPartitionInfo, PartitionInfo>, Partition> calcUsedInvalidMvPartitions(
            Plan rewrittenPlan,
            MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        // check partition is valid or not
        MTMV mtmv = materializationContext.getMTMV();
        PartitionInfo mvPartitionInfo = mtmv.getPartitionInfo();
        if (PartitionType.UNPARTITIONED.equals(mvPartitionInfo.getType())) {
            // if not partition, if rewrite success, it means mv is available
            return ImmutableMultimap.of();
        }
        // check mv related table partition is valid or not
        MTMVPartitionInfo mvCustomPartitionInfo = mtmv.getMvPartitionInfo();
        BaseTableInfo relatedPartitionTable = mvCustomPartitionInfo.getRelatedTableInfo();
        if (relatedPartitionTable == null) {
            return ImmutableMultimap.of();
        }
        // get mv valid partitions
        Set<Long> mvDataValidPartitionIdSet = MTMVRewriteUtil.getMTMVCanRewritePartitions(mtmv,
                        cascadesContext.getConnectContext(), System.currentTimeMillis()).stream()
                .map(Partition::getId)
                .collect(Collectors.toSet());
        // get partitions query used
        Set<Long> mvPartitionSetQueryUsed = rewrittenPlan.collectToList(node -> node instanceof LogicalOlapScan
                        && Objects.equals(((CatalogRelation) node).getTable().getName(), mtmv.getName()))
                .stream()
                .map(node -> ((LogicalOlapScan) node).getSelectedPartitionIds())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        // get invalid partition ids
        Set<Long> invalidMvPartitionIdSet = new HashSet<>(mvPartitionSetQueryUsed);
        invalidMvPartitionIdSet.removeAll(mvDataValidPartitionIdSet);
        ImmutableMultimap.Builder<Pair<MTMVPartitionInfo, PartitionInfo>, Partition> invalidPartitionMapBuilder =
                ImmutableMultimap.builder();
        Pair<MTMVPartitionInfo, PartitionInfo> partitionInfo = Pair.of(mvCustomPartitionInfo, mvPartitionInfo);
        invalidMvPartitionIdSet.forEach(invalidPartitionId ->
                        invalidPartitionMapBuilder.put(partitionInfo, mtmv.getPartition(invalidPartitionId)));
        return invalidPartitionMapBuilder.build();
    }

    /**
     * Rewrite query by view, for aggregate or join rewriting should be different inherit class implementation
     */
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext) {
        return tempRewritedPlan;
    }

    /**
     * Use target expression to represent the source expression. Visit the source expression,
     * try to replace the source expression with target expression in targetExpressionMapping, if found then
     * replace the source expression by target expression mapping value.
     *
     * @param sourceExpressionsToWrite the source expression to write by target expression
     * @param sourcePlan the source plan witch the source expression belong to
     * @param targetExpressionMapping target expression mapping, if finding the expression in key set of the mapping
     *         then use the corresponding value of mapping to replace it
     * @param targetExpressionNeedSourceBased if targetExpressionNeedSourceBased is true,
     *         we should make the target expression map key to source based,
     *         Note: the key expression in targetExpressionMapping should be shuttled. with the method
     *         ExpressionUtils.shuttleExpressionWithLineage.
     *         example as following:
     *         source                           target
     *         project(slot 1, 2)              project(slot 3, 2, 1)
     *         scan(table)                        scan(table)
     *         then
     *         transform source to:
     *         project(slot 2, 1)
     *         target
     */
    protected List<Expression> rewriteExpression(List<? extends Expression> sourceExpressionsToWrite, Plan sourcePlan,
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping,
            boolean targetExpressionNeedSourceBased, BitSet sourcePlanBitSet) {
        // Firstly, rewrite the target expression using source with inverse mapping
        // then try to use the target expression to represent the query. if any of source expressions
        // can not be represented by target expressions, return null.
        // generate target to target replacement expression mapping, and change target expression to source based
        List<? extends Expression> sourceShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                sourceExpressionsToWrite, sourcePlan, sourcePlanBitSet);
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionNeedSourceBased
                ? targetExpressionMapping.keyPermute(targetToSourceMapping) : targetExpressionMapping;
        // target to target replacement expression mapping, because mv is 1:1 so get first element
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        Map<? extends Expression, ? extends Expression> targetToTargetReplacementMapping = flattenExpressionMap.get(0);

        List<Expression> rewrittenExpressions = new ArrayList<>();
        for (Expression expressionShuttledToRewrite : sourceShuttledExpressions) {
            if (expressionShuttledToRewrite instanceof Literal) {
                rewrittenExpressions.add(expressionShuttledToRewrite);
                continue;
            }
            final Set<Object> slotsToRewrite =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof Slot);
            Expression replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                    targetToTargetReplacementMapping);
            if (replacedExpression.anyMatch(slotsToRewrite::contains)) {
                // if contains any slot to rewrite, which means can not be rewritten by target, bail out
                return ImmutableList.of();
            }
            rewrittenExpressions.add(replacedExpression);
        }
        return rewrittenExpressions;
    }

    /**
     * Normalize expression with query, keep the consistency of exprId and nullable props with
     * query
     */
    private NamedExpression normalizeExpression(
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
     * Compensate mv predicates by query predicates, compensate predicate result is query based.
     * Such as a > 5 in mv, and a > 10 in query, the compensatory predicate is a > 10.
     * For another example as following:
     * predicate a = b in mv, and a = b and c = d in query, the compensatory predicate is c = d
     */
    protected SplitPredicate predicatesCompensate(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext
    ) {
        // TODO: Use set of list? And consider view expr
        List<Expression> queryPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getQueryExpressions());
        // set pulled up expression to queryStructInfo predicates and update related predicates
        if (!queryPulledUpExpressions.isEmpty()) {
            queryStructInfo = queryStructInfo.withPredicates(
                    queryStructInfo.getPredicates().merge(queryPulledUpExpressions));
        }
        List<Expression> viewPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getViewExpressions());
        // set pulled up expression to viewStructInfo predicates and update related predicates
        if (!viewPulledUpExpressions.isEmpty()) {
            viewStructInfo = viewStructInfo.withPredicates(
                    viewStructInfo.getPredicates().merge(viewPulledUpExpressions));
        }
        // if the join type in query and mv plan is different, we should check query is have the
        // filters which rejects null
        Set<Set<Slot>> requireNoNullableViewSlot = comparisonResult.getViewNoNullableSlot();
        // check query is use the null reject slot which view comparison need
        if (!requireNoNullableViewSlot.isEmpty()) {
            SlotMapping queryToViewMapping = viewToQuerySlotMapping.inverse();
            // try to use
            boolean valid = containsNullRejectSlot(requireNoNullableViewSlot,
                    queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, cascadesContext);
            if (!valid) {
                queryStructInfo = queryStructInfo.withPredicates(
                        queryStructInfo.getPredicates().merge(comparisonResult.getQueryAllPulledUpExpressions()));
                valid = containsNullRejectSlot(requireNoNullableViewSlot,
                        queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, cascadesContext);
            }
            if (!valid) {
                return SplitPredicate.INVALID_INSTANCE;
            }
        }
        // viewEquivalenceClass to query based
        // equal predicate compensate
        final Set<Expression> equalCompensateConjunctions = Predicates.compensateEquivalence(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult);
        // range compensate
        final Set<Expression> rangeCompensatePredicates = Predicates.compensateRangePredicate(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult,
                cascadesContext);
        // residual compensate
        final Set<Expression> residualCompensatePredicates = Predicates.compensateResidualPredicate(
                queryStructInfo,
                viewStructInfo,
                viewToQuerySlotMapping,
                comparisonResult);
        if (equalCompensateConjunctions == null || rangeCompensatePredicates == null
                || residualCompensatePredicates == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        if (equalCompensateConjunctions.stream().anyMatch(expr -> expr.containsType(AggregateFunction.class))
                || rangeCompensatePredicates.stream().anyMatch(expr -> expr.containsType(AggregateFunction.class))
                || residualCompensatePredicates.stream().anyMatch(expr ->
                expr.containsType(AggregateFunction.class))) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        return SplitPredicate.of(equalCompensateConjunctions.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(equalCompensateConjunctions),
                rangeCompensatePredicates.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(rangeCompensatePredicates),
                residualCompensatePredicates.isEmpty() ? BooleanLiteral.TRUE
                        : ExpressionUtils.and(residualCompensatePredicates));
    }

    /**
     * Check the queryPredicates contains the required nullable slot
     */
    private boolean containsNullRejectSlot(Set<Set<Slot>> requireNoNullableViewSlot,
            Set<Expression> queryPredicates,
            SlotMapping queryToViewMapping,
            CascadesContext cascadesContext) {
        Set<Expression> queryPulledUpPredicates = queryPredicates.stream()
                .flatMap(expr -> ExpressionUtils.extractConjunction(expr).stream())
                .map(expr -> {
                    // NOTICE inferNotNull generate Not with isGeneratedIsNotNull = false,
                    //  so, we need set this flag to false before comparison.
                    if (expr instanceof Not) {
                        return ((Not) expr).withGeneratedIsNotNull(false);
                    }
                    return expr;
                })
                .collect(Collectors.toSet());
        Set<Expression> nullRejectPredicates = ExpressionUtils.inferNotNull(queryPulledUpPredicates, cascadesContext);
        Set<Expression> queryUsedNeedRejectNullSlotsViewBased = nullRejectPredicates.stream()
                .map(expression -> TypeUtils.isNotNull(expression).orElse(null))
                .filter(Objects::nonNull)
                .map(expr -> ExpressionUtils.replace((Expression) expr, queryToViewMapping.toSlotReferenceMap()))
                .collect(Collectors.toSet());
        // query pulledUp predicates should have null reject predicates and contains any require noNullable slot
        return !queryPulledUpPredicates.containsAll(nullRejectPredicates)
                && requireNoNullableViewSlot.stream().noneMatch(set ->
                Sets.intersection(set, queryUsedNeedRejectNullSlotsViewBased).isEmpty());
    }

    /**
     * Decide the match mode
     *
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations) {
        List<TableIf> queryTableRefs = queryRelations.stream().map(CatalogRelation::getTable)
                .collect(Collectors.toList());
        List<TableIf> viewTableRefs = viewRelations.stream().map(CatalogRelation::getTable)
                .collect(Collectors.toList());
        boolean sizeSame = viewTableRefs.size() == queryTableRefs.size();
        boolean queryPartial = viewTableRefs.containsAll(queryTableRefs);
        if (!sizeSame && queryPartial) {
            return MatchMode.QUERY_PARTIAL;
        }
        boolean viewPartial = queryTableRefs.containsAll(viewTableRefs);
        if (!sizeSame && viewPartial) {
            return MatchMode.VIEW_PARTIAL;
        }
        if (sizeSame && queryPartial && viewPartial) {
            return MatchMode.COMPLETE;
        }
        return MatchMode.NOT_MATCH;
    }

    /**
     * Check the pattern of query or materializedView is supported or not.
     */
    protected boolean checkPattern(StructInfo structInfo) {
        if (structInfo.getRelations().isEmpty()) {
            return false;
        }
        return true;
    }

    protected void recordIfRewritten(Plan plan, MaterializationContext context) {
        context.setSuccess(true);
        if (plan.getGroupExpression().isPresent()) {
            context.addMatchedGroup(plan.getGroupExpression().get().getOwnerGroup().getGroupId(), true);
        }
    }

    protected boolean checkIfRewritten(Plan plan, MaterializationContext context) {
        return plan.getGroupExpression().isPresent()
                && context.alreadyRewrite(plan.getGroupExpression().get().getOwnerGroup().getGroupId());
    }

    /**
     * Query and mv match node
     */
    protected enum MatchMode {
        /**
         * The tables in query are same to the tables in view
         */
        COMPLETE,
        /**
         * The tables in query contains all the tables in view
         */
        VIEW_PARTIAL,
        /**
         * The tables in view contains all the tables in query
         */
        QUERY_PARTIAL,
        /**
         * Except for COMPLETE and VIEW_PARTIAL and QUERY_PARTIAL
         */
        NOT_MATCH
    }
}
