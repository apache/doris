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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRewriteUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule implements ExplorationRuleFactory {
    public static final HashSet<JoinType> SUPPORTED_JOIN_TYPE_SET = Sets.newHashSet(JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN);

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * multi materialization every time. if exception it will catch the exception and record it to
     * materialization context.
     */
    public List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<Plan> rewrittenPlans = new ArrayList<>();
        // already rewrite or query is invalid, bail out
        List<StructInfo> queryStructInfos = checkQuery(queryPlan, cascadesContext);
        if (queryStructInfos.isEmpty()) {
            return rewrittenPlans;
        }
        for (MaterializationContext context : cascadesContext.getMaterializationContexts()) {
            if (checkIfRewritten(queryPlan, context)) {
                continue;
            }
            // TODO Just support only one query struct info, support multi later.
            StructInfo queryStructInfo = queryStructInfos.get(0);
            try {
                rewrittenPlans.addAll(doRewrite(queryStructInfo, cascadesContext, context));
            } catch (Exception exception) {
                context.recordFailReason(queryStructInfo,
                        Pair.of("Materialized view rule exec fail", exception.toString()));
            }
        }
        return rewrittenPlans;
    }

    /**
     * Check query is valid or not, if valid return the query struct infos, if invalid return empty list.
     */
    protected List<StructInfo> checkQuery(Plan queryPlan, CascadesContext cascadesContext) {
        List<StructInfo> validQueryStructInfos = new ArrayList<>();
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        if (materializationContexts.isEmpty()) {
            return validQueryStructInfos;
        }
        List<StructInfo> queryStructInfos = MaterializedViewUtils.extractStructInfo(queryPlan, cascadesContext);
        // TODO Just Check query queryPlan firstly, support multi later.
        StructInfo queryStructInfo = queryStructInfos.get(0);
        if (!checkPattern(queryStructInfo)) {
            cascadesContext.getMaterializationContexts().forEach(ctx ->
                    ctx.recordFailReason(queryStructInfo,
                            Pair.of("Query struct info is invalid",
                                    String.format("queryPlan is %s", queryPlan.treeString())))
            );
            return validQueryStructInfos;
        }
        validQueryStructInfos.add(queryStructInfo);
        return validQueryStructInfos;
    }

    /**
     * The abstract template method for query rewrite, it contains the main logic, try to rewrite query by
     * only one materialization every time. Different query pattern should override the sub logic.
     */
    protected List<Plan> doRewrite(StructInfo queryStructInfo, CascadesContext cascadesContext,
            MaterializationContext materializationContext) {
        List<Plan> rewriteResults = new ArrayList<>();
        List<StructInfo> viewStructInfos = MaterializedViewUtils.extractStructInfo(
                materializationContext.getMvPlan(), cascadesContext);
        if (viewStructInfos.size() > 1) {
            // view struct info should only have one
            materializationContext.recordFailReason(queryStructInfo,
                    Pair.of("The num of view struct info is more then one",
                            String.format("mv plan is %s", materializationContext.getMvPlan().treeString())));
            return rewriteResults;
        }
        StructInfo viewStructInfo = viewStructInfos.get(0);
        if (!checkPattern(viewStructInfo)) {
            materializationContext.recordFailReason(queryStructInfo,
                    Pair.of("View struct info is invalid",
                            String.format(", view plan is %s", viewStructInfo.getOriginalPlan().treeString())));
            return rewriteResults;
        }
        MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations());
        if (MatchMode.COMPLETE != matchMode) {
            materializationContext.recordFailReason(queryStructInfo,
                    Pair.of("Match mode is invalid", String.format("matchMode is %s", matchMode)));
            return rewriteResults;
        }
        List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(queryStructInfo.getRelations(),
                viewStructInfo.getRelations());
        // if any relation in query and view can not map, bail out.
        if (queryToViewTableMappings == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    Pair.of("Query to view table mapping is null", ""));
            return rewriteResults;
        }
        for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
            SlotMapping queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
            if (queryToViewSlotMapping == null) {
                materializationContext.recordFailReason(queryStructInfo,
                        Pair.of("Query to view slot mapping is null", ""));
                continue;
            }
            SlotMapping viewToQuerySlotMapping = queryToViewSlotMapping.inverse();
            // check the column used in query is in mv or not
            if (!checkColumnUsedValid(queryStructInfo, viewStructInfo, queryToViewSlotMapping)) {
                materializationContext.recordFailReason(queryStructInfo,
                        Pair.of("The columns used by query are not in view",
                                String.format("query struct info is %s, view struct info is %s",
                                        queryStructInfo.getTopPlan().treeString(),
                                        viewStructInfo.getTopPlan().treeString())));
                continue;
            }
            LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                    queryToViewTableMapping, queryToViewSlotMapping, queryStructInfo, viewStructInfo);
            ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo,
                    compatibilityContext);
            if (comparisonResult.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        Pair.of("The graph logic between query and view is not consistent",
                                comparisonResult.getErrorMessage()));
                continue;
            }
            SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                    viewToQuerySlotMapping, comparisonResult, cascadesContext);
            // Can not compensate, bail out
            if (compensatePredicates.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        Pair.of("Predicate compensate fail",
                                String.format("query predicates = %s,\n query equivalenceClass = %s, \n"
                                                + "view predicates = %s,\n query equivalenceClass = %s\n"
                                                + "comparisonResult = %s ",
                                        queryStructInfo.getPredicates(),
                                        queryStructInfo.getEquivalenceClass(),
                                        viewStructInfo.getPredicates(),
                                        viewStructInfo.getEquivalenceClass(),
                                        comparisonResult)));
                continue;
            }
            Plan rewrittenPlan;
            Plan mvScan = materializationContext.getMvScanPlan();
            Plan originalPlan = queryStructInfo.getOriginalPlan();
            if (compensatePredicates.isAlwaysTrue()) {
                rewrittenPlan = mvScan;
            } else {
                // Try to rewrite compensate predicates by using mv scan
                List<Expression> rewriteCompensatePredicates = rewriteExpression(compensatePredicates.toList(),
                        originalPlan, materializationContext.getMvExprToMvScanExprMapping(),
                        viewToQuerySlotMapping, true);
                if (rewriteCompensatePredicates.isEmpty()) {
                    materializationContext.recordFailReason(queryStructInfo,
                            Pair.of("Rewrite compensate predicate by view fail", String.format(
                                    "compensatePredicates = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                            + "viewToQuerySlotMapping = %s",
                                    compensatePredicates,
                                    materializationContext.getMvExprToMvScanExprMapping(),
                                    viewToQuerySlotMapping)));
                    continue;
                }
                rewrittenPlan = new LogicalFilter<>(Sets.newHashSet(rewriteCompensatePredicates), mvScan);
            }
            // Rewrite query by view
            rewrittenPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    rewrittenPlan, materializationContext);
            if (rewrittenPlan == null) {
                continue;
            }
            rewrittenPlan = rewriteByRules(cascadesContext, rewrittenPlan, originalPlan);
            if (!isOutputValid(originalPlan, rewrittenPlan)) {
                materializationContext.recordFailReason(queryStructInfo, Pair.of(
                        "RewrittenPlan output logical properties is different with target group",
                        String.format("planOutput logical properties = %s,\n"
                                        + "groupOutput logical properties = %s", rewrittenPlan.getLogicalProperties(),
                                originalPlan.getLogicalProperties())));
                continue;
            }
            // check the partitions used by rewritten plan is valid or not
            Set<Long> invalidPartitionsQueryUsed =
                    calcInvalidPartitions(rewrittenPlan, materializationContext, cascadesContext);
            if (!invalidPartitionsQueryUsed.isEmpty()) {
                materializationContext.recordFailReason(queryStructInfo,
                        Pair.of("Check partition query used validation fail",
                                String.format("the partition used by query is invalid by materialized view,"
                                                + "invalid partition info query used is %s",
                                        materializationContext.getMTMV().getPartitions().stream()
                                                .filter(partition ->
                                                        invalidPartitionsQueryUsed.contains(partition.getId()))
                                                .collect(Collectors.toSet()))));
                continue;
            }
            recordIfRewritten(originalPlan, materializationContext);
            rewriteResults.add(rewrittenPlan);
        }
        return rewriteResults;
    }

    protected boolean checkColumnUsedValid(StructInfo queryInfo, StructInfo mvInfo,
            SlotMapping queryToViewSlotMapping) {
        Set<ExprId> queryUsedSlotViewBased = ExpressionUtils.shuttleExpressionWithLineage(
                        queryInfo.getTopPlan().getOutput(), queryInfo.getTopPlan()).stream()
                .flatMap(expr -> ExpressionUtils.replace(expr, queryToViewSlotMapping.toSlotReferenceMap())
                        .collectToSet(each -> each instanceof Slot).stream())
                .map(each -> ((Slot) each).getExprId())
                .collect(Collectors.toSet());

        Set<ExprId> viewSet = ExpressionUtils.shuttleExpressionWithLineage(mvInfo.getTopPlan().getOutput(),
                        mvInfo.getTopPlan()).stream()
                .flatMap(expr -> expr.collectToSet(each -> each instanceof Slot).stream())
                .map(each -> ((Slot) each).getExprId())
                .collect(Collectors.toSet());
        return viewSet.containsAll(queryUsedSlotViewBased);
    }

    /**
     * Rewrite by rules and try to make output is the same after optimize by rules
     */
    protected Plan rewriteByRules(CascadesContext cascadesContext, Plan rewrittenPlan, Plan originPlan) {
        List<Slot> originOutputs = originPlan.getOutput();
        if (originOutputs.size() != rewrittenPlan.getOutput().size()) {
            return null;
        }
        Map<Slot, ExprId> originSlotToRewrittenExprId = Maps.newHashMap();
        for (int i = 0; i < originOutputs.size(); i++) {
            originSlotToRewrittenExprId.put(originOutputs.get(i), rewrittenPlan.getOutput().get(i).getExprId());
        }
        // run rbo job on mv rewritten plan
        CascadesContext rewrittenPlanContext = CascadesContext.initContext(
                cascadesContext.getStatementContext(), rewrittenPlan,
                cascadesContext.getCurrentJobContext().getRequiredProperties());
        Rewriter.getWholeTreeRewriter(rewrittenPlanContext).execute();
        rewrittenPlan = rewrittenPlanContext.getRewritePlan();

        // for get right nullable after rewritten, we need this map
        Map<ExprId, Slot> exprIdToNewRewrittenSlot = Maps.newHashMap();
        for (Slot slot : rewrittenPlan.getOutput()) {
            exprIdToNewRewrittenSlot.put(slot.getExprId(), slot);
        }

        // normalize nullable
        ImmutableList<NamedExpression> convertNullable = originOutputs.stream()
                .map(s -> normalizeExpression(s, exprIdToNewRewrittenSlot.get(originSlotToRewrittenExprId.get(s))))
                .collect(ImmutableList.toImmutableList());
        return new LogicalProject<>(convertNullable, rewrittenPlan);
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
     */
    protected Set<Long> calcInvalidPartitions(Plan rewrittenPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        // check partition is valid or not
        MTMV mtmv = materializationContext.getMTMV();
        PartitionInfo mvPartitionInfo = mtmv.getPartitionInfo();
        if (PartitionType.UNPARTITIONED.equals(mvPartitionInfo.getType())) {
            // if not partition, if rewrite success, it means mv is available
            return ImmutableSet.of();
        }
        // check mv related table partition is valid or not
        MTMVPartitionInfo mvCustomPartitionInfo = mtmv.getMvPartitionInfo();
        BaseTableInfo relatedPartitionTable = mvCustomPartitionInfo.getRelatedTableInfo();
        if (relatedPartitionTable == null) {
            return ImmutableSet.of();
        }
        // get mv valid partitions
        Set<Long> mvDataValidPartitionIdSet = MTMVRewriteUtil.getMTMVCanRewritePartitions(mtmv,
                        cascadesContext.getConnectContext(), System.currentTimeMillis()).stream()
                .map(Partition::getId)
                .collect(Collectors.toSet());
        Set<Long> queryUsedPartitionIdSet = rewrittenPlan.collectToList(node -> node instanceof LogicalOlapScan
                        && Objects.equals(((CatalogRelation) node).getTable().getName(), mtmv.getName()))
                .stream()
                .map(node -> ((LogicalOlapScan) node).getSelectedPartitionIds())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        queryUsedPartitionIdSet.removeAll(mvDataValidPartitionIdSet);
        return queryUsedPartitionIdSet;
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
            boolean targetExpressionNeedSourceBased) {
        // Firstly, rewrite the target expression using source with inverse mapping
        // then try to use the target expression to represent the query. if any of source expressions
        // can not be represented by target expressions, return null.
        // generate target to target replacement expression mapping, and change target expression to source based
        List<? extends Expression> sourceShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                sourceExpressionsToWrite, sourcePlan);
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
            context.addMatchedGroup(plan.getGroupExpression().get().getOwnerGroup().getGroupId());
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
