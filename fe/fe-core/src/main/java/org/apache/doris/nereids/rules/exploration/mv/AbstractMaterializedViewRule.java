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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.mapping.EquivalenceClassSetMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule {

    /**
     * The abstract template method for query rewrite, it contains the main logic and different query
     * pattern should override the sub logic.
     */
    protected List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        List<Plan> rewriteResults = new ArrayList<>();
        if (materializationContexts.isEmpty()) {
            return rewriteResults;
        }

        List<StructInfo> queryStructInfos = extractStructInfo(queryPlan, cascadesContext);
        // TODO Just Check query queryPlan firstly, support multi later.
        StructInfo queryStructInfo = queryStructInfos.get(0);
        if (!checkPattern(queryStructInfo)) {
            return rewriteResults;
        }

        for (MaterializationContext materializationContext : materializationContexts) {
            // already rewrite, bail out
            if (queryPlan.getGroupExpression().isPresent()
                    && materializationContext.alreadyRewrite(
                    queryPlan.getGroupExpression().get().getOwnerGroup().getGroupId())) {
                continue;
            }
            Plan mvPlan = materializationContext.getMtmv().getMvCache().getLogicalPlan();
            List<StructInfo> viewStructInfos = extractStructInfo(mvPlan, cascadesContext);
            if (viewStructInfos.size() > 1) {
                // view struct info should only have one
                return rewriteResults;
            }
            StructInfo viewStructInfo = viewStructInfos.get(0);
            if (!checkPattern(viewStructInfo)) {
                continue;
            }
            MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations());
            if (MatchMode.COMPLETE != matchMode) {
                continue;
            }
            List<RelationMapping> queryToViewTableMappings =
                    RelationMapping.generate(queryStructInfo.getRelations(), viewStructInfo.getRelations());
            if (queryToViewTableMappings == null) {
                return rewriteResults;
            }
            for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
                SlotMapping queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
                if (queryToViewSlotMapping == null) {
                    continue;
                }
                LogicalCompatibilityContext compatibilityContext =
                        LogicalCompatibilityContext.from(queryToViewTableMapping, queryToViewSlotMapping,
                                queryStructInfo, viewStructInfo);
                // todo outer join compatibility check
                if (!StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo, compatibilityContext)) {
                    continue;
                }
                SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                        queryToViewSlotMapping);
                // Can not compensate, bail out
                if (compensatePredicates.isEmpty()) {
                    continue;
                }
                Plan rewritedPlan;
                Plan mvScan = materializationContext.getMvScanPlan();
                if (compensatePredicates.isAlwaysTrue()) {
                    rewritedPlan = mvScan;
                } else {
                    // Try to rewrite compensate predicates by using mv scan
                    List<Expression> rewriteCompensatePredicates = rewriteExpression(
                            compensatePredicates.toList(),
                            materializationContext.getViewExpressionIndexMapping(),
                            queryToViewSlotMapping);
                    if (rewriteCompensatePredicates.isEmpty()) {
                        continue;
                    }
                    rewritedPlan = new LogicalFilter<>(Sets.newHashSet(rewriteCompensatePredicates), mvScan);
                }
                // Rewrite query by view
                rewritedPlan = rewriteQueryByView(matchMode,
                        queryStructInfo,
                        viewStructInfo,
                        queryToViewSlotMapping,
                        rewritedPlan,
                        materializationContext);
                if (rewritedPlan == null) {
                    continue;
                }
                rewriteResults.add(rewritedPlan);
            }
        }
        return rewriteResults;
    }

    /**
     * Rewrite query by view, for aggregate or join rewriting should be different inherit class implementation
     */
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping queryToViewSlotMappings,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext) {
        return tempRewritedPlan;
    }

    /**
     * Use target output expression to represent the source expression
     */
    protected List<Expression> rewriteExpression(
            List<? extends Expression> sourceExpressionsToWrite,
            ExpressionMapping mvExpressionToMvScanExpressionMapping,
            SlotMapping sourceToTargetMapping) {
        // Firstly, rewrite the target plan output expression using query with inverse mapping
        // then try to use the mv expression to represent the query. if any of source expressions
        // can not be represented by mv, return null
        //
        // example as following:
        //     source                           target
        //        project(slot 1, 2)              project(slot 3, 2, 1)
        //          scan(table)                        scan(table)
        //
        //     transform source to:
        //        project(slot 2, 1)
        //            target
        // generate mvSql to mvScan mvExpressionToMvScanExpressionMapping, and change mv sql expression to query based
        ExpressionMapping expressionMappingKeySourceBased =
                mvExpressionToMvScanExpressionMapping.keyPermute(sourceToTargetMapping.inverse());
        List<Map<? extends Expression, ? extends Expression>> flattenExpressionMap =
                expressionMappingKeySourceBased.flattenMap();
        // view to view scan expression is 1:1 so get first element
        Map<? extends Expression, ? extends Expression> mvSqlToMvScanMappingQueryBased = flattenExpressionMap.get(0);

        List<Expression> rewrittenExpressions = new ArrayList<>();
        for (Expression expressionToRewrite : sourceExpressionsToWrite) {
            if (expressionToRewrite instanceof Literal) {
                rewrittenExpressions.add(expressionToRewrite);
                continue;
            }
            final Set<Object> slotsToRewrite =
                    expressionToRewrite.collectToSet(expression -> expression instanceof Slot);
            boolean wiAlias = expressionToRewrite instanceof NamedExpression;
            Expression replacedExpression = ExpressionUtils.replace(expressionToRewrite,
                    mvSqlToMvScanMappingQueryBased,
                    wiAlias);
            if (replacedExpression.anyMatch(slotsToRewrite::contains)) {
                // if contains any slot to rewrite, which means can not be rewritten by target, bail out
                return null;
            }
            rewrittenExpressions.add(replacedExpression);
        }
        return rewrittenExpressions;
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
            SlotMapping queryToViewSlotMapping
    ) {
        EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass();
        EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();
        // viewEquivalenceClass to query based
        Map<SlotReference, SlotReference> viewToQuerySlotMapping = queryToViewSlotMapping.inverse()
                .toSlotReferenceMap();
        EquivalenceClass viewEquivalenceClassQueryBased = viewEquivalenceClass.permute(viewToQuerySlotMapping);
        if (viewEquivalenceClassQueryBased == null) {
            return SplitPredicate.empty();
        }
        final List<Expression> equalCompensateConjunctions = new ArrayList<>();
        if (queryEquivalenceClass.isEmpty() && viewEquivalenceClass.isEmpty()) {
            equalCompensateConjunctions.add(BooleanLiteral.of(true));
        }
        if (queryEquivalenceClass.isEmpty()
                && !viewEquivalenceClass.isEmpty()) {
            return SplitPredicate.empty();
        }
        EquivalenceClassSetMapping queryToViewEquivalenceMapping =
                EquivalenceClassSetMapping.generate(queryEquivalenceClass, viewEquivalenceClassQueryBased);
        // can not map all target equivalence class, can not compensate
        if (queryToViewEquivalenceMapping.getEquivalenceClassSetMap().size()
                < viewEquivalenceClass.getEquivalenceSetList().size()) {
            return SplitPredicate.empty();
        }
        // do equal compensate
        Set<Set<SlotReference>> mappedQueryEquivalenceSet =
                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().keySet();
        queryEquivalenceClass.getEquivalenceSetList().forEach(
                queryEquivalenceSet -> {
                    // compensate the equivalence in query but not in view
                    if (!mappedQueryEquivalenceSet.contains(queryEquivalenceSet)) {
                        Iterator<SlotReference> iterator = queryEquivalenceSet.iterator();
                        SlotReference first = iterator.next();
                        while (iterator.hasNext()) {
                            Expression equals = new EqualTo(first, iterator.next());
                            equalCompensateConjunctions.add(equals);
                        }
                    } else {
                        // compensate the equivalence both in query and view, but query has more equivalence
                        Set<SlotReference> viewEquivalenceSet =
                                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().get(queryEquivalenceSet);
                        Set<SlotReference> copiedQueryEquivalenceSet = new HashSet<>(queryEquivalenceSet);
                        copiedQueryEquivalenceSet.removeAll(viewEquivalenceSet);
                        SlotReference first = viewEquivalenceSet.iterator().next();
                        for (SlotReference slotReference : copiedQueryEquivalenceSet) {
                            Expression equals = new EqualTo(first, slotReference);
                            equalCompensateConjunctions.add(equals);
                        }
                    }
                }
        );
        // TODO range predicates and residual predicates compensate, Simplify implementation.
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        // range compensate
        List<Expression> rangeCompensate = new ArrayList<>();
        Expression queryRangePredicate = querySplitPredicate.getRangePredicate();
        Expression viewRangePredicate = viewSplitPredicate.getRangePredicate();
        Expression viewRangePredicateQueryBased =
                ExpressionUtils.replace(viewRangePredicate, viewToQuerySlotMapping);

        Set<Expression> queryRangeSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(queryRangePredicate));
        Set<Expression> viewRangeQueryBasedSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(viewRangePredicateQueryBased));
        // query range predicate can not contain all view range predicate when view have range predicate, bail out
        if (!viewRangePredicateQueryBased.equals(BooleanLiteral.TRUE)
                && !queryRangeSet.containsAll(viewRangeQueryBasedSet)) {
            return SplitPredicate.empty();
        }
        queryRangeSet.removeAll(viewRangeQueryBasedSet);
        rangeCompensate.addAll(queryRangeSet);

        // residual compensate
        List<Expression> residualCompensate = new ArrayList<>();
        Expression queryResidualPredicate = querySplitPredicate.getResidualPredicate();
        Expression viewResidualPredicate = viewSplitPredicate.getResidualPredicate();
        Expression viewResidualPredicateQueryBased =
                ExpressionUtils.replace(viewResidualPredicate, viewToQuerySlotMapping);
        Set<Expression> queryResidualSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(queryResidualPredicate));
        Set<Expression> viewResidualQueryBasedSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(viewResidualPredicateQueryBased));
        // query residual predicate can not contain all view residual predicate when view have residual predicate,
        // bail out
        if (!viewResidualPredicateQueryBased.equals(BooleanLiteral.TRUE)
                && !queryResidualSet.containsAll(viewResidualQueryBasedSet)) {
            return SplitPredicate.empty();
        }
        queryResidualSet.removeAll(viewResidualQueryBasedSet);
        residualCompensate.addAll(queryResidualSet);

        return SplitPredicate.of(ExpressionUtils.and(equalCompensateConjunctions),
                rangeCompensate.isEmpty() ? BooleanLiteral.of(true) : ExpressionUtils.and(rangeCompensate),
                residualCompensate.isEmpty() ? BooleanLiteral.of(true) : ExpressionUtils.and(residualCompensate));
    }

    /**
     * Decide the match mode
     *
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations) {
        List<TableIf> queryTableRefs = queryRelations
                .stream()
                .map(CatalogRelation::getTable)
                .collect(Collectors.toList());
        List<TableIf> viewTableRefs = viewRelations
                .stream()
                .map(CatalogRelation::getTable)
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
     * Extract struct info from plan, support to get struct info from logical plan or plan in group.
     */
    protected List<StructInfo> extractStructInfo(Plan plan, CascadesContext cascadesContext) {
        if (plan.getGroupExpression().isPresent()
                && !plan.getGroupExpression().get().getOwnerGroup().getStructInfos().isEmpty()) {
            return plan.getGroupExpression().get().getOwnerGroup().getStructInfos();
        } else {
            // build struct info and add them to current group
            List<StructInfo> structInfos = StructInfo.of(plan);
            if (plan.getGroupExpression().isPresent()) {
                plan.getGroupExpression().get().getOwnerGroup().addStructInfo(structInfos);
            }
            return structInfos;
        }
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
