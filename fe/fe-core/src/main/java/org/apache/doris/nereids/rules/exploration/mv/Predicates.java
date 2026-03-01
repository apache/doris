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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.EquivalenceClassMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This record the predicates which can be pulled up or some other type predicates.
 * Also contains the necessary method for predicates process
 */
public class Predicates {

    // Predicates that can be pulled up
    private final Set<Expression> pulledUpPredicates;
    // Predicates that can not be pulled up, should be equals between query and view
    private final Set<Expression> couldNotPulledUpPredicates;

    public Predicates(Set<Expression> pulledUpPredicates, Set<Expression> couldNotPulledUpPredicates) {
        this.pulledUpPredicates = pulledUpPredicates;
        this.couldNotPulledUpPredicates = couldNotPulledUpPredicates;
    }

    public static Predicates of(Set<Expression> pulledUpPredicates, Set<Expression> predicatesUnderBreaker) {
        return new Predicates(pulledUpPredicates, predicatesUnderBreaker);
    }

    public Set<Expression> getPulledUpPredicates() {
        return pulledUpPredicates;
    }

    public Set<Expression> getCouldNotPulledUpPredicates() {
        return couldNotPulledUpPredicates;
    }

    public Predicates mergePulledUpPredicates(Collection<Expression> predicates) {
        Set<Expression> mergedPredicates = new HashSet<>(predicates);
        mergedPredicates.addAll(this.pulledUpPredicates);
        return new Predicates(mergedPredicates, this.couldNotPulledUpPredicates);
    }

    /**
     * Split the expression to equal, range and residual predicate.
     */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSplitter predicatesSplit = new PredicatesSplitter(expression);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * try to compensate could not pull up predicates
     */
    public static Map<Expression, ExpressionInfo> compensateCouldNotPullUpPredicates(
            StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, ComparisonResult comparisonResult) {

        Predicates queryStructInfoPredicates = queryStructInfo.getPredicates();
        Predicates viewStructInfoPredicates = viewStructInfo.getPredicates();
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return ImmutableMap.of();
        }
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && !viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return null;
        }
        if (!queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return null;
        }

        List<? extends Expression> viewPredicatesShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                Lists.newArrayList(viewStructInfoPredicates.getCouldNotPulledUpPredicates()),
                viewStructInfo.getTopPlan());
        List<Expression> viewPredicatesQueryBased = ExpressionUtils.replace((List<Expression>) viewPredicatesShuttled,
                viewToQuerySlotMapping.toSlotReferenceMap());
        // could not be pulled up predicates in query and view should be same
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().equals(
                Sets.newHashSet(viewPredicatesQueryBased))) {
            return ImmutableMap.of();
        }
        return null;
    }

    /**
     * Compensate equivalence predicates based on equivalence classes.
     * Collects uncovered equivalence predicates into uncoveredEquals for residual compensation.
     */
    public static Map<Expression, ExpressionInfo> compensateEquivalence(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            Set<Expression> uncoveredEquals) {
        EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass();
        EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();
        Map<SlotReference, SlotReference> viewToQuerySlotMap = viewToQuerySlotMapping.toSlotReferenceMap();
        EquivalenceClass viewEquivalenceClassQueryBased = viewEquivalenceClass.permute(viewToQuerySlotMap);
        if (viewEquivalenceClassQueryBased == null) {
            return null;
        }
        if (queryEquivalenceClass.isEmpty() && viewEquivalenceClass.isEmpty()) {
            return ImmutableMap.of();
        }
        EquivalenceClassMapping queryToViewEquivalenceMapping =
                EquivalenceClassMapping.generate(queryEquivalenceClass, viewEquivalenceClassQueryBased);
        if (queryToViewEquivalenceMapping.getEquivalenceClassSetMap().size()
                < viewEquivalenceClass.getEquivalenceSetList().size()) {
            return null;
        }
        Map<Expression, ExpressionInfo> compensations = new HashMap<>();
        Set<List<SlotReference>> mappedQueryEquivalenceSet =
                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().keySet();

        for (List<SlotReference> queryEquivalenceSet : queryEquivalenceClass.getEquivalenceSetList()) {
            // equality condition in query is not covered, add to uncoveredEquals
            if (!mappedQueryEquivalenceSet.contains(queryEquivalenceSet)) {
                SlotReference first = queryEquivalenceSet.get(0);
                queryEquivalenceSet.stream()
                        .skip(1)
                        .map(slot -> new EqualTo(first, slot))
                        .forEach(uncoveredEquals::add);
            } else {
                List<SlotReference> viewEquivalenceSet =
                        queryToViewEquivalenceMapping.getEquivalenceClassSetMap().get(queryEquivalenceSet);
                List<SlotReference> queryExtraSlots = new ArrayList<>(queryEquivalenceSet);
                queryExtraSlots.removeAll(viewEquivalenceSet);

                SlotReference firstViewSlot = viewEquivalenceSet.get(0);
                for (SlotReference extraSlot : queryExtraSlots) {
                    Expression equals = new EqualTo(firstViewSlot, extraSlot);
                    if (equals.anyMatch(AggregateFunction.class::isInstance)) {
                        return null;
                    }
                    compensations.put(equals, ExpressionInfo.EMPTY);
                }
            }
        }
        return compensations;
    }

    /**
     * Compensate range predicates.
     * Collects uncovered range predicates into uncoveredRanges for residual compensation.
     */
    public static Map<Expression, ExpressionInfo> compensateRangePredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext,
            Set<Expression> uncoveredRanges) {
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Map<SlotReference, SlotReference> slotMap = viewToQuerySlotMapping.toSlotReferenceMap();
        Set<Expression> viewRangeQueryBasedSet = viewSplitPredicate.getRangePredicateMap().keySet().stream()
                .filter(expr -> !ExpressionUtils.isInferred(expr))
                .map(expr -> ExpressionUtils.replace(expr, slotMap))
                .filter(expr -> expr != BooleanLiteral.TRUE)
                .collect(ImmutableSet.toImmutableSet());

        Set<Expression> queryRangeSet = querySplitPredicate.getRangePredicateMap().keySet().stream()
                .filter(expr -> !ExpressionUtils.isInferred(expr))
                .collect(ImmutableSet.toImmutableSet());

        // TODO: Seems already normalized. Is further normalization necessary?
        Set<Expression> normalizedViewRange = normalizeExpressionSet(viewRangeQueryBasedSet, cascadesContext);
        Set<Expression> normalizedQueryRange = normalizeExpressionSet(queryRangeSet, cascadesContext);

        if (!Sets.difference(normalizedViewRange, normalizedQueryRange).isEmpty()) {
            return null;
        }

        uncoveredRanges.addAll(Sets.difference(normalizedQueryRange, normalizedViewRange));
        return ImmutableMap.of();
    }

    /**
     * Compensate residual predicates with extra query residuals (uncovered equal/range predicates).
     * Supports OR branch matching. For example, if MV has predicate (id = 5 OR id > 10 OR id = 2)
     * and query has (id = 5 OR id = 2) AND score = 1, the query matches MV's OR predicate.
     * Compensation NOT(id > 10) + score = 1 is generated.
     */
    public static Map<Expression, ExpressionInfo> compensateResidualPredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            Set<Expression> extraQueryResiduals) {
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Map<SlotReference, SlotReference> slotMap = viewToQuerySlotMapping.toSlotReferenceMap();
        Set<Expression> viewResidualQueryBasedSet = viewSplitPredicate.getResidualPredicateMap().keySet().stream()
                .filter(expr -> !ExpressionUtils.isInferred(expr))
                .map(expr -> ExpressionUtils.replace(expr, slotMap))
                .filter(expr -> expr != BooleanLiteral.TRUE)
                .collect(ImmutableSet.toImmutableSet());

        Set<Expression> queryResidualSet = new HashSet<>(querySplitPredicate.getResidualPredicateMap().keySet().stream()
                .filter(expr -> !ExpressionUtils.isInferred(expr))
                .collect(ImmutableSet.toImmutableSet()));
        if (extraQueryResiduals != null) {
            queryResidualSet.addAll(extraQueryResiduals);
        }

        Set<Expression> compensations = coverResidualSets(viewResidualQueryBasedSet, queryResidualSet);
        if (compensations == null) {
            return null;
        }

        Map<Expression, ExpressionInfo> result = new HashMap<>();
        for (Expression expr : compensations) {
            if (expr.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            Set<Literal> literalSet = expr.collect(node -> node instanceof Literal);
            ExpressionInfo info = ExpressionInfo.EMPTY;
            if (expr instanceof ComparisonPredicate
                    && !(expr instanceof GreaterThan || expr instanceof LessThanEqual)
                    && literalSet.size() == 1) {
                info = new ExpressionInfo(literalSet.iterator().next());
            }
            result.put(expr, info);
        }
        return ImmutableMap.copyOf(result);
    }

    /**
     * Check if MV residual predicates can cover query residual predicates, return compensation expressions.
     * <p>
     * Example:
     * MV residuals: [(id = 5 OR id > 10 OR id = 2)]
     * Query residuals: [(id = 5 OR id = 2), (score = 1)]
     * <p>
     * Process:
     * 1. (id = 5 OR id > 10 OR id = 2) matches (id = 5 OR id = 2) → compensation: NOT(id > 10)
     * 2. (score = 1) is uncovered → added to compensation
     * <p>
     * Result compensation = NOT(MV extra OR branches) + uncovered query residuals = NOT(id > 10) + score = 1
     */
    private static Set<Expression> coverResidualSets(Set<Expression> viewResidualSet,
            Set<Expression> queryResidualSet) {
        Set<Expression> coveredQueryResiduals = new HashSet<>();
        Set<Expression> compensations = new HashSet<>();

        for (Expression viewResidual : viewResidualSet) {
            Pair<Expression, Set<Expression>> result = coverSingleResidual(viewResidual, queryResidualSet);
            if (result == null) {
                return null;
            }
            if (result.first != null) {
                coveredQueryResiduals.add(result.first);
            }
            compensations.addAll(result.second);
        }

        compensations.addAll(Sets.difference(queryResidualSet, coveredQueryResiduals));
        return compensations;
    }

    /**
     * Check if a single MV residual expression can be covered by query residual expressions.
     * Uses a pass-through strategy: when query OR is a proper subset of MV OR, the MV data is
     * a superset, so the view residual is satisfied. The query's OR predicate is NOT consumed
     * and will remain as a filter on the MV scan. This preserves the original predicate structure
     * for nested MV rewrite.
     *
     * @return Pair(consumed query expression or null, compensation set), or null if cannot match
     */
    private static Pair<Expression, Set<Expression>> coverSingleResidual(
            Expression viewResidual, Set<Expression> queryResidualSet) {
        Set<Expression> mvBranches = ImmutableSet.copyOf(ExpressionUtils.extractDisjunction(viewResidual));

        for (Expression queryResidual : queryResidualSet) {
            Set<Expression> queryResidualBranches = ImmutableSet.copyOf(
                    ExpressionUtils.extractDisjunction(queryResidual));
            if (mvBranches.equals(queryResidualBranches)) {
                return Pair.of(queryResidual, ImmutableSet.of());
            }
            if (mvBranches.containsAll(queryResidualBranches)) {
                return Pair.of(null, ImmutableSet.of());
            }
        }
        return null;
    }

    private static Set<Expression> normalizeExpressionSet(Set<Expression> expressions,
            CascadesContext cascadesContext) {
        // ExpressionUtils.and(empty) returns BooleanLiteral.TRUE, which breaks Sets.difference logic
        // So we return empty set directly to avoid this issue
        if (expressions.isEmpty()) {
            return ImmutableSet.of();
        }
        Expression expression = ExpressionUtils.and(expressions);
        ExpressionNormalization expressionNormalization = new ExpressionNormalization();
        ExpressionOptimization expressionOptimization = new ExpressionOptimization();
        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);
        expression = expressionNormalization.rewrite(expression, context);
        expression = expressionOptimization.rewrite(expression, context);
        return ExpressionUtils.extractConjunctionToSet(expression);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("Predicates", "pulledUpPredicates", pulledUpPredicates,
                "predicatesUnderBreaker", couldNotPulledUpPredicates);
    }

    /**
     * The struct info for expression, such as the constant that it used
     */
    public static final class ExpressionInfo {

        public static final ExpressionInfo EMPTY = new ExpressionInfo(null);

        public final Literal literal;

        public ExpressionInfo(Literal literal) {
            this.literal = literal;
        }
    }

    /**
     * The split different representation for predicate expression, such as equal, range and residual predicate.
     */
    public static final class SplitPredicate {
        public static final SplitPredicate INVALID_INSTANCE =
                SplitPredicate.of(null, null, null);
        private final Map<Expression, ExpressionInfo> equalPredicateMap;
        private final Map<Expression, ExpressionInfo> rangePredicateMap;
        private final Map<Expression, ExpressionInfo> residualPredicateMap;

        public SplitPredicate(Map<Expression, ExpressionInfo> equalPredicateMap,
                Map<Expression, ExpressionInfo> rangePredicateMap,
                Map<Expression, ExpressionInfo> residualPredicateMap) {
            this.equalPredicateMap = equalPredicateMap;
            this.rangePredicateMap = rangePredicateMap;
            this.residualPredicateMap = residualPredicateMap;
        }

        public Map<Expression, ExpressionInfo> getEqualPredicateMap() {
            return equalPredicateMap;
        }

        public Map<Expression, ExpressionInfo> getRangePredicateMap() {
            return rangePredicateMap;
        }

        public Map<Expression, ExpressionInfo> getResidualPredicateMap() {
            return residualPredicateMap;
        }

        /**
         * SplitPredicate construct
         */
        public static SplitPredicate of(Map<Expression, ExpressionInfo> equalPredicateMap,
                Map<Expression, ExpressionInfo> rangePredicateMap,
                Map<Expression, ExpressionInfo> residualPredicateMap) {
            return new SplitPredicate(equalPredicateMap, rangePredicateMap, residualPredicateMap);
        }

        /**
         * Check the predicates are invalid or not. If any of the predicates is null, it is invalid.
         */
        public boolean isInvalid() {
            return Objects.equals(this, INVALID_INSTANCE);
        }

        /**
         * Get expression list in predicates
         */
        public List<Expression> toList() {
            if (isInvalid()) {
                return ImmutableList.of();
            }
            List<Expression> flattenExpressions = new ArrayList<>(getEqualPredicateMap().keySet());
            flattenExpressions.addAll(getRangePredicateMap().keySet());
            flattenExpressions.addAll(getResidualPredicateMap().keySet());
            return flattenExpressions;
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            return getEqualPredicateMap().isEmpty() && getRangePredicateMap().isEmpty()
                    && getResidualPredicateMap().isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitPredicate that = (SplitPredicate) o;
            return Objects.equals(equalPredicateMap, that.equalPredicateMap)
                    && Objects.equals(rangePredicateMap, that.residualPredicateMap)
                    && Objects.equals(residualPredicateMap, that.residualPredicateMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(equalPredicateMap, rangePredicateMap, residualPredicateMap);
        }

        @Override
        public String toString() {
            return Utils.toSqlString("SplitPredicate",
                    "equalPredicate", equalPredicateMap,
                    "rangePredicate", rangePredicateMap,
                    "residualPredicate", residualPredicateMap);
        }
    }
}
