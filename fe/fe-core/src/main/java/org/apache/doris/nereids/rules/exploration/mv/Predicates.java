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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.EquivalenceClassMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This record the predicates which can be pulled up or some other type predicates.
 * Also contains the necessary method for predicates process
 */
public class Predicates {

    // Guard DNF expansion from exponential branch blow-up.
    private static final int MAX_DNF_BRANCHES = 1024;

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
     * collect equivalence candidates from query/view difference.
     */
    public static Map<Expression, ExpressionInfo> collectEquivalenceCandidates(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult) {
        EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass();
        EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();
        Map<SlotReference, SlotReference> viewToQuerySlotMap = viewToQuerySlotMapping.toSlotReferenceMap();
        EquivalenceClass viewEquivalenceClassQueryBased = viewEquivalenceClass.permute(viewToQuerySlotMap);
        if (viewEquivalenceClassQueryBased == null) {
            return null;
        }
        final Map<Expression, ExpressionInfo> equalCompensateConjunctions = new HashMap<>();
        if (queryEquivalenceClass.isEmpty() && viewEquivalenceClass.isEmpty()) {
            return ImmutableMap.of();
        }
        if (queryEquivalenceClass.isEmpty() && !viewEquivalenceClass.isEmpty()) {
            return null;
        }
        EquivalenceClassMapping queryToViewEquivalenceMapping =
                EquivalenceClassMapping.generate(queryEquivalenceClass, viewEquivalenceClassQueryBased);
        // can not map all target equivalence class, can not compensate
        if (queryToViewEquivalenceMapping.getEquivalenceClassSetMap().size()
                < viewEquivalenceClass.getEquivalenceSetList().size()) {
            return null;
        }
        // do equal compensate
        Set<List<SlotReference>> mappedQueryEquivalenceSet =
                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().keySet();

        for (List<SlotReference> queryEquivalenceSet : queryEquivalenceClass.getEquivalenceSetList()) {
            // compensate the equivalence in query but not in view
            if (!mappedQueryEquivalenceSet.contains(queryEquivalenceSet)) {
                Iterator<SlotReference> iterator = queryEquivalenceSet.iterator();
                SlotReference first = iterator.next();
                while (iterator.hasNext()) {
                    Expression equals = new EqualTo(first, iterator.next());
                    if (equals.anyMatch(AggregateFunction.class::isInstance)) {
                        return null;
                    }
                    equalCompensateConjunctions.put(equals, ExpressionInfo.EMPTY);
                }
            } else {
                // compensate the equivalence both in query and view, but query has more equivalence
                List<SlotReference> viewEquivalenceSet =
                        queryToViewEquivalenceMapping.getEquivalenceClassSetMap().get(queryEquivalenceSet);
                List<SlotReference> copiedQueryEquivalenceSet = new ArrayList<>(queryEquivalenceSet);
                copiedQueryEquivalenceSet.removeAll(viewEquivalenceSet);
                SlotReference first = viewEquivalenceSet.iterator().next();
                for (SlotReference slotReference : copiedQueryEquivalenceSet) {
                    Expression equals = new EqualTo(first, slotReference);
                    if (equals.anyMatch(AggregateFunction.class::isInstance)) {
                        return null;
                    }
                    equalCompensateConjunctions.put(equals, ExpressionInfo.EMPTY);
                }
            }
        }
        return equalCompensateConjunctions;
    }

    /**
     * collect range candidates from query/view difference.
     */
    public static Map<Expression, ExpressionInfo> collectRangeCandidates(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext) {
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Set<Expression> viewRangeQueryBasedSet = new HashSet<>();
        for (Expression viewExpression : viewSplitPredicate.getRangePredicateMap().keySet()) {
            viewRangeQueryBasedSet.add(
                    ExpressionUtils.replace(viewExpression, viewToQuerySlotMapping.toSlotReferenceMap()));
        }
        viewRangeQueryBasedSet.remove(BooleanLiteral.TRUE);

        Set<Expression> queryRangeSet = querySplitPredicate.getRangePredicateMap().keySet();
        queryRangeSet.remove(BooleanLiteral.TRUE);

        Set<Expression> differentExpressions = new HashSet<>();
        Sets.difference(queryRangeSet, viewRangeQueryBasedSet).copyInto(differentExpressions);
        Sets.difference(viewRangeQueryBasedSet, queryRangeSet).copyInto(differentExpressions);
        // the range predicate in query and view is same, don't need to compensate
        if (differentExpressions.isEmpty()) {
            return ImmutableMap.of();
        }
        // try to normalize the different expressions
        Set<Expression> normalizedExpressions =
                normalizeExpression(ExpressionUtils.and(differentExpressions), cascadesContext);
        if (!queryRangeSet.containsAll(normalizedExpressions)) {
            // normalized expressions is not in query, can not compensate
            return null;
        }
        Map<Expression, ExpressionInfo> normalizedExpressionsWithLiteral = new HashMap<>();
        for (Expression expression : normalizedExpressions) {
            Set<Literal> literalSet = expression.collect(expressionTreeNode -> expressionTreeNode instanceof Literal);
            if (!(expression instanceof ComparisonPredicate)
                    || (expression instanceof GreaterThan || expression instanceof LessThanEqual)
                    || literalSet.size() != 1) {
                if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                    return null;
                }
                normalizedExpressionsWithLiteral.put(expression, ExpressionInfo.EMPTY);
                continue;
            }
            if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            normalizedExpressionsWithLiteral.put(expression, new ExpressionInfo(literalSet.iterator().next()));
        }
        return ImmutableMap.copyOf(normalizedExpressionsWithLiteral);
    }

    private static Set<Expression> normalizeExpression(Expression expression, CascadesContext cascadesContext) {
        ExpressionNormalization expressionNormalization = new ExpressionNormalization();
        ExpressionOptimization expressionOptimization = new ExpressionOptimization();
        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);
        expression = expressionNormalization.rewrite(expression, context);
        expression = expressionOptimization.rewrite(expression, context);
        return ExpressionUtils.extractConjunctionToSet(expression);
    }

    /**
     * collect all predicate compensation candidates before residual finalization.
     */
    public static PredicateCompensation collectCompensationCandidates(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext) {
        Map<Expression, ExpressionInfo> equalCandidates = collectEquivalenceCandidates(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult);
        Map<Expression, ExpressionInfo> rangeCandidates = collectRangeCandidates(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult, cascadesContext);
        Map<Expression, ExpressionInfo> residualCandidates = collectResidualCandidates(queryStructInfo);
        if (equalCandidates == null || rangeCandidates == null || residualCandidates == null) {
            return null;
        }
        return new PredicateCompensation(equalCandidates, rangeCandidates, residualCandidates);
    }

    /**
     * compensate predicates in one step.
     */
    public static PredicateCompensation compensatePredicates(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext) {
        PredicateCompensation compensationCandidates = collectCompensationCandidates(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult, cascadesContext);
        if (compensationCandidates == null) {
            return null;
        }
        return compensateCandidatesByViewResidual(viewStructInfo, viewToQuerySlotMapping,
                compensationCandidates);
    }

    /**
     * Build the final predicate compensation from collected candidates.
     *
     * This step uses query-based view residual predicates to:
     * 1) validate compensation safety (candidates must imply view residual), and
     * 2) remove candidate predicates already covered by view residual.
     *
     * Returns null when compensation is unsafe or cannot be proven within DNF guard limits.
     */
    public static PredicateCompensation compensateCandidatesByViewResidual(
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            PredicateCompensation compensationCandidates) {
        try {
            return doCompensateCandidatesByViewResidual(viewStructInfo, viewToQuerySlotMapping,
                    compensationCandidates);
        } catch (DnfBranchOverflowException e) {
            // DNF branch expansion may explode exponentially; fail compensation conservatively.
            return null;
        }
    }

    private static PredicateCompensation doCompensateCandidatesByViewResidual(
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            PredicateCompensation compensationCandidates) {
        Set<Expression> viewResidualPredicatesQueryBased =
                collectViewResidualPredicates(viewStructInfo, viewToQuerySlotMapping);
        Expression combinedViewResidualQueryBased = buildCombinedPredicate(viewResidualPredicatesQueryBased);

        if (!validateCompensationByViewResidual(combinedViewResidualQueryBased,
                compensationCandidates)) {
            return null;
        }

        return new PredicateCompensation(
                removePredicatesImpliedByViewResidual(
                        compensationCandidates.getEquals(), combinedViewResidualQueryBased),
                removePredicatesImpliedByViewResidual(
                        compensationCandidates.getRanges(), combinedViewResidualQueryBased),
                removePredicatesImpliedByViewResidual(
                        compensationCandidates.getResiduals(), combinedViewResidualQueryBased));
    }

    private static Set<Expression> collectViewResidualPredicates(StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping) {
        Set<Expression> viewResidualPredicates = new LinkedHashSet<>();
        for (Expression viewExpression : viewStructInfo.getSplitPredicate().getResidualPredicateMap().keySet()) {
            Expression queryBasedExpression =
                    ExpressionUtils.replace(viewExpression, viewToQuerySlotMapping.toSlotReferenceMap());
            if (!BooleanLiteral.TRUE.equals(queryBasedExpression)) {
                viewResidualPredicates.add(queryBasedExpression);
            }
        }
        return viewResidualPredicates;
    }

    private static Map<Expression, ExpressionInfo> collectResidualCandidates(StructInfo queryStructInfo) {
        Collection<Expression> expressions = queryStructInfo.getSplitPredicate().getResidualPredicateMap().keySet();
        Map<Expression, ExpressionInfo> residualPredicates = new LinkedHashMap<>();
        for (Expression expression : expressions) {
            if (BooleanLiteral.TRUE.equals(expression)) {
                continue;
            }
            // Aggregate functions in residual predicates are not safe for detail-MV compensation.
            if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            residualPredicates.put(expression, ExpressionInfo.EMPTY);
        }
        return ImmutableMap.copyOf(residualPredicates);
    }

    private static boolean validateCompensationByViewResidual(
            Expression combinedViewResidualQueryBased,
            PredicateCompensation compensationCandidates) {
        // Safety check: combinedQueryCandidates must imply viewResidual.
        Expression combinedQueryCandidates = buildCombinedPredicate(
                compensationCandidates.getEquals().keySet(),
                compensationCandidates.getRanges().keySet(),
                compensationCandidates.getResiduals().keySet());
        return impliesByDnf(combinedQueryCandidates, combinedViewResidualQueryBased);
    }

    @SafeVarargs
    private static Expression buildCombinedPredicate(Collection<Expression>... predicateCollections) {
        List<Expression> combinedPredicates = new ArrayList<>();
        for (Collection<Expression> predicateCollection : predicateCollections) {
            for (Expression predicate : predicateCollection) {
                if (!BooleanLiteral.TRUE.equals(predicate)) {
                    combinedPredicates.add(predicate);
                }
            }
        }
        return ExpressionUtils.and(combinedPredicates);
    }

    private static Map<Expression, ExpressionInfo> removePredicatesImpliedByViewResidual(
            Map<Expression, ExpressionInfo> predicates,
            Expression viewResidual) {
        // Remove candidates already implied by the view residual predicate.
        Map<Expression, ExpressionInfo> remainingPredicates = new LinkedHashMap<>();
        for (Map.Entry<Expression, ExpressionInfo> entry : predicates.entrySet()) {
            // If viewResidual => candidate, candidate is redundant and can be dropped.
            if (!impliesByDnf(viewResidual, entry.getKey())) {
                remainingPredicates.put(entry.getKey(), entry.getValue());
            }
        }
        return ImmutableMap.copyOf(remainingPredicates);
    }

    private static boolean impliesByDnf(Expression source, Expression target) {
        // Check whether source => target.
        List<Set<Expression>> sourceBranches = extractDnfBranches(source);
        List<Set<Expression>> targetBranches = extractDnfBranches(target);
        if (sourceBranches.isEmpty()) {
            return true;
        }
        if (targetBranches.isEmpty()) {
            return false;
        }
        for (Set<Expression> sourceBranch : sourceBranches) {
            boolean branchMatched = false;
            for (Set<Expression> targetBranch : targetBranches) {
                // In DNF each branch is an AND-set.
                // sourceBranch containsAll(targetBranch) means source branch is stronger,
                // therefore source branch implies target branch.
                if (sourceBranch.containsAll(targetBranch)) {
                    branchMatched = true;
                    break;
                }
            }
            if (!branchMatched) {
                return false;
            }
        }
        return true;
    }

    private static List<Set<Expression>> extractDnfBranches(Expression expression) {
        if (BooleanLiteral.TRUE.equals(expression)) {
            List<Set<Expression>> trueBranches = new ArrayList<>();
            trueBranches.add(new LinkedHashSet<>());
            return trueBranches;
        }
        if (BooleanLiteral.FALSE.equals(expression)) {
            return ImmutableList.of();
        }
        if (expression instanceof Or) {
            List<Set<Expression>> branches = new ArrayList<>();
            for (Expression child : ExpressionUtils.extractDisjunction(expression)) {
                List<Set<Expression>> childBranches = extractDnfBranches(child);
                long expectedSize = (long) branches.size() + childBranches.size();
                if (expectedSize > MAX_DNF_BRANCHES) {
                    throw DnfBranchOverflowException.INSTANCE;
                }
                branches.addAll(childBranches);
            }
            return branches;
        }
        if (expression instanceof And) {
            // (A OR B) AND C -> {A, C}, {B, C}
            List<Set<Expression>> branches = new ArrayList<>();
            branches.add(new LinkedHashSet<>());
            for (Expression child : ExpressionUtils.extractConjunction(expression)) {
                List<Set<Expression>> childBranches = extractDnfBranches(child);
                branches = crossProductBranches(branches, childBranches);
                if (branches.isEmpty()) {
                    return branches;
                }
            }
            return branches;
        }
        List<Set<Expression>> branches = new ArrayList<>();
        Set<Expression> branch = new LinkedHashSet<>();
        branch.add(expression);
        branches.add(branch);
        return branches;
    }

    private static List<Set<Expression>> crossProductBranches(List<Set<Expression>> leftBranches,
            List<Set<Expression>> rightBranches) {
        if (leftBranches.isEmpty() || rightBranches.isEmpty()) {
            return ImmutableList.of();
        }
        long expectedSize = (long) leftBranches.size() * rightBranches.size();
        if (expectedSize > MAX_DNF_BRANCHES) {
            throw DnfBranchOverflowException.INSTANCE;
        }
        List<Set<Expression>> mergedBranches = new ArrayList<>((int) expectedSize);
        for (Set<Expression> leftBranch : leftBranches) {
            for (Set<Expression> rightBranch : rightBranches) {
                Set<Expression> mergedBranch = new LinkedHashSet<>(leftBranch);
                mergedBranch.addAll(rightBranch);
                mergedBranches.add(mergedBranch);
            }
        }
        return mergedBranches;
    }

    private static final class DnfBranchOverflowException extends RuntimeException {
        private static final DnfBranchOverflowException INSTANCE = new DnfBranchOverflowException();

        private DnfBranchOverflowException() {
            super(null, null, true, false);
        }
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
     * Predicate compensation result holding equals, ranges and residuals.
     * Used both as intermediate candidates and as final compensation output.
     */
    public static final class PredicateCompensation {
        private final Map<Expression, ExpressionInfo> equals;
        private final Map<Expression, ExpressionInfo> ranges;
        private final Map<Expression, ExpressionInfo> residuals;

        public PredicateCompensation(Map<Expression, ExpressionInfo> equals,
                Map<Expression, ExpressionInfo> ranges,
                Map<Expression, ExpressionInfo> residuals) {
            this.equals = ImmutableMap.copyOf(equals);
            this.ranges = ImmutableMap.copyOf(ranges);
            this.residuals = ImmutableMap.copyOf(residuals);
        }

        public Map<Expression, ExpressionInfo> getEquals() {
            return equals;
        }

        public Map<Expression, ExpressionInfo> getRanges() {
            return ranges;
        }

        public Map<Expression, ExpressionInfo> getResiduals() {
            return residuals;
        }

        public SplitPredicate toSplitPredicate() {
            return SplitPredicate.of(equals, ranges, residuals);
        }

        @Override
        public String toString() {
            return Utils.toSqlString("PredicateCompensation",
                    "equals", equals, "ranges", ranges, "residuals", residuals);
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
