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
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
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
import java.util.stream.Collectors;

/**
 * This record the predicates which can be pulled up or some other type predicates.
 * Also contains the necessary method for predicates process
 */
public class Predicates {

    // Guard DNF expansion from exponential branch blow-up.
    private static final int MAX_DNF_BRANCHES = 1024;
    private static final List<PredicateImplicationRule> PREDICATE_IMPLICATION_RULES = ImmutableList.of(
            new SameExpressionImplicationRule(),
            new ComparablePredicateImplicationRule());

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
        List<Expression> queryBasedViewPredicates = ExpressionUtils.replace((List<Expression>) viewPredicatesShuttled,
                viewToQuerySlotMapping.toSlotReferenceMap());
        // could not be pulled up predicates in query and view should be same
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().equals(
                Sets.newHashSet(queryBasedViewPredicates))) {
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
        EquivalenceClass queryBasedViewEquivalenceClass = viewEquivalenceClass.permute(viewToQuerySlotMap);
        if (queryBasedViewEquivalenceClass == null) {
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
                EquivalenceClassMapping.generate(queryEquivalenceClass, queryBasedViewEquivalenceClass);
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

        Set<Expression> queryBasedViewRangeSet = collectNonInferredQueryBasedExpressions(
                viewSplitPredicate.getRangePredicateMap().keySet(), viewToQuerySlotMapping);
        Set<Expression> queryRangeSet = collectNonInferredExpressions(
                querySplitPredicate.getRangePredicateMap().keySet());

        Set<Expression> differentExpressions = new HashSet<>();
        Sets.difference(queryRangeSet, queryBasedViewRangeSet).copyInto(differentExpressions);
        Sets.difference(queryBasedViewRangeSet, queryRangeSet).copyInto(differentExpressions);
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
        Map<Expression, ExpressionInfo> normalizedExpressionsWithLiteral = new LinkedHashMap<>();
        for (Expression expression : normalizedExpressions) {
            if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            normalizedExpressionsWithLiteral.put(expression, buildRangeExpressionInfo(expression));
        }
        return ImmutableMap.copyOf(normalizedExpressionsWithLiteral);
    }

    private static ExpressionInfo buildRangeExpressionInfo(Expression expression) {
        Set<Literal> literalSet = expression.collect(expressionTreeNode -> expressionTreeNode instanceof Literal);
        if (!(expression instanceof ComparisonPredicate)
                || expression instanceof GreaterThan
                || expression instanceof LessThanEqual
                || literalSet.size() != 1) {
            return ExpressionInfo.EMPTY;
        }
        return new ExpressionInfo(literalSet.iterator().next());
    }

    private static Set<Expression> normalizeExpression(Expression expression, CascadesContext cascadesContext) {
        ExpressionNormalization expressionNormalization = new ExpressionNormalization();
        ExpressionOptimization expressionOptimization = new ExpressionOptimization();
        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);
        expression = expressionNormalization.rewrite(expression, context);
        expression = expressionOptimization.rewrite(expression, context);
        return ExpressionUtils.extractConjunctionToSet(expression);
    }

    /** Collect all predicate compensation candidates before residual finalization. */
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

    /** Compensate predicates in one step. */
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
        Set<Expression> queryBasedViewResidualPredicates = collectNonInferredQueryBasedExpressions(
                viewStructInfo.getSplitPredicate().getResidualPredicateMap().keySet(), viewToQuerySlotMapping);
        Set<Expression> exactCoveredPredicates = new LinkedHashSet<>();
        exactCoveredPredicates.addAll(Sets.intersection(
                compensationCandidates.getEquals().keySet(), queryBasedViewResidualPredicates));
        exactCoveredPredicates.addAll(Sets.intersection(
                compensationCandidates.getRanges().keySet(), queryBasedViewResidualPredicates));
        exactCoveredPredicates.addAll(Sets.intersection(
                compensationCandidates.getResiduals().keySet(), queryBasedViewResidualPredicates));
        Set<Expression> remainingQueryBasedViewResidualPredicates = new LinkedHashSet<>(
                Sets.difference(queryBasedViewResidualPredicates, exactCoveredPredicates));

        // Exact-covered predicates are enforced by both query and view. Preserve that fast path before
        // proving implication for the remaining residuals, because DNF expansion is only needed for
        // non-exact implication.
        PredicateCompensation exactPrunedCompensationCandidates = new PredicateCompensation(
                removeExactCoveredPredicates(compensationCandidates.getEquals(), exactCoveredPredicates),
                removeExactCoveredPredicates(compensationCandidates.getRanges(), exactCoveredPredicates),
                removeExactCoveredPredicates(compensationCandidates.getResiduals(), exactCoveredPredicates));

        Expression combinedCompensationCandidates = buildCombinedPredicate(
                exactPrunedCompensationCandidates.getEquals().keySet(),
                exactPrunedCompensationCandidates.getRanges().keySet(),
                exactPrunedCompensationCandidates.getResiduals().keySet());
        Expression combinedQueryBasedViewResidual = buildCombinedPredicate(remainingQueryBasedViewResidualPredicates);
        if (BooleanLiteral.TRUE.equals(combinedQueryBasedViewResidual)) {
            // The target residual is TRUE, so implication always holds and DNF expansion is unnecessary.
            return rejectUnsafeResidualCompensation(exactPrunedCompensationCandidates);
        }

        try {
            // The compensation must not widen the view result:
            // compensationCandidates => combinedQueryBasedViewResidual.
            if (!impliesByDnf(combinedCompensationCandidates, combinedQueryBasedViewResidual)) {
                return null;
            }

            PredicateCompensation finalCompensation = new PredicateCompensation(
                    removePredicatesImpliedByViewResidual(
                            exactPrunedCompensationCandidates.getEquals(), combinedQueryBasedViewResidual),
                    removePredicatesImpliedByViewResidual(
                            exactPrunedCompensationCandidates.getRanges(), combinedQueryBasedViewResidual),
                    removePredicatesImpliedByViewResidual(
                            exactPrunedCompensationCandidates.getResiduals(), combinedQueryBasedViewResidual));
            return rejectUnsafeResidualCompensation(finalCompensation);
        } catch (DnfBranchOverflowException e) {
            // DNF branch expansion may explode exponentially; fail compensation conservatively.
            return null;
        }
    }

    private static Map<Expression, ExpressionInfo> collectResidualCandidates(StructInfo queryStructInfo) {
        Set<Expression> expressions = collectNonInferredExpressions(
                queryStructInfo.getSplitPredicate().getResidualPredicateMap().keySet());
        Map<Expression, ExpressionInfo> residualCandidates = new LinkedHashMap<>();
        for (Expression expression : expressions) {
            residualCandidates.put(expression, ExpressionInfo.EMPTY);
        }
        return ImmutableMap.copyOf(residualCandidates);
    }

    private static PredicateCompensation rejectUnsafeResidualCompensation(PredicateCompensation compensation) {
        for (Expression expression : compensation.getResiduals().keySet()) {
            if (expression.anyMatch(WindowExpression.class::isInstance)
                    || expression.anyMatch(AggregateFunction.class::isInstance)) {
                // Aggregate and window residuals are not safe as detail-MV compensation predicates.
                return null;
            }
        }
        return compensation;
    }

    private static Set<Expression> collectNonInferredExpressions(Collection<Expression> expressions) {
        return expressions.stream()
                .filter(expression -> !ExpressionUtils.isInferred(expression))
                .filter(expression -> !BooleanLiteral.TRUE.equals(expression))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<Expression> collectNonInferredQueryBasedExpressions(Collection<Expression> expressions,
            SlotMapping viewToQuerySlotMapping) {
        Map<SlotReference, SlotReference> slotReferenceMap = viewToQuerySlotMapping.toSlotReferenceMap();
        return expressions.stream()
                .filter(expression -> !ExpressionUtils.isInferred(expression))
                .map(expression -> ExpressionUtils.replace(expression, slotReferenceMap))
                .filter(expression -> !BooleanLiteral.TRUE.equals(expression))
                .collect(Collectors.toCollection(LinkedHashSet::new));
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

    private static Map<Expression, ExpressionInfo> removeExactCoveredPredicates(
            Map<Expression, ExpressionInfo> predicates,
            Set<Expression> exactCoveredPredicates) {
        Map<Expression, ExpressionInfo> remainingPredicates = new LinkedHashMap<>();
        for (Map.Entry<Expression, ExpressionInfo> entry : predicates.entrySet()) {
            if (exactCoveredPredicates.contains(entry.getKey())) {
                continue;
            }
            remainingPredicates.put(entry.getKey(), entry.getValue());
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
                if (branchImplies(sourceBranch, targetBranch)) {
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

    private static boolean branchImplies(Set<Expression> sourceBranch, Set<Expression> targetBranch) {
        for (Expression targetPredicate : targetBranch) {
            boolean predicateMatched = false;
            for (Expression sourcePredicate : sourceBranch) {
                if (predicateImplies(sourcePredicate, targetPredicate)) {
                    predicateMatched = true;
                    break;
                }
            }
            if (!predicateMatched) {
                return false;
            }
        }
        return true;
    }

    private static boolean predicateImplies(Expression source, Expression target) {
        for (PredicateImplicationRule rule : PREDICATE_IMPLICATION_RULES) {
            if (rule.proves(source, target)) {
                return true;
            }
        }
        return false;
    }

    private static ComparisonPredicate normalizeComparisonPredicate(ComparisonPredicate predicate) {
        if (predicate.left() instanceof Literal && !(predicate.right() instanceof Literal)) {
            return (ComparisonPredicate) predicate.commute();
        }
        return predicate;
    }

    private interface PredicateImplicationRule {
        boolean proves(Expression source, Expression target);
    }

    private static final class SameExpressionImplicationRule implements PredicateImplicationRule {
        @Override
        public boolean proves(Expression source, Expression target) {
            return source.equals(target);
        }
    }

    /*
     * Prove implication between comparable predicates on the same input by converting them to ranges.
     * For example, "a > 20" implies "a > 10", and "a = 10" implies "a >= 10".
     */
    private static final class ComparablePredicateImplicationRule implements PredicateImplicationRule {
        @Override
        public boolean proves(Expression source, Expression target) {
            ComparablePredicateRange sourceRange = ComparablePredicateRange.from(source);
            ComparablePredicateRange targetRange = ComparablePredicateRange.from(target);
            return sourceRange != null && targetRange != null && sourceRange.implies(targetRange);
        }

        private static final class ComparablePredicateRange {
            private final Expression input;
            private final Bound lowerBound;
            private final Bound upperBound;

            private ComparablePredicateRange(Expression input, Bound lowerBound, Bound upperBound) {
                this.input = input;
                this.lowerBound = lowerBound;
                this.upperBound = upperBound;
            }

            private static ComparablePredicateRange from(Expression expression) {
                if (!(expression instanceof ComparisonPredicate)) {
                    return null;
                }
                ComparisonPredicate normalized = normalizeComparisonPredicate((ComparisonPredicate) expression);
                if (!(normalized.right() instanceof ComparableLiteral)) {
                    return null;
                }
                ComparableLiteral literal = (ComparableLiteral) normalized.right();
                if (normalized instanceof EqualTo) {
                    Bound bound = new Bound(literal, true);
                    return new ComparablePredicateRange(normalized.left(), bound, bound);
                }
                if (normalized instanceof GreaterThan) {
                    return new ComparablePredicateRange(normalized.left(), new Bound(literal, false), null);
                }
                if (normalized instanceof GreaterThanEqual) {
                    return new ComparablePredicateRange(normalized.left(), new Bound(literal, true), null);
                }
                if (normalized instanceof LessThan) {
                    return new ComparablePredicateRange(normalized.left(), null, new Bound(literal, false));
                }
                if (normalized instanceof LessThanEqual) {
                    return new ComparablePredicateRange(normalized.left(), null, new Bound(literal, true));
                }
                return null;
            }

            private boolean implies(ComparablePredicateRange target) {
                return input.equals(target.input)
                        && impliesLowerBound(target.lowerBound)
                        && impliesUpperBound(target.upperBound);
            }

            private boolean impliesLowerBound(Bound targetLowerBound) {
                if (targetLowerBound == null) {
                    return true;
                }
                if (lowerBound == null) {
                    return false;
                }
                int compareResult = lowerBound.literal.compareTo(targetLowerBound.literal);
                if (compareResult != 0) {
                    return compareResult > 0;
                }
                return !lowerBound.inclusive || targetLowerBound.inclusive;
            }

            private boolean impliesUpperBound(Bound targetUpperBound) {
                if (targetUpperBound == null) {
                    return true;
                }
                if (upperBound == null) {
                    return false;
                }
                int compareResult = upperBound.literal.compareTo(targetUpperBound.literal);
                if (compareResult != 0) {
                    return compareResult < 0;
                }
                return !upperBound.inclusive || targetUpperBound.inclusive;
            }
        }

        private static final class Bound {
            private final ComparableLiteral literal;
            private final boolean inclusive;

            private Bound(ComparableLiteral literal, boolean inclusive) {
                this.literal = literal;
                this.inclusive = inclusive;
            }
        }
    }

    /*
     * Example:
     *   (A OR B) AND C
     * becomes:
     *   [{A, C}, {B, C}]
     */
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

    /** Predicate compensation result holding equals, ranges and residuals. */
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
