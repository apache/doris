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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
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
     * compensate equivalence predicates
     */
    public static Map<Expression, ExpressionInfo> compensateEquivalence(StructInfo queryStructInfo,
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
     * compensate range predicates
     */
    public static Map<Expression, ExpressionInfo> compensateRangePredicate(StructInfo queryStructInfo,
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

        // Try to detect whole-bucket ranges and synthesize date_trunc equality predicates
        Map<Expression, ExpressionInfo> syntheticPredicates = detectAndSynthesizeWholeBucketPredicates(
                normalizedExpressions, viewStructInfo, viewToQuerySlotMapping);
        if (syntheticPredicates != null) {
            return syntheticPredicates;
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
        return normalizedExpressionsWithLiteral;
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
     * Detect if normalized expressions form a whole-bucket range and synthesize date_trunc equality predicates.
     * Returns null if no whole-bucket pattern detected.
     */
    private static Map<Expression, ExpressionInfo> detectAndSynthesizeWholeBucketPredicates(
            Set<Expression> normalizedExpressions,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping) {
        // Group predicates by slot
        Map<Expression, List<Expression>> slotToPredicates = new HashMap<>();
        for (Expression expr : normalizedExpressions) {
            if (!(expr instanceof ComparisonPredicate)) {
                continue;
            }
            ComparisonPredicate pred = (ComparisonPredicate) expr;
            Expression left = pred.left();
            if (left instanceof SlotReference) {
                slotToPredicates.computeIfAbsent(left, k -> new ArrayList<>()).add(expr);
            }
        }

        // Check if view has date_trunc on any of these slots
        Map<SlotReference, DateTrunc> viewDateTruncMap = extractViewDateTruncExpressions(viewStructInfo);
        if (viewDateTruncMap.isEmpty()) {
            return null;
        }

        // Map view slots to query slots
        Map<SlotReference, SlotReference> viewToQuerySlotMap = viewToQuerySlotMapping.toSlotReferenceMap();
        Map<SlotReference, DateTrunc> querySlotToViewDateTrunc = new HashMap<>();
        for (Map.Entry<SlotReference, DateTrunc> entry : viewDateTruncMap.entrySet()) {
            SlotReference viewSlot = entry.getKey();
            SlotReference querySlot = viewToQuerySlotMap.get(viewSlot);
            if (querySlot != null) {
                querySlotToViewDateTrunc.put(querySlot, entry.getValue());
            }
        }

        // Try to detect whole-bucket ranges across all date_trunc slots
        Map<Expression, ExpressionInfo> syntheticResults = new HashMap<>();
        Set<Expression> allConsumedPredicates = new HashSet<>();

        for (Map.Entry<Expression, List<Expression>> entry : slotToPredicates.entrySet()) {
            if (!(entry.getKey() instanceof SlotReference)) {
                continue;
            }
            SlotReference slot = (SlotReference) entry.getKey();

            // Only apply to DATE/DATEV2 types, not DATETIME/DATETIMEV2
            if (!slot.getDataType().isDateType() && !slot.getDataType().isDateV2Type()) {
                continue;
            }

            DateTrunc viewDateTrunc = querySlotToViewDateTrunc.get(slot);
            if (viewDateTrunc == null) {
                continue;
            }

            List<Expression> predicates = entry.getValue();
            // Only supports exactly one closed range (lower + upper bound) forming a single bucket.
            // Multi-bucket ranges, single-sided ranges, or 3+ predicates on the same slot are not handled.
            if (predicates.size() != 2) {
                continue;
            }

            // Extract lower and upper bounds
            DateLiteral lowerBound = null;
            DateLiteral upperBound = null;
            for (Expression pred : predicates) {
                if (!(pred instanceof ComparisonPredicate)) {
                    continue;
                }
                ComparisonPredicate cp = (ComparisonPredicate) pred;
                if (cp.right() instanceof DateTimeLiteral || !(cp.right() instanceof DateLiteral)) {
                    continue;
                }
                DateLiteral literal = (DateLiteral) cp.right();
                if (cp instanceof GreaterThanEqual) {
                    lowerBound = literal;
                } else if (cp instanceof GreaterThan) {
                    lowerBound = (DateLiteral) literal.plusDays(1);
                } else if (cp instanceof LessThanEqual) {
                    upperBound = literal;
                } else if (cp instanceof LessThan) {
                    upperBound = (DateLiteral) literal.plusDays(-1);
                }
            }

            if (lowerBound == null || upperBound == null) {
                continue;
            }
            if (lowerBound.getDouble() > upperBound.getDouble()) {
                continue;
            }

            java.util.Optional<DateTruncRangeDetector.BucketInfo> bucketInfo =
                    DateTruncRangeDetector.detectWholeBucket(lowerBound, upperBound);
            if (!bucketInfo.isPresent()) {
                continue;
            }

            String bucketUnit = bucketInfo.get().unit;
            String viewUnit = extractDateTruncUnit(viewDateTrunc);
            if (viewUnit == null || !viewUnit.equalsIgnoreCase(bucketUnit)) {
                continue;
            }

            DateLiteral bucketStart = bucketInfo.get().bucketStart;
            Expression syntheticPredicate = new EqualTo(
                    rebuildDateTruncOnQuerySlot(viewDateTrunc, slot),
                    bucketStart
            );
            syntheticResults.put(syntheticPredicate, new ExpressionInfo(bucketStart, true));
            allConsumedPredicates.addAll(predicates);
        }

        if (syntheticResults.isEmpty()) {
            return null;
        }

        // Build result: synthetic predicates + remaining non-consumed predicates
        Map<Expression, ExpressionInfo> result = new HashMap<>(syntheticResults);
        for (Expression expr : normalizedExpressions) {
            if (allConsumedPredicates.contains(expr)) {
                continue;
            }
            Set<Literal> literalSet = expr.collect(e -> e instanceof Literal);
            if (expr.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            if (literalSet.size() == 1 && expr instanceof ComparisonPredicate
                    && !(expr instanceof GreaterThan || expr instanceof LessThanEqual)) {
                result.put(expr, new ExpressionInfo(literalSet.iterator().next()));
            } else {
                result.put(expr, ExpressionInfo.EMPTY);
            }
        }
        return result;
    }

    /**
     * Extract date_trunc expressions from view output shuttled expressions.
     */
    private static Map<SlotReference, DateTrunc> extractViewDateTruncExpressions(StructInfo viewStructInfo) {
        Map<SlotReference, DateTrunc> result = new HashMap<>();
        for (Expression expr : viewStructInfo.getPlanOutputShuttledExpressions()) {
            Expression unwrapped = expr;
            if (unwrapped instanceof Alias) {
                unwrapped = ((Alias) unwrapped).child();
            }
            if (unwrapped instanceof DateTrunc) {
                DateTrunc dateTrunc = (DateTrunc) unwrapped;
                Expression dateArg = dateTrunc.child(0).getDataType().isDateLikeType()
                        ? dateTrunc.child(0) : dateTrunc.child(1);
                if (dateArg instanceof SlotReference) {
                    result.put((SlotReference) dateArg, dateTrunc);
                }
            }
        }
        return result;
    }

    /**
     * Extract the unit string from a DateTrunc expression.
     */
    private static String extractDateTruncUnit(DateTrunc dateTrunc) {
        Expression arg0 = dateTrunc.child(0);
        Expression arg1 = dateTrunc.child(1);
        if (arg1 instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) arg1).getStringValue();
        } else if (arg0 instanceof StringLikeLiteral) {
            return ((StringLikeLiteral) arg0).getStringValue();
        }
        return null;
    }

    private static DateTrunc rebuildDateTruncOnQuerySlot(DateTrunc viewDateTrunc, SlotReference querySlot) {
        Expression arg0 = viewDateTrunc.child(0);
        Expression arg1 = viewDateTrunc.child(1);
        if (arg0.getDataType().isDateLikeType()) {
            return new DateTrunc(querySlot, arg1);
        }
        return new DateTrunc(arg0, querySlot);
    }

    /**
     * compensate residual predicates
     */
    public static Map<Expression, ExpressionInfo> compensateResidualPredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult) {
        // TODO Residual predicates compensate, simplify implementation currently.
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Set<Expression> viewResidualQueryBasedSet = new HashSet<>();
        for (Expression viewExpression : viewSplitPredicate.getResidualPredicateMap().keySet()) {
            viewResidualQueryBasedSet.add(
                    ExpressionUtils.replace(viewExpression, viewToQuerySlotMapping.toSlotReferenceMap()));
        }
        viewResidualQueryBasedSet.remove(BooleanLiteral.TRUE);

        Set<Expression> queryResidualSet = querySplitPredicate.getResidualPredicateMap().keySet();
        // remove unnecessary literal BooleanLiteral.TRUE
        queryResidualSet.remove(BooleanLiteral.TRUE);
        // query residual predicate can not contain all view residual predicate when view have residual predicate,
        // bail out
        if (!queryResidualSet.containsAll(viewResidualQueryBasedSet)) {
            return null;
        }
        queryResidualSet.removeAll(viewResidualQueryBasedSet);
        Map<Expression, ExpressionInfo> expressionExpressionInfoMap = new HashMap<>();
        for (Expression needCompensate : queryResidualSet) {
            if (needCompensate.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            expressionExpressionInfoMap.put(needCompensate, ExpressionInfo.EMPTY);
        }
        return expressionExpressionInfoMap;
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

        public static final ExpressionInfo EMPTY = new ExpressionInfo(null, false);

        public final Literal literal;
        public final boolean isSyntheticDateTruncEquality;

        public ExpressionInfo(Literal literal) {
            this(literal, false);
        }

        public ExpressionInfo(Literal literal, boolean isSyntheticDateTruncEquality) {
            this.literal = literal;
            this.isSyntheticDateTruncEquality = isSyntheticDateTruncEquality;
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
