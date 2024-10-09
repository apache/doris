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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * This record the predicates which can be pulled up or some other type predicates.
 * Also contains the necessary method for predicates process
 */
public class Predicates {

    // Predicates that can be pulled up
    private final Set<Expression> pulledUpPredicates;

    public Predicates(Set<Expression> pulledUpPredicates) {
        this.pulledUpPredicates = pulledUpPredicates;
    }

    public static Predicates of(Set<Expression> pulledUpPredicates) {
        return new Predicates(pulledUpPredicates);
    }

    public Set<Expression> getPulledUpPredicates() {
        return pulledUpPredicates;
    }

    public Predicates merge(Collection<Expression> predicates) {
        Set<Expression> mergedPredicates = new HashSet<>(predicates);
        mergedPredicates.addAll(this.pulledUpPredicates);
        return new Predicates(mergedPredicates);
    }

    /**
     * Split the expression to equal, range and residual predicate.
     */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSplitter predicatesSplit = new PredicatesSplitter(expression);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * compensate equivalence predicates
     */
    public static Set<Expression> compensateEquivalence(StructInfo queryStructInfo,
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
        final Set<Expression> equalCompensateConjunctions = new HashSet<>();
        if (queryEquivalenceClass.isEmpty() && viewEquivalenceClass.isEmpty()) {
            equalCompensateConjunctions.add(BooleanLiteral.TRUE);
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
                        List<SlotReference> viewEquivalenceSet =
                                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().get(queryEquivalenceSet);
                        List<SlotReference> copiedQueryEquivalenceSet = new ArrayList<>(queryEquivalenceSet);
                        copiedQueryEquivalenceSet.removeAll(viewEquivalenceSet);
                        SlotReference first = viewEquivalenceSet.iterator().next();
                        for (SlotReference slotReference : copiedQueryEquivalenceSet) {
                            Expression equals = new EqualTo(first, slotReference);
                            equalCompensateConjunctions.add(equals);
                        }
                    }
                }
        );
        return equalCompensateConjunctions;
    }

    /**
     * compensate range predicates
     */
    public static Set<Expression> compensateRangePredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext) {
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Expression queryRangePredicate = querySplitPredicate.getRangePredicate();
        Expression viewRangePredicate = viewSplitPredicate.getRangePredicate();
        Expression viewRangePredicateQueryBased =
                ExpressionUtils.replace(viewRangePredicate, viewToQuerySlotMapping.toSlotReferenceMap());

        Set<Expression> queryRangeSet = ExpressionUtils.extractConjunctionToSet(queryRangePredicate);
        Set<Expression> viewRangeQueryBasedSet = ExpressionUtils.extractConjunctionToSet(viewRangePredicateQueryBased);
        Set<Expression> differentExpressions = new HashSet<>();
        Sets.difference(queryRangeSet, viewRangeQueryBasedSet).copyInto(differentExpressions);
        Sets.difference(viewRangeQueryBasedSet, queryRangeSet).copyInto(differentExpressions);
        // the range predicate in query and view is same, don't need to compensate
        if (differentExpressions.isEmpty()) {
            return differentExpressions;
        }
        // try to normalize the different expressions
        Set<Expression> normalizedExpressions =
                normalizeExpression(ExpressionUtils.and(differentExpressions), cascadesContext);
        if (!queryRangeSet.containsAll(normalizedExpressions)) {
            // normalized expressions is not in query, can not compensate
            return null;
        }
        return normalizedExpressions;
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
     * compensate residual predicates
     */
    public static Set<Expression> compensateResidualPredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult) {
        // TODO Residual predicates compensate, simplify implementation currently.
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();
        Expression queryResidualPredicate = querySplitPredicate.getResidualPredicate();
        Expression viewResidualPredicate = viewSplitPredicate.getResidualPredicate();
        Expression viewResidualPredicateQueryBased =
                ExpressionUtils.replace(viewResidualPredicate, viewToQuerySlotMapping.toSlotReferenceMap());
        Set<Expression> queryResidualSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(queryResidualPredicate));
        Set<Expression> viewResidualQueryBasedSet =
                Sets.newHashSet(ExpressionUtils.extractConjunction(viewResidualPredicateQueryBased));
        // remove unnecessary literal BooleanLiteral.TRUE
        queryResidualSet.remove(BooleanLiteral.TRUE);
        viewResidualQueryBasedSet.remove(BooleanLiteral.TRUE);
        // query residual predicate can not contain all view residual predicate when view have residual predicate,
        // bail out
        if (!queryResidualSet.containsAll(viewResidualQueryBasedSet)) {
            return null;
        }
        queryResidualSet.removeAll(viewResidualQueryBasedSet);
        return queryResidualSet;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("Predicates", "pulledUpPredicates", pulledUpPredicates);
    }

    /**
     * The split different representation for predicate expression, such as equal, range and residual predicate.
     */
    public static final class SplitPredicate {
        public static final SplitPredicate INVALID_INSTANCE =
                SplitPredicate.of(null, null, null);
        private final Optional<Expression> equalPredicate;
        private final Optional<Expression> rangePredicate;
        private final Optional<Expression> residualPredicate;

        public SplitPredicate(Expression equalPredicate, Expression rangePredicate, Expression residualPredicate) {
            this.equalPredicate = Optional.ofNullable(equalPredicate);
            this.rangePredicate = Optional.ofNullable(rangePredicate);
            this.residualPredicate = Optional.ofNullable(residualPredicate);
        }

        public Expression getEqualPredicate() {
            return equalPredicate.orElse(BooleanLiteral.TRUE);
        }

        public Expression getRangePredicate() {
            return rangePredicate.orElse(BooleanLiteral.TRUE);
        }

        public Expression getResidualPredicate() {
            return residualPredicate.orElse(BooleanLiteral.TRUE);
        }

        /**
         * SplitPredicate construct
         */
        public static SplitPredicate of(Expression equalPredicates,
                Expression rangePredicates,
                Expression residualPredicates) {
            return new SplitPredicate(equalPredicates, rangePredicates, residualPredicates);
        }

        /**
         * Check the predicates are invalid or not. If any of the predicates is null, it is invalid.
         */
        public boolean isInvalid() {
            return Objects.equals(this, INVALID_INSTANCE);
        }

        public List<Expression> toList() {
            return ImmutableList.of(getEqualPredicate(), getRangePredicate(), getResidualPredicate());
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            Expression equalExpr = getEqualPredicate();
            Expression rangeExpr = getRangePredicate();
            Expression residualExpr = getResidualPredicate();
            return equalExpr instanceof BooleanLiteral
                    && rangeExpr instanceof BooleanLiteral
                    && residualExpr instanceof BooleanLiteral
                    && ((BooleanLiteral) equalExpr).getValue()
                    && ((BooleanLiteral) rangeExpr).getValue()
                    && ((BooleanLiteral) residualExpr).getValue();
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
            return Objects.equals(equalPredicate, that.equalPredicate)
                    && Objects.equals(rangePredicate, that.rangePredicate)
                    && Objects.equals(residualPredicate, that.residualPredicate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(equalPredicate, rangePredicate, residualPredicate);
        }

        @Override
        public String toString() {
            return Utils.toSqlString("SplitPredicate",
                    "equalPredicate", equalPredicate,
                    "rangePredicate", rangePredicate,
                    "residualPredicate", residualPredicate);
        }
    }
}
