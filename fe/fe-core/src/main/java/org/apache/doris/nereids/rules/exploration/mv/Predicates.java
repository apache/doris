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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This record the predicates which can be pulled up or some other type predicates
 */
public class Predicates {

    // Predicates that can be pulled up
    private final Set<Expression> pulledUpPredicates = new HashSet<>();

    private Predicates() {
    }

    public static Predicates of() {
        return new Predicates();
    }

    public static Predicates of(List<? extends Expression> pulledUpPredicates) {
        Predicates predicates = new Predicates();
        pulledUpPredicates.forEach(predicates::addPredicate);
        return predicates;
    }

    public Set<? extends Expression> getPulledUpPredicates() {
        return pulledUpPredicates;
    }

    public void addPredicate(Expression expression) {
        this.pulledUpPredicates.add(expression);
    }

    public Expression composedExpression() {
        return ExpressionUtils.and(pulledUpPredicates.stream().map(Expression.class::cast)
                .collect(Collectors.toList()));
    }

    /**
     * Split the expression to equal, range and residual predicate.
     */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSplitter predicatesSplit = new PredicatesSplitter(expression);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * The split different representation for predicate expression, such as equal, range and residual predicate.
     */
    public static final class SplitPredicate {
        private Optional<Expression> equalPredicate;
        private Optional<Expression> rangePredicate;
        private Optional<Expression> residualPredicate;

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

        public static SplitPredicate empty() {
            return new SplitPredicate(null, null, null);
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
         * isEmpty
         */
        public boolean isEmpty() {
            return !equalPredicate.isPresent()
                    && !rangePredicate.isPresent()
                    && !residualPredicate.isPresent();
        }

        public List<Expression> toList() {
            return ImmutableList.of(equalPredicate.orElse(BooleanLiteral.TRUE),
                    rangePredicate.orElse(BooleanLiteral.TRUE),
                    residualPredicate.orElse(BooleanLiteral.TRUE));
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            Expression equalExpr = equalPredicate.orElse(BooleanLiteral.TRUE);
            Expression rangeExpr = rangePredicate.orElse(BooleanLiteral.TRUE);
            Expression residualExpr = residualPredicate.orElse(BooleanLiteral.TRUE);
            return equalExpr instanceof BooleanLiteral
                    && rangeExpr instanceof BooleanLiteral
                    && residualExpr instanceof BooleanLiteral
                    && ((BooleanLiteral) equalExpr).getValue()
                    && ((BooleanLiteral) rangeExpr).getValue()
                    && ((BooleanLiteral) residualExpr).getValue();
        }
    }
}
