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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This record the predicates which can be pulled up or some other type predicates
 */
public class Predicates {

    // Predicates that can be pulled up
    private final List<Expression> pulledUpPredicates = new ArrayList<>();

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

    public List<? extends Expression> getPulledUpPredicates() {
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
        private Optional<Expression> equalPredicates;
        private Optional<Expression> rangePredicates;
        private Optional<Expression> residualPredicates;

        public SplitPredicate(Expression equalPredicates, Expression rangePredicates, Expression residualPredicates) {
            this.equalPredicates = Optional.ofNullable(equalPredicates);
            this.rangePredicates = Optional.ofNullable(rangePredicates);
            this.residualPredicates = Optional.ofNullable(residualPredicates);
        }

        public Expression getEqualPredicate() {
            return equalPredicates.orElse(BooleanLiteral.TRUE);
        }

        public Expression getRangePredicate() {
            return rangePredicates.orElse(BooleanLiteral.TRUE);
        }

        public Expression getResidualPredicate() {
            return residualPredicates.orElse(BooleanLiteral.TRUE);
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
            return !equalPredicates.isPresent()
                    && !rangePredicates.isPresent()
                    && !residualPredicates.isPresent();
        }

        public List<Expression> toList() {
            return ImmutableList.of(equalPredicates.orElse(BooleanLiteral.TRUE),
                    rangePredicates.orElse(BooleanLiteral.TRUE),
                    residualPredicates.orElse(BooleanLiteral.TRUE));
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            Expression equalExpr = equalPredicates.orElse(BooleanLiteral.TRUE);
            Expression rangeExpr = rangePredicates.orElse(BooleanLiteral.TRUE);
            Expression residualExpr = residualPredicates.orElse(BooleanLiteral.TRUE);
            return equalExpr instanceof BooleanLiteral
                    && rangeExpr instanceof BooleanLiteral
                    && residualExpr instanceof BooleanLiteral
                    && ((BooleanLiteral) equalExpr).getValue()
                    && ((BooleanLiteral) rangeExpr).getValue()
                    && ((BooleanLiteral) residualExpr).getValue();
        }
    }
}
