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

import java.util.List;
import java.util.Set;

/**
 * This record the predicates which can be pulled up or some other type predicates
 * */
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

    public Expression composedExpression() {
        return ExpressionUtils.and(pulledUpPredicates);
    }

    /**
     * Split the expression to equal, range and residual predicate.
     * */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSplitter predicatesSplit = new PredicatesSplitter(expression);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * The split different representation for predicate expression, such as equal, range and residual predicate.
     * */
    public static final class SplitPredicate {
        private final Expression equalPredicate;
        private final Expression rangePredicate;
        private final Expression residualPredicate;

        public SplitPredicate(Expression equalPredicate, Expression rangePredicate, Expression residualPredicate) {
            this.equalPredicate = equalPredicate;
            this.rangePredicate = rangePredicate;
            this.residualPredicate = residualPredicate;
        }

        public Expression getEqualPredicate() {
            return equalPredicate;
        }

        public Expression getRangePredicate() {
            return rangePredicate;
        }

        public Expression getResidualPredicate() {
            return residualPredicate;
        }

        public static SplitPredicate empty() {
            return new SplitPredicate(null, null, null);
        }

        /**
         * SplitPredicate construct
         * */
        public static SplitPredicate of(Expression equalPredicates,
                Expression rangePredicates,
                Expression residualPredicates) {
            return new SplitPredicate(equalPredicates, rangePredicates, residualPredicates);
        }

        /**
         * isEmpty
         * */
        public boolean isEmpty() {
            return equalPredicate == null
                    && rangePredicate == null
                    && residualPredicate == null;
        }

        public List<Expression> toList() {
            return ImmutableList.of(equalPredicate, rangePredicate, residualPredicate);
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            return equalPredicate instanceof BooleanLiteral
                    && rangePredicate instanceof BooleanLiteral
                    && residualPredicate instanceof BooleanLiteral
                    && ((BooleanLiteral) equalPredicate).getValue()
                    && ((BooleanLiteral) rangePredicate).getValue()
                    && ((BooleanLiteral) residualPredicate).getValue();
        }
    }
}
