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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.rules.rewrite.NonEqualPredicateInfer;
import org.apache.doris.nereids.rules.rewrite.ReplacePredicate;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.HashSet;
import java.util.Set;

/** PredicateInferUtils */
public class PredicateInferUtils {
    public static boolean isSlotOrLiteral(Expression expr) {
        return expr instanceof SlotReference || expr instanceof Literal;
    }

    /**The inputs predicate is divided into two parts. One is the predicate directly reserved, which does not enter
     *  the non equivalent derivation, and the other is the predicates entering the non equivalent derivation*/
    public static void getComplexAndSimplePredicates(Set<Expression> inputs, Set<Expression> complex,
            Set<ComparisonPredicate> simple) {
        for (Expression input : inputs) {
            if (input instanceof ComparisonPredicate && !(input instanceof NullSafeEqual)) {
                ComparisonPredicate comparisonPredicate = (ComparisonPredicate) input;
                if (comparisonPredicate.left().equals(comparisonPredicate.right())) {
                    complex.add(input);
                }
                Set<Slot> leftSlots = comparisonPredicate.left().getInputSlots();
                Set<Slot> rightSlots = comparisonPredicate.right().getInputSlots();
                if (leftSlots.isEmpty() && rightSlots.isEmpty()) {
                    complex.add(input);
                }
                if (!isSlotOrLiteral(comparisonPredicate.left()) || !isSlotOrLiteral(comparisonPredicate.right())) {
                    complex.add(input);
                }
                if (comparisonPredicate instanceof GreaterThan || comparisonPredicate instanceof GreaterThanEqual
                        || comparisonPredicate instanceof EqualTo || comparisonPredicate instanceof LessThan
                        || comparisonPredicate instanceof LessThanEqual) {
                    simple.add(comparisonPredicate);
                } else {
                    complex.add(input);
                }
            } else {
                complex.add(input);
            }
        }
    }

    /**The predicate derivation is based on the input predicate predicates, which is divided into two parts.
     * The equivalent relation used in ReplacePredicate and calculated by union-find derive like, in, not
     * and ComparisonPredicate;
     * The NonEqualPredicateInfer class deduces predicates based on non-equal relations, and deletes
     * the useless ComparisonPredicates derived from ReplacePredicate*/
    public static Set<Expression> inferPredicate(Set<Expression> predicates) {
        Set<Expression> inferPredicates = new HashSet<>();
        Set<Expression> complexPredicates = new HashSet<>();
        Set<ComparisonPredicate> simplePredicates = new HashSet<>();
        Set<Expression> inferAndOriginPredicates = ReplacePredicate.infer(predicates);
        inferAndOriginPredicates.addAll(predicates);
        PredicateInferUtils.getComplexAndSimplePredicates(inferAndOriginPredicates, complexPredicates,
                simplePredicates);
        inferPredicates.addAll(complexPredicates);
        inferPredicates.addAll(NonEqualPredicateInfer.inferUnequalPredicates(simplePredicates));
        return inferPredicates;
    }
}
