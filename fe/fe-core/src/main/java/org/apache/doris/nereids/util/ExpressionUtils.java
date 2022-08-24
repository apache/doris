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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Expression rewrite helper class.
 */
public class ExpressionUtils {

    public static List<Expression> extractConjunction(Expression expr) {
        return extract(And.class, expr);
    }

    public static List<Expression> extractDisjunction(Expression expr) {
        return extract(Or.class, expr);
    }

    /**
     * Split predicates with `And/Or` form recursively.
     * Some examples for `And`:
     * <p>
     * a and b -> a, b
     * (a and b) and c -> a, b, c
     * (a or b) and (c and d) -> (a or b), c , d
     * <p>
     * Stop recursion when meeting `Or`, so this func will ignore `And` inside `Or`.
     * Warning examples:
     * (a and b) or c -> (a and b) or c
     */
    public static List<Expression> extract(CompoundPredicate expr) {
        return extract(expr.getClass(), expr);
    }

    private static List<Expression> extract(Class<? extends Expression> type, Expression expr) {
        List<Expression> result = Lists.newArrayList();
        extract(type, expr, result);
        return result;
    }

    private static void extract(Class<? extends Expression> type, Expression expr, List<Expression> result) {
        if (type.isInstance(expr)) {
            CompoundPredicate predicate = (CompoundPredicate) expr;
            extract(type, predicate.left(), result);
            extract(type, predicate.right(), result);
        } else {
            result.add(expr);
        }
    }

    public static Expression and(List<Expression> expressions) {
        return combine(And.class, expressions);
    }

    public static Expression and(Expression... expressions) {
        return combine(And.class, Lists.newArrayList(expressions));
    }

    public static Expression or(Expression... expressions) {
        return combine(Or.class, Lists.newArrayList(expressions));
    }

    public static Expression or(List<Expression> expressions) {
        return combine(Or.class, expressions);
    }

    /**
     * Use AND/OR to combine expressions together.
     */
    public static Expression combine(Class<? extends Expression> type, List<Expression> expressions) {
        /*
         *             (AB) (CD) E   ((AB)(CD))  E     (((AB)(CD))E)
         *               ▲   ▲   ▲       ▲       ▲          ▲
         *               │   │   │       │       │          │
         * A B C D E ──► A B C D E ──► (AB) (CD) E ──► ((AB)(CD)) E ──► (((AB)(CD))E)
         */
        Preconditions.checkArgument(type == And.class || type == Or.class);
        Objects.requireNonNull(expressions, "expressions is null");

        Expression shortCircuit = (type == And.class ? BooleanLiteral.FALSE : BooleanLiteral.TRUE);
        Expression skip = (type == And.class ? BooleanLiteral.TRUE : BooleanLiteral.FALSE);
        Set<Expression> distinctExpressions = Sets.newLinkedHashSetWithExpectedSize(expressions.size());
        for (Expression expression : expressions) {
            if (expression.equals(shortCircuit)) {
                return shortCircuit;
            } else if (!expression.equals(skip)) {
                distinctExpressions.add(expression);
            }
        }

        return distinctExpressions.stream()
                .reduce(type == And.class ? And::new : Or::new)
                .orElse(new BooleanLiteral(type == And.class));
    }

    /**
     * Check whether lhs and rhs (both are List of SlotReference) are intersecting.
     */
    public static boolean isIntersecting(List<SlotReference> lhs, List<SlotReference> rhs) {
        for (SlotReference lSlot : lhs) {
            for (SlotReference rSlot : rhs) {
                if (lSlot.equals(rSlot)) {
                    return true;
                }
            }
        }
        return false;
    }
}
