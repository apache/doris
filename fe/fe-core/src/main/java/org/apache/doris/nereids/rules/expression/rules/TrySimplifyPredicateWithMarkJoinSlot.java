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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

/**
 * TrySimplifyPredicateWithMarkJoinSlot
 */
public class TrySimplifyPredicateWithMarkJoinSlot extends AbstractExpressionRewriteRule {
    public static final TrySimplifyPredicateWithMarkJoinSlot INSTANCE =
            new TrySimplifyPredicateWithMarkJoinSlot();

    @Override
    public Expression visitAnd(And and, ExpressionRewriteContext context) {
        /*
         *  predicate(with mark slot) and   predicate(no mark slot)
         *          false             and       TRUE  -> false(*)   -> discard
         *          false             and       NULL  -> null       -> discard
         *          false             and       FALSE -> false      -> discard
         *
         *          null              and       TRUE  -> null(*)    -> discard
         *          null              and       NULL  -> null       -> discard
         *          null              and       FALSE -> false      -> discard
         *
         *          true              and       TRUE  -> true(x)    -> keep
         *          true              and       NULL  -> null       -> discard
         *          true              and       FALSE -> false      -> discard
         *
         * we can see only 'predicate(with mark slot) and TRUE' may produce different results(*)
         * because in filter predicate, we discard null and false values and only keep true values
         * we can substitute mark slot with null and false to evaluate the predicate
         * if the result are true, or result is either false or null, we can use non-nullable mark slot
         * see ExpressionUtils.canInferNotNullForMarkSlot for more info
         * we change 'predicate(with mark slot) and predicate(no mark slot)' -> predicate(with mark slot) and true
         * to evaluate the predicate
         */
        Expression left = and.left();
        Expression newLeft = left.accept(this, context);

        if (newLeft.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newLeft = BooleanLiteral.TRUE;
        }

        Expression right = and.right();
        Expression newRight = right.accept(this, context);
        if (newRight.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newRight = BooleanLiteral.TRUE;
        }
        Expression expr = new And(newLeft, newRight);
        return expr;
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        /*
         *  predicate(with mark slot) or   predicate(no mark slot)
         *          false             or       TRUE  -> true        -> keep
         *          false             or       NULL  -> null(^)     -> discard
         *          false             or       FALSE -> false(*)    -> discard
         *
         *          null              or       TRUE  -> true        -> keep
         *          null              or       NULL  -> null(^)     -> discard
         *          null              or       FALSE -> null(*)     -> discard
         *
         *          true              or       TRUE  -> true        -> keep
         *          true              or       NULL  -> true(#)     -> keep
         *          true              or       FALSE -> true(x)     -> keep
         *
         * like And operator, even there are more differences. we can get the same conclusion.
         * by substituting mark slot with null and false to evaluate the predicate
         * if the result are true, or result is either false or null, we can use non-nullable mark slot
         * we change 'predicate(with mark slot) or predicate(no mark slot)' -> predicate(with mark slot) or false
         * to evaluate the predicate
         */
        Expression left = or.left();
        Expression newLeft = left.accept(this, context);

        if (newLeft.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newLeft = BooleanLiteral.FALSE;
        }

        Expression right = or.right();
        Expression newRight = right.accept(this, context);
        if (newRight.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newRight = BooleanLiteral.FALSE;
        }
        Expression expr = new Or(newLeft, newRight);
        return expr;
    }

}
