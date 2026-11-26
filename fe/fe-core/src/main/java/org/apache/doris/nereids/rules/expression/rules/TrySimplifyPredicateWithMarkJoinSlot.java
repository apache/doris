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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * TrySimplifyPredicateWithMarkJoinSlot
 */
public class TrySimplifyPredicateWithMarkJoinSlot extends AbstractExpressionRewriteRule {
    public static final TrySimplifyPredicateWithMarkJoinSlot INSTANCE =
            new TrySimplifyPredicateWithMarkJoinSlot();

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext context) {
        /*
         * the And/Or neutral-element simplification in visitAnd/visitOr is only sound in a
         * boolean predicate position, i.e. the top level of the predicate or directly
         * nested under another And/Or. do not descend into other expressions: a mark-free
         * subtree nested under a NULL-observing wrapper (e.g. ifnull(M, flag OR FALSE)) is
         * observed by the wrapper when the mark slot is null, so replacing it with the
         * neutral element would change the semantics and make the inference unsound.
         * see ExpressionUtils.inferMarkSlotNotNullMap for how the simplified predicate
         * is used.
         */
        return expr;
    }

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
         * we can substitute the target mark slot with null and false, and substitute other mark
         * slots with true, false and null to evaluate the predicate
         * if the target slot taking false or null always evaluates to the same boolean value,
         * we can use non-nullable mark slot
         * see ExpressionUtils.inferMarkSlotNotNullMap for more info
         * we change 'predicate(with mark slot) and predicate(no mark slot)' -> predicate(with mark slot) and true
         * to evaluate the predicate
         */
        List<Expression> newChildren = Lists.newArrayListWithCapacity(and.children().size());
        for (Expression child : and.children()) {
            Expression newChild = child.accept(this, context);
            if (newChild.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
                newChild = BooleanLiteral.TRUE;
            }
            newChildren.add(newChild);
        }
        Expression expr = new And(newChildren);
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
         * by substituting the target mark slot with null and false, and substituting other mark
         * slots with true, false and null to evaluate the predicate
         * if the target slot taking false or null always evaluates to the same boolean value,
         * we can use non-nullable mark slot
         * we change 'predicate(with mark slot) or predicate(no mark slot)' -> predicate(with mark slot) or false
         * to evaluate the predicate
         */

        List<Expression> newChildren = Lists.newArrayListWithCapacity(or.children().size());
        for (Expression child : or.children()) {
            Expression newChild = child.accept(this, context);
            if (newChild.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
                newChild = BooleanLiteral.FALSE;
            }
            newChildren.add(newChild);
        }

        Expression expr = new Or(newChildren);
        return expr;
    }

}
