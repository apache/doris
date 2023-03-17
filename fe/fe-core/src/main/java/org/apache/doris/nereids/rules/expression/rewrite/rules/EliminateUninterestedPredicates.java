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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.rules.EliminateUninterestedPredicates.Context;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import java.util.Set;

/**
 * EliminateUninterestedPredicates
 *
 * this rewriter usually used to extract the partition columns related predicates,
 * and eliminate partition columns related predicate.
 *
 * e.g.
 *    (part = 1 and non_part = 'a') or (part = 2)
 * -> (part = 1 and true) or (part = 2)
 * -> (part = 1) or (part = 2)
 */
public class EliminateUninterestedPredicates extends DefaultExpressionRewriter<Context> {
    private final Set<Slot> interestedSlots;
    private final ExpressionRewriteContext expressionRewriteContext;

    private EliminateUninterestedPredicates(Set<Slot> interestedSlots, CascadesContext cascadesContext) {
        this.interestedSlots = interestedSlots;
        this.expressionRewriteContext = new ExpressionRewriteContext(cascadesContext);
    }

    public static Expression process(Expression expression, Set<Slot> interestedSlots,
            CascadesContext cascadesContext) {
        // before eliminate uninterested predicate, we must push down `Not` under CompoundPredicate
        expression = expression.accept(new SimplifyNotExprRule(), null);
        return expression.accept(new EliminateUninterestedPredicates(interestedSlots, cascadesContext), new Context());
    }

    @Override
    public Expression visit(Expression expr, Context parentContext) {
        Context currentContext = new Context();

        // postorder traversal
        expr = super.visit(expr, currentContext);

        // process predicate
        if (expr.getDataType().isBooleanType()) {
            // if a predicate contains not only interested slots but also non-interested slots,
            // we can not eliminate non-interested slots:
            // e.g.
            //    not(uninterested slot b + interested slot a > 1)
            // -> not(uninterested slot b + interested slot a > 1)
            if (!currentContext.childrenContainsInterestedSlots && currentContext.childrenContainsNonInterestedSlots) {
                // propagate true value up to eliminate uninterested slots,
                // because we don't know the runtime value of the slots
                // e.g.
                //    not(uninterested slot b > 1)
                // -> not(true)
                // -> true
                expr = BooleanLiteral.TRUE;
            } else {
                // simplify the predicate expression, the interested slots may be eliminated too
                // e.g.
                //    ((interested slot a) and not(uninterested slot b > 1)) or true
                // -> ((interested slot a) and not(true)) or true
                // -> ((interested slot a) and true) or true
                // -> (interested slot a) or true
                // -> true
                expr = expr.accept(FoldConstantRuleOnFE.INSTANCE, expressionRewriteContext);
            }
        }

        parentContext.childrenContainsInterestedSlots |= currentContext.childrenContainsInterestedSlots;
        parentContext.childrenContainsNonInterestedSlots |= currentContext.childrenContainsNonInterestedSlots;

        return expr;
    }

    @Override
    public Expression visitSlot(Slot slot, Context context) {
        boolean isInterestedSlot = interestedSlots.contains(slot);
        context.childrenContainsInterestedSlots |= isInterestedSlot;
        context.childrenContainsNonInterestedSlots |= !isInterestedSlot;
        return slot;
    }

    /** Context */
    public static class Context {
        private boolean childrenContainsInterestedSlots;
        private boolean childrenContainsNonInterestedSlots;
    }
}
