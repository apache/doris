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
        Expression left = and.left();
        Expression newLeft = left.accept(this, context);

        if (newLeft.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newLeft = BooleanLiteral.TRUE;
        }

        Expression right = and.right();
        Expression newRight = this.visit(right, context);
        if (newRight.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newRight = BooleanLiteral.TRUE;
        }
        Expression expr = new And(newLeft, newRight);
        return expr;
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        Expression left = or.left();
        Expression newLeft = left.accept(this, context);

        if (newLeft.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newLeft = BooleanLiteral.FALSE;
        }

        Expression right = or.right();
        Expression newRight = this.visit(right, context);
        if (newRight.getInputSlots().stream().noneMatch(MarkJoinSlotReference.class::isInstance)) {
            newRight = BooleanLiteral.FALSE;
        }
        Expression expr = new Or(newLeft, newRight);
        return expr;
    }

}
