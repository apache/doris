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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.List;

/**
 * Calculate the weight of each expression
 * Now we just set all expression 1 except simple slot
 */
public class ExprCostModel extends ExpressionVisitor<Double, Void> {
    public static double calculateExprCost(List<? extends Expression> expressionList) {
        ExprCostModel exprCostModel = new ExprCostModel();
        return expressionList.stream()
                .map(e -> e.accept(exprCostModel, null))
                .reduce(0.0, (a, b) -> a + b);
    }

    public static double calculateExprCost(Expression expression) {
        ExprCostModel exprCostModel = new ExprCostModel();
        return expression.accept(exprCostModel, null);
    }

    @Override
    public Double visit(Expression expr, Void context) {
        return 1.0;
    }

    @Override
    public Double visitAlias(Alias alias, Void context) {
        return alias.children().stream()
                .map(e -> e.accept(this, context))
                .reduce(0.0, (a, b) -> a + b);
    }

    @Override
    public Double visitSlot(Slot slot, Void context) {
        return 0.0;
    }

    @Override
    public Double visitSlotReference(SlotReference slotReference, Void context) {
        return 0.0;
    }

    @Override
    public Double visitLiteral(Literal literal, Void context) {
        return 0.0;
    }

    @Override
    public Double visitAnd(And and, Void context) {
        return 0.0;
    }

    @Override
    public Double visitOr(Or or, Void context) {
        return 0.0;
    }

}
