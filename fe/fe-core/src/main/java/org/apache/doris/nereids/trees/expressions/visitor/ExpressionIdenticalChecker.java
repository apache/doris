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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

/**
 * Checker for identifying two expressions are identical.
 */
public class ExpressionIdenticalChecker extends DefaultExpressionVisitor<Boolean, Expression> {

    public static final ExpressionIdenticalChecker INSTANCE = new ExpressionIdenticalChecker();

    public boolean check(Expression expression, Expression other) {
        return expression.accept(this, other);
    }

    private boolean isClassMatch(Object o1, Object o2) {
        return o1.getClass().equals(o2.getClass());
    }

    private boolean isSameChild(Expression expression, Expression other) {
        if (expression.children().size() != other.children().size()) {
            return false;
        }
        for (int i = 0; i < expression.children().size(); ++i) {
            if (!expression.children().get(i).accept(this, other.children().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visit(Expression expression, Expression other) {
        return isClassMatch(expression, other) && isSameChild(expression, other);
    }

    @Override
    public Boolean visitSlotReference(SlotReference slotReference, Expression other) {
        return slotReference.equals(other);
    }

    @Override
    public Boolean visitLiteral(Literal literal, Expression other) {
        return literal.equals(other);
    }

    @Override
    public Boolean visitComparisonPredicate(ComparisonPredicate cp, Expression other) {
        return cp.equals(other) || cp.commute().equals(other);
    }
}
