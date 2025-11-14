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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Expression replacer which support to handle VirtualSlotReference, if replaceMap contained, just use it
 */
public class MaterializedViewExprReplacer extends DefaultExpressionRewriter<Void> {

    private final Map<? extends Expression, ? extends Expression> replaceMap;
    // indicate whether all expressions are replaced successfully
    private boolean replaceSuccess = true;

    public MaterializedViewExprReplacer(
            Map<? extends Expression, ? extends Expression> replaceMap) {
        this.replaceMap = replaceMap;
    }

    public boolean isReplaceSuccess() {
        return replaceSuccess;
    }

    @Override
    public Expression visit(Expression expr, Void context) {
        if (!replaceSuccess) {
            return expr;
        }
        Expression replacedExpr = replaceMap.get(expr);
        if (replacedExpr != null) {
            return replacedExpr;
        }
        List<Expression> newChildren = new ArrayList<>();
        boolean hasChanged = false;
        for (Expression child : expr.children()) {
            Expression newChild = child.accept(this, context);
            if (!replaceSuccess) {
                return expr;
            }
            if (newChild != child) {
                hasChanged = true;
            }
            newChildren.add(newChild);
        }
        return hasChanged ? expr.withChildren(newChildren) : expr;
    }

    @Override
    public Expression visitSlot(Slot slot, Void context) {
        if (!replaceSuccess) {
            return slot;
        }
        Expression replacedExpr = replaceMap.get(slot);
        if (replacedExpr != null) {
            return replacedExpr;
        }
        replaceSuccess = false;
        return slot;
    }

    @Override
    public Expression visitVirtualReference(VirtualSlotReference virtualSlotReference, Void context) {
        if (!replaceSuccess) {
            return virtualSlotReference;
        }
        return handleVirtualSlot(virtualSlotReference, context);
    }

    private Expression handleVirtualSlot(
            VirtualSlotReference virtualSlot,
            Void context) {
        if (!replaceSuccess) {
            return virtualSlot;
        }
        Optional<GroupingScalarFunction> originExpression = virtualSlot.getOriginExpression();
        return originExpression.map(groupingScalarFunction ->
                        Repeat.generateVirtualSlotByFunction(
                                (GroupingScalarFunction) groupingScalarFunction.accept(this, context)))
                .orElseGet(Repeat::generateVirtualGroupingIdSlot);
    }
}
