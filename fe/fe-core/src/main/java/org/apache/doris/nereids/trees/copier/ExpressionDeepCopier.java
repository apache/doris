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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.GroupingSetShapes;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.base.Function;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * deep copy expression, generate new expr id for SlotReference and Alias.
 */
public class ExpressionDeepCopier extends DefaultExpressionRewriter<DeepCopierContext> {

    public static ExpressionDeepCopier INSTANCE = new ExpressionDeepCopier();

    public Expression deepCopy(Expression expression, DeepCopierContext context) {
        return expression.accept(this, context);
    }

    @Override
    public Expression visitAlias(Alias alias, DeepCopierContext context) {
        Expression child = alias.child().accept(this, context);
        Map<ExprId, ExprId> exprIdReplaceMap = context.exprIdReplaceMap;
        Alias newOne;
        if (exprIdReplaceMap.containsKey(alias.getExprId())) {
            // NOTICE: because we do not do normalize agg, so we could get same Alias in more than one place
            //  so, if we already copy this Alias once, we must use the existed ExprId for this Alias.
            newOne = new Alias(exprIdReplaceMap.get(alias.getExprId()), child, alias.getName());
        } else {
            newOne = new Alias(child, alias.getName());
            exprIdReplaceMap.put(alias.getExprId(), newOne.getExprId());
        }
        return newOne;
    }

    @Override
    public Expression visitSlotReference(SlotReference slotReference, DeepCopierContext context) {
        Map<ExprId, ExprId> exprIdReplaceMap = context.exprIdReplaceMap;
        ExprId newExprId;
        if (exprIdReplaceMap.containsKey(slotReference.getExprId())) {
            newExprId = exprIdReplaceMap.get(slotReference.getExprId());
        } else {
            newExprId = StatementScopeIdGenerator.newExprId();
            exprIdReplaceMap.put(slotReference.getExprId(), newExprId);
        }
        return slotReference.withExprId(newExprId);
    }

    @Override
    public Expression visitVirtualReference(VirtualSlotReference virtualSlotReference, DeepCopierContext context) {
        Map<ExprId, ExprId> exprIdReplaceMap = context.exprIdReplaceMap;
        ExprId newExprId;
        if (exprIdReplaceMap.containsKey(virtualSlotReference.getExprId())) {
            newExprId = exprIdReplaceMap.get(virtualSlotReference.getExprId());
        } else {
            newExprId = StatementScopeIdGenerator.newExprId();
        }
        // according to VirtualReference generating logic in Repeat.java
        // generateVirtualGroupingIdSlot and generateVirtualSlotByFunction
        Optional<GroupingScalarFunction> newOriginExpression = virtualSlotReference.getOriginExpression()
                .map(func -> (GroupingScalarFunction) func.accept(this, context));
        Function<GroupingSetShapes, List<Long>> newFunction = newOriginExpression
                .<Function<GroupingSetShapes, List<Long>>>map(f -> f::computeVirtualSlotValue)
                .orElseGet(() -> GroupingSetShapes::computeVirtualGroupingIdValue);
        VirtualSlotReference newOne = new VirtualSlotReference(newExprId,
                virtualSlotReference.getName(), virtualSlotReference.getDataType(),
                virtualSlotReference.nullable(), virtualSlotReference.getQualifier(),
                newOriginExpression, newFunction);
        exprIdReplaceMap.put(virtualSlotReference.getExprId(), newOne.getExprId());
        return newOne;
    }

    @Override
    public Expression visitArrayItemReference(ArrayItemReference arrayItemSlot, DeepCopierContext context) {
        Expression arrayExpression = arrayItemSlot.getArrayExpression().accept(this, context);
        Map<ExprId, ExprId> exprIdReplaceMap = context.exprIdReplaceMap;
        ArrayItemReference newOne;
        if (exprIdReplaceMap.containsKey(arrayItemSlot.getExprId())) {
            newOne = new ArrayItemReference(exprIdReplaceMap.get(arrayItemSlot.getExprId()),
                    arrayItemSlot.getName(), arrayExpression);
        } else {
            newOne = new ArrayItemReference(arrayItemSlot.getName(), arrayExpression);
            exprIdReplaceMap.put(arrayItemSlot.getExprId(), newOne.getExprId());
        }
        return newOne;
    }

    @Override
    public Expression visitExistsSubquery(Exists exists, DeepCopierContext context) {
        LogicalPlan logicalPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy(exists.getQueryPlan(), context);
        List<Slot> correlateSlots = exists.getCorrelateSlots().stream()
                .map(s -> (Slot) s.accept(this, context))
                .collect(Collectors.toList());
        Optional<Expression> typeCoercionExpr = exists.getTypeCoercionExpr()
                .map(c -> c.accept(this, context));
        return new Exists(logicalPlan, correlateSlots, typeCoercionExpr, exists.isNot());
    }

    @Override
    public Expression visitListQuery(ListQuery listQuery, DeepCopierContext context) {
        LogicalPlan logicalPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy(listQuery.getQueryPlan(), context);
        List<Slot> correlateSlots = listQuery.getCorrelateSlots().stream()
                .map(s -> (Slot) s.accept(this, context))
                .collect(Collectors.toList());
        Optional<Expression> typeCoercionExpr = listQuery.getTypeCoercionExpr()
                .map(c -> c.accept(this, context));
        return new ListQuery(logicalPlan, correlateSlots, typeCoercionExpr);
    }

    @Override
    public Expression visitInSubquery(InSubquery in, DeepCopierContext context) {
        Expression compareExpr = in.getCompareExpr().accept(this, context);
        List<Slot> correlateSlots = in.getCorrelateSlots().stream()
                .map(s -> (Slot) s.accept(this, context))
                .collect(Collectors.toList());
        Optional<Expression> typeCoercionExpr = in.getTypeCoercionExpr()
                .map(c -> c.accept(this, context));
        ListQuery listQuery = (ListQuery) in.getListQuery().accept(this, context);
        return new InSubquery(compareExpr, listQuery, correlateSlots, typeCoercionExpr, in.isNot());
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery scalar, DeepCopierContext context) {
        LogicalPlan logicalPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy(scalar.getQueryPlan(), context);
        List<Slot> correlateSlots = scalar.getCorrelateSlots().stream()
                .map(s -> (Slot) s.accept(this, context))
                .collect(Collectors.toList());
        Optional<Expression> typeCoercionExpr = scalar.getTypeCoercionExpr()
                .map(c -> c.accept(this, context));
        return new ScalarSubquery(logicalPlan, correlateSlots, typeCoercionExpr);
    }
}
