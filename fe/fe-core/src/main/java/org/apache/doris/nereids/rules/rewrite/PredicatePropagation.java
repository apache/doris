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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * derive additional predicates.
 * for example:
 * a = b and a = 1 => b = 1
 */
public class PredicatePropagation {

    /**
     * infer additional predicates.
     */
    public Set<Expression> infer(Set<Expression> predicates) {
        Set<Expression> inferred = Sets.newHashSet();
        for (Expression predicate : predicates) {
            if (canEquivalentInfer(predicate)) {
                List<Expression> newInferred = predicates.stream()
                        .filter(p -> !p.equals(predicate))
                        .map(p -> doInfer(predicate, p))
                        .collect(Collectors.toList());
                inferred.addAll(newInferred);
            }
        }
        inferred.removeAll(predicates);
        return inferred;
    }

    /**
     * Use the left or right child of `leftSlotEqualToRightSlot` to replace the left or right child of `expression`
     * Now only support infer `ComparisonPredicate`.
     * TODO: We should determine whether `expression` satisfies the condition for replacement
     *       eg: Satisfy `expression` is non-deterministic
     */
    private Expression doInfer(Expression leftSlotEqualToRightSlot, Expression expression) {
        return expression.accept(new DefaultExpressionRewriter<Void>() {

            @Override
            public Expression visit(Expression expr, Void context) {
                return expr;
            }

            @Override
            public Expression visitComparisonPredicate(ComparisonPredicate cp, Void context) {
                // we need to get expression covered by cast, because we want to infer different datatype
                if (ExpressionUtils.isExpressionSlotCoveredByCast(cp.left()) && (cp.right().isConstant())) {
                    return replaceSlot(cp, ExpressionUtils.getDatatypeCoveredByCast(cp.left()));
                } else if (ExpressionUtils.isExpressionSlotCoveredByCast(cp.right()) && cp.left().isConstant()) {
                    return replaceSlot(cp, ExpressionUtils.getDatatypeCoveredByCast(cp.right()));
                }
                return super.visit(cp, context);
            }

            private boolean isDataTypeValid(DataType originDataType, Expression expr) {
                if ((leftSlotEqualToRightSlot.child(0).getDataType() instanceof IntegralType)
                        && (leftSlotEqualToRightSlot.child(1).getDataType() instanceof IntegralType)
                                && (originDataType instanceof IntegralType)) {
                    // infer filter can not be lower than original datatype, or dataset would be wrong
                    if (!((IntegralType) originDataType).widerThan(
                            (IntegralType) leftSlotEqualToRightSlot.child(0).getDataType())
                                    && !((IntegralType) originDataType).widerThan(
                                            (IntegralType) leftSlotEqualToRightSlot.child(1).getDataType())) {
                        return true;
                    }
                }
                return false;
            }

            private Expression replaceSlot(Expression expr, DataType originDataType) {
                return expr.rewriteUp(e -> {
                    if (isDataTypeValid(originDataType, leftSlotEqualToRightSlot)) {
                        if (ExpressionUtils.isTwoExpressionEqualWithCast(e, leftSlotEqualToRightSlot.child(0))) {
                            return leftSlotEqualToRightSlot.child(1);
                        } else if (ExpressionUtils.isTwoExpressionEqualWithCast(e, leftSlotEqualToRightSlot.child(1))) {
                            return leftSlotEqualToRightSlot.child(0);
                        }
                    }
                    return e;
                });
            }
        }, null);
    }

    /**
     * Currently only equivalence derivation is supported
     * and requires that the left and right sides of an expression must be slot
     */
    private boolean canEquivalentInfer(Expression predicate) {
        return predicate instanceof EqualTo
                && predicate.children().stream().allMatch(e ->
                    (e instanceof SlotReference) || (e instanceof Cast && e.child(0) instanceof SlotReference))
                && predicate.child(0).getDataType().equals(predicate.child(1).getDataType());
    }

}
