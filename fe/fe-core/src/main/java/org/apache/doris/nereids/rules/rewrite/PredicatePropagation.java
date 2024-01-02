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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rules.DateFunctionRewrite;
import org.apache.doris.nereids.rules.expression.rules.SimplifyComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * derive additional predicates.
 * for example:
 * a = b and a = 1 => b = 1
 */
public class PredicatePropagation {

    private enum InferType {
        NONE(null),
        INTEGRAL(IntegralType.class),
        STRING(CharacterType.class),
        DATE(DateLikeType.class),
        OTHER(DataType.class)
        ;

        private final Class<? extends DataType> superClazz;

        InferType(Class<? extends DataType> superClazz) {
            this.superClazz = superClazz;
        }
    }

    private class ComparisonInferInfo {

        public final InferType inferType;
        public final Optional<Expression> left;
        public final Optional<Expression> right;
        public final ComparisonPredicate comparisonPredicate;

        public ComparisonInferInfo(InferType inferType,
                Optional<Expression> left, Optional<Expression> right,
                ComparisonPredicate comparisonPredicate) {
            this.inferType = inferType;
            this.left = left;
            this.right = right;
            this.comparisonPredicate = comparisonPredicate;
        }
    }

    /**
     * infer additional predicates.
     */
    public Set<Expression> infer(Set<Expression> predicates) {
        Set<Expression> inferred = Sets.newHashSet();
        for (Expression predicate : predicates) {
            // if we support more infer predicate expression type, we should impl withInferred() method.
            // And should add inferred props in withChildren() method just like ComparisonPredicate,
            // and it's subclass, to mark the predicate is from infer.
            if (!(predicate instanceof ComparisonPredicate)) {
                continue;
            }
            ComparisonInferInfo equalInfo = getEquivalentInferInfo((ComparisonPredicate) predicate);
            if (equalInfo.inferType == InferType.NONE) {
                continue;
            }
            Set<Expression> newInferred = predicates.stream()
                    .filter(ComparisonPredicate.class::isInstance)
                    .filter(p -> !p.equals(predicate))
                    .map(ComparisonPredicate.class::cast)
                    .map(this::inferInferInfo)
                    .filter(predicateInfo -> predicateInfo.inferType != InferType.NONE)
                    .map(predicateInfo -> doInfer(equalInfo, predicateInfo))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            inferred.addAll(newInferred);
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
    private Expression doInfer(ComparisonInferInfo equalInfo, ComparisonInferInfo predicateInfo) {
        Expression predicateLeft = predicateInfo.left.get();
        Expression predicateRight = predicateInfo.right.get();
        Expression equalLeft = equalInfo.left.get();
        Expression equalRight = equalInfo.right.get();
        Expression newLeft = inferOneSide(predicateLeft, equalLeft, equalRight);
        Expression newRight = inferOneSide(predicateRight, equalLeft, equalRight);
        if (newLeft == null || newRight == null) {
            return null;
        }
        ComparisonPredicate newPredicate = (ComparisonPredicate) predicateInfo
                .comparisonPredicate.withChildren(newLeft, newRight);
        Expression expr = SimplifyComparisonPredicate.INSTANCE
                .rewrite(TypeCoercionUtils.processComparisonPredicate(newPredicate), null);
        return DateFunctionRewrite.INSTANCE.rewrite(expr, null).withInferred(true);
    }

    private Expression inferOneSide(Expression predicateOneSide, Expression equalLeft, Expression equalRight) {
        if (predicateOneSide instanceof SlotReference) {
            if (predicateOneSide.equals(equalLeft)) {
                return equalRight;
            } else if (predicateOneSide.equals(equalRight)) {
                return equalLeft;
            }
        } else if (predicateOneSide.isConstant()) {
            if (predicateOneSide instanceof IntegerLikeLiteral) {
                return new NereidsParser().parseExpression(((IntegerLikeLiteral) predicateOneSide).toSql());
            } else {
                return predicateOneSide;
            }
        }
        return null;
    }

    private Optional<Expression> validForInfer(Expression expression, InferType inferType) {
        if (!inferType.superClazz.isAssignableFrom(expression.getDataType().getClass())) {
            return Optional.empty();
        }
        if (expression instanceof SlotReference || expression.isConstant()) {
            return Optional.of(expression);
        }
        if (inferType == InferType.INTEGRAL) {
            if (expression instanceof Cast) {
                // avoid cast from wider type to narrower type, such as cast(int as smallint)
                // IntegralType dataType = (IntegralType) expression.getDataType();
                // DataType childType = ((Cast) expression).child().getDataType();
                // if (childType instanceof IntegralType && dataType.widerThan((IntegralType) childType)) {
                //     return validForInfer(((Cast) expression).child(), inferType);
                // }
                return validForInfer(((Cast) expression).child(), inferType);
            }
        } else if (inferType == InferType.DATE) {
            if (expression instanceof Cast) {
                DataType dataType = expression.getDataType();
                DataType childType = ((Cast) expression).child().getDataType();
                // avoid lost precision
                if (dataType instanceof DateType) {
                    if (childType instanceof DateV2Type || childType instanceof DateType) {
                        return validForInfer(((Cast) expression).child(), inferType);
                    }
                } else if (dataType instanceof DateV2Type) {
                    if (childType instanceof DateType || childType instanceof DateV2Type) {
                        return validForInfer(((Cast) expression).child(), inferType);
                    }
                } else if (dataType instanceof DateTimeType) {
                    if (!(childType instanceof DateTimeV2Type)) {
                        return validForInfer(((Cast) expression).child(), inferType);
                    }
                } else if (dataType instanceof DateTimeV2Type) {
                    return validForInfer(((Cast) expression).child(), inferType);
                }
            }
        } else if (inferType == InferType.STRING) {
            if (expression instanceof Cast) {
                DataType dataType = expression.getDataType();
                DataType childType = ((Cast) expression).child().getDataType();
                // avoid substring cast such as cast(char(3) as char(2))
                if (dataType.width() <= 0 || (dataType.width() >= childType.width() && childType.width() >= 0)) {
                    return validForInfer(((Cast) expression).child(), inferType);
                }
            }
        } else {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private ComparisonInferInfo inferInferInfo(ComparisonPredicate comparisonPredicate) {
        DataType leftType = comparisonPredicate.left().getDataType();
        InferType inferType;
        if (leftType instanceof CharacterType) {
            inferType = InferType.STRING;
        } else if (leftType instanceof IntegralType) {
            inferType = InferType.INTEGRAL;
        } else if (leftType instanceof DateLikeType) {
            inferType = InferType.DATE;
        } else {
            inferType = InferType.OTHER;
        }
        Optional<Expression> left = validForInfer(comparisonPredicate.left(), inferType);
        Optional<Expression> right = validForInfer(comparisonPredicate.right(), inferType);
        if (!left.isPresent() || !right.isPresent()) {
            inferType = InferType.NONE;
        }
        return new ComparisonInferInfo(inferType, left, right, comparisonPredicate);
    }

    /**
     * Currently only equivalence derivation is supported
     * and requires that the left and right sides of an expression must be slot
     */
    private ComparisonInferInfo getEquivalentInferInfo(ComparisonPredicate predicate) {
        if (!(predicate instanceof EqualTo)) {
            return new ComparisonInferInfo(InferType.NONE,
                    Optional.of(predicate.left()), Optional.of(predicate.right()), predicate);
        }
        ComparisonInferInfo info = inferInferInfo(predicate);
        if (info.inferType == InferType.NONE) {
            return info;
        }
        if (info.left.get() instanceof SlotReference && info.right.get() instanceof SlotReference) {
            return info;
        }
        return new ComparisonInferInfo(InferType.NONE, info.left, info.right, info.comparisonPredicate);
    }
}
