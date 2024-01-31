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
import org.apache.doris.nereids.trees.expressions.InPredicate;
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
        OTHER(DataType.class);

        private final Class<? extends DataType> superClazz;

        InferType(Class<? extends DataType> superClazz) {
            this.superClazz = superClazz;
        }
    }

    private static class EqualInferInfo {

        public final InferType inferType;
        public final Expression left;
        public final Expression right;
        public final ComparisonPredicate comparisonPredicate;

        public EqualInferInfo(InferType inferType,
                Expression left, Expression right,
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
    public static Set<Expression> infer(Set<Expression> predicates) {
        Set<Expression> inferred = Sets.newHashSet();
        for (Expression predicate : predicates) {
            if (!(predicate instanceof ComparisonPredicate
                    || (predicate instanceof InPredicate && ((InPredicate) predicate).isLiteralChildren()))) {
                continue;
            }
            if (predicate instanceof InPredicate) {
                continue;
            }
            EqualInferInfo equalInfo = getEqualInferInfo((ComparisonPredicate) predicate);
            if (equalInfo.inferType == InferType.NONE) {
                continue;
            }
            Set<Expression> newInferred = predicates.stream()
                    .filter(p -> !p.equals(predicate))
                    .filter(p -> p instanceof ComparisonPredicate || p instanceof InPredicate)
                    .map(predicateInfo -> doInferPredicate(equalInfo, predicateInfo))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
            inferred.addAll(newInferred);
        }
        inferred.removeAll(predicates);
        return inferred;
    }

    private static Expression doInferPredicate(EqualInferInfo equalInfo, Expression predicate) {
        Expression equalLeft = equalInfo.left;
        Expression equalRight = equalInfo.right;

        DataType leftType = predicate.child(0).getDataType();
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
        if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;
            Optional<Expression> left = validForInfer(comparisonPredicate.left(), inferType);
            Optional<Expression> right = validForInfer(comparisonPredicate.right(), inferType);
            if (!left.isPresent() || !right.isPresent()) {
                return null;
            }
        } else if (predicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) predicate;
            Optional<Expression> left = validForInfer(inPredicate.getCompareExpr(), inferType);
            if (!left.isPresent()) {
                return null;
            }
        }

        Expression newPredicate = predicate.rewriteUp(e -> {
            if (e.equals(equalLeft)) {
                return equalRight;
            } else if (e.equals(equalRight)) {
                return equalLeft;
            } else {
                return e;
            }
        });
        if (predicate instanceof ComparisonPredicate) {
            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate) newPredicate,
                    newPredicate.child(0),
                    newPredicate.child(1));
        } else {
            return TypeCoercionUtils.processInPredicate((InPredicate) newPredicate);
        }
    }

    /**
     * Use the left or right child of `leftSlotEqualToRightSlot` to replace the left or right child of `expression`
     * Now only support infer `ComparisonPredicate`.
     * TODO: We should determine whether `expression` satisfies the condition for replacement
     *       eg: Satisfy `expression` is non-deterministic
     */
    private static Expression doInfer(EqualInferInfo equalInfo, EqualInferInfo predicateInfo) {
        Expression equalLeft = equalInfo.left;
        Expression equalRight = equalInfo.right;

        Expression predicateLeft = predicateInfo.left;
        Expression predicateRight = predicateInfo.right;
        Expression newLeft = inferOneSide(predicateLeft, equalLeft, equalRight);
        Expression newRight = inferOneSide(predicateRight, equalLeft, equalRight);
        if (newLeft == null || newRight == null) {
            return null;
        }
        ComparisonPredicate newPredicate = (ComparisonPredicate) predicateInfo
                .comparisonPredicate.withChildren(newLeft, newRight);
        Expression expr = SimplifyComparisonPredicate.INSTANCE
                .rewrite(TypeCoercionUtils.processComparisonPredicate(newPredicate, newLeft, newRight), null);
        return DateFunctionRewrite.INSTANCE.rewrite(expr, null);
    }

    private static Expression inferOneSide(Expression predicateOneSide, Expression equalLeft, Expression equalRight) {
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

    private static Optional<Expression> validForInfer(Expression expression, InferType inferType) {
        if (!inferType.superClazz.isAssignableFrom(expression.getDataType().getClass())) {
            return Optional.empty();
        }
        if (expression instanceof SlotReference || expression.isConstant()) {
            return Optional.of(expression);
        }
        if (!(expression instanceof Cast)) {
            return Optional.empty();
        }
        Cast cast = (Cast) expression;
        Expression child = cast.child();
        DataType dataType = cast.getDataType();
        DataType childType = child.getDataType();
        if (inferType == InferType.INTEGRAL) {
            // avoid cast from wider type to narrower type, such as cast(int as smallint)
            // IntegralType dataType = (IntegralType) expression.getDataType();
            // DataType childType = ((Cast) expression).child().getDataType();
            // if (childType instanceof IntegralType && dataType.widerThan((IntegralType) childType)) {
            //     return validForInfer(((Cast) expression).child(), inferType);
            // }
            return validForInfer(child, inferType);
        } else if (inferType == InferType.DATE) {
            // avoid lost precision
            if (dataType instanceof DateType) {
                if (childType instanceof DateV2Type || childType instanceof DateType) {
                    return validForInfer(child, inferType);
                }
            } else if (dataType instanceof DateV2Type) {
                if (childType instanceof DateType || childType instanceof DateV2Type) {
                    return validForInfer(child, inferType);
                }
            } else if (dataType instanceof DateTimeType) {
                if (!(childType instanceof DateTimeV2Type)) {
                    return validForInfer(child, inferType);
                }
            } else if (dataType instanceof DateTimeV2Type) {
                return validForInfer(child, inferType);
            }
        } else if (inferType == InferType.STRING) {
            // avoid substring cast such as cast(char(3) as char(2))
            if (dataType.width() <= 0 || (dataType.width() >= childType.width() && childType.width() >= 0)) {
                return validForInfer(child, inferType);
            }
        }
        return Optional.empty();
    }

    private static EqualInferInfo inferInferInfo(ComparisonPredicate comparisonPredicate) {
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
        return new EqualInferInfo(inferType, left.orElse(comparisonPredicate.left()),
                right.orElse(comparisonPredicate.right()), comparisonPredicate);
    }

    /**
     * Currently only equivalence derivation is supported
     * and requires that the left and right sides of an expression must be slot
     * <p>
     * TODO: NullSafeEqual
     */
    private static EqualInferInfo getEqualInferInfo(ComparisonPredicate predicate) {
        if (!(predicate instanceof EqualTo)) {
            return new EqualInferInfo(InferType.NONE, predicate.left(), predicate.right(), predicate);
        }
        EqualInferInfo info = inferInferInfo(predicate);
        if (info.inferType == InferType.NONE) {
            return info;
        }
        if (info.left instanceof SlotReference && info.right instanceof SlotReference) {
            return info;
        }
        return new EqualInferInfo(InferType.NONE, info.left, info.right, info.comparisonPredicate);
    }
}
