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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    /**
     * infer additional predicates.
     */
    public static Set<Expression> infer(Set<Expression> predicates) {
        ImmutableEqualSet.Builder<Slot> equalSetBuilder = new ImmutableEqualSet.Builder<>();
        Map<Slot, List<Expression>> slotPredicates = new HashMap<>();
        Set<Pair<Slot, Slot>> equalPairs = new HashSet<>();
        for (Expression predicate : predicates) {
            Set<Slot> inputSlots = predicate.getInputSlots();
            if (inputSlots.size() == 1) {
                if (predicate instanceof ComparisonPredicate
                        || (predicate instanceof InPredicate && ((InPredicate) predicate).isLiteralChildren())) {
                    slotPredicates.computeIfAbsent(inputSlots.iterator().next(), k -> new ArrayList<>()).add(predicate);
                }
                continue;
            }

            if (predicate instanceof EqualTo) {
                getEqualSlot(equalSetBuilder, equalPairs, (EqualTo) predicate);
            }
        }

        ImmutableEqualSet<Slot> equalSet = equalSetBuilder.build();

        Set<Expression> inferred = new HashSet<>();
        slotPredicates.forEach((left, exprs) -> {
            for (Slot right : equalSet.calEqualSet(left)) {
                for (Expression expr : exprs) {
                    Expression inferPredicate = doInferPredicate(left, right, expr);
                    if (inferPredicate != null) {
                        inferred.add(inferPredicate);
                    }
                }
            }
        });

        // infer equal to equal like a = b & b = c -> a = c
        // a b c | e f g
        // get (a b) (a c) (b c) | (e f) (e g) (f g)
        List<Set<Slot>> equalSetList = equalSet.calEqualSetList();
        for (Set<Slot> es : equalSetList) {
            List<Slot> el = es.stream().sorted(Comparator.comparingInt(s -> s.getExprId().asInt()))
                    .collect(Collectors.toList());
            for (int i = 0; i < el.size(); i++) {
                Slot left = el.get(i);
                for (int j = i + 1; j < el.size(); j++) {
                    Slot right = el.get(j);
                    if (!equalPairs.contains(Pair.of(left, right))) {
                        inferred.add(TypeCoercionUtils.processComparisonPredicate(new EqualTo(left, right))
                                .withInferred(true));
                    }
                }
            }
        }

        return inferred;
    }

    private static Expression doInferPredicate(Expression equalLeft, Expression equalRight, Expression predicate) {
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
            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate) newPredicate).withInferred(true);
        } else {
            return TypeCoercionUtils.processInPredicate((InPredicate) newPredicate).withInferred(true);
        }
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

    private static Optional<Pair<Expression, Expression>> inferInferInfo(ComparisonPredicate comparisonPredicate) {
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
            return Optional.empty();
        }
        return Optional.of(Pair.of(left.get(), right.get()));
    }

    private static void getEqualSlot(ImmutableEqualSet.Builder<Slot> equalSlots, Set<Pair<Slot, Slot>> equalPairs,
            EqualTo predicate) {
        inferInferInfo(predicate)
                .filter(info -> info.first instanceof Slot && info.second instanceof Slot)
                .ifPresent(pair -> {
                    Slot left = (Slot) pair.first;
                    Slot right = (Slot) pair.second;
                    equalSlots.addEqualPair(left, right);
                    equalPairs.add(left.getExprId().asInt() <= right.getExprId().asInt()
                            ? Pair.of(left, right) : Pair.of(right, left));
                });
    }
}
