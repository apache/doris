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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.rewrite.InferPredicateByReplace;
import org.apache.doris.nereids.rules.rewrite.UnequalPredicateInfer;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/** PredicateInferUtils */
public class PredicateInferUtils {
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

    public static boolean isSlotOrLiteral(Expression expr) {
        return expr instanceof SlotReference || expr instanceof Literal;
    }

    /**The inputs predicate is divided into two parts. One is the predicate directly reserved, which does not enter
     *  the non equivalent derivation, and the other is the predicates entering the non equivalent derivation*/
    public static void getComplexAndSimplePredicates(Set<Expression> inputs, Set<Expression> complex,
            Set<ComparisonPredicate> simple) {
        for (Expression input : inputs) {
            if (input instanceof GreaterThan || input instanceof GreaterThanEqual
                    || input instanceof EqualTo || input instanceof LessThan
                    || input instanceof LessThanEqual) {
                simple.add((ComparisonPredicate) input);
            } else {
                complex.add(input);
            }
        }
    }

    /**The predicate derivation is based on the input predicate predicates, which is divided into two parts.
     * The equivalent relation used in ReplacePredicate and calculated by union-find derive like, in, not
     * and ComparisonPredicate;
     * The NonEqualPredicateInfer class deduces predicates based on non-equal relations, and deletes
     * the useless ComparisonPredicates derived from ReplacePredicate*/
    public static Set<Expression> inferPredicate(Set<Expression> predicates) {
        if (predicates.size() < 2) {
            return predicates;
        }
        Set<Expression> inferAndOriginPredicates = InferPredicateByReplace.infer(predicates);
        Set<Expression> inferPredicates = new LinkedHashSet<>(
                UnequalPredicateInfer.inferUnequalPredicates(inferAndOriginPredicates));
        // Keep the order of predicates. The input predicates are in the front
        // and the derived predicates are in the rear
        Set<Expression> res = new LinkedHashSet<>();
        for (Expression pred : predicates) {
            if (inferPredicates.contains(pred)) {
                res.add(pred);
                inferPredicates.remove(pred);
            }
        }
        res.addAll(inferPredicates);
        return res;
    }

    /** get all predicates(with redundant predicates), e.g. b>1 a>b -> a>1 a>b b>1*/
    public static Set<Expression> inferAllPredicate(Set<Expression> predicates) {
        if (predicates.size() < 2) {
            return predicates;
        }
        Set<Expression> inferAndOriginPredicates = InferPredicateByReplace.infer(predicates);
        return new LinkedHashSet<>(UnequalPredicateInfer.inferAllPredicates(inferAndOriginPredicates));
    }

    /**getPairFromCast*/
    public static Optional<Pair<Expression, Expression>> getPairFromCast(ComparisonPredicate comparisonPredicate) {
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

    private static Optional<Expression> validForInfer(Expression expression, InferType inferType) {
        if (!inferType.superClazz.isAssignableFrom(expression.getDataType().getClass())) {
            return Optional.empty();
        }
        if (!(expression instanceof Cast)) {
            return Optional.of(expression);
        }
        Cast cast = (Cast) expression;
        Expression child = cast.child();
        DataType dataType = cast.getDataType();
        DataType childType = child.getDataType();
        if (inferType == InferType.INTEGRAL) {
            if (dataType instanceof IntegralType) {
                IntegralType integralType = (IntegralType) dataType;
                if (childType instanceof IntegralType && integralType.widerThan((IntegralType) childType)) {
                    return validForInfer(((Cast) expression).child(), inferType);
                }
            }
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
}
