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
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.rules.AbstractEqualSet;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**ReplacePredicate*/
public class ReplacePredicate {
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

    private static Set<Expression> getAllSubExpressions(Expression expr) {
        Set<Expression> subExpressions = new HashSet<>();
        getAllSubExpressions(expr, subExpressions);
        return subExpressions;
    }

    private static void getAllSubExpressions(Expression expr, Set<Expression> res) {
        res.add(expr);
        if (expr.children().size() != 1) {
            Set<Slot> slots = expr.getInputSlots();
            if (slots.size() == 1) {
                res.add(slots.iterator().next());
            }
            return;
        }
        getAllSubExpressions(expr.child(0), res);
    }

    /**fill map exprPredicates : expression and all its corresponding predicates*/
    private static class PredicatesCollector extends ExpressionVisitor<Void, Map<Expression, Set<Expression>>> {
        public static PredicatesCollector INSTANCE = new PredicatesCollector();

        @Override
        public Void visit(Expression expr, Map<Expression, Set<Expression>> context) {
            return null;
        }

        @Override
        public Void visitOr(Or expr, Map<Expression, Set<Expression>> context) {
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate inPredicate, Map<Expression, Set<Expression>> context) {
            if (!validInPredicate(inPredicate)) {
                return null;
            }
            for (Expression expr : getAllSubExpressions(inPredicate.getCompareExpr())) {
                context.computeIfAbsent(expr, k -> new HashSet<>()).add(inPredicate);
            }
            return null;
        }

        @Override
        public Void visitComparisonPredicate(ComparisonPredicate comparisonPredicate,
                Map<Expression, Set<Expression>> context) {
            if (!validComparisonPredicate(comparisonPredicate)) {
                return null;
            }
            // It is believed that 1<a has been rewritten as a>1
            for (Expression expr : getAllSubExpressions(comparisonPredicate.child(0))) {
                context.computeIfAbsent(expr, k -> new HashSet<>()).add(comparisonPredicate);
            }
            return null;
        }

        @Override
        public Void visitNot(Not not, Map<Expression, Set<Expression>> context) {
            if (not.child(0) instanceof InPredicate && validInPredicate((InPredicate) not.child(0))
                    || not.child(0) instanceof ComparisonPredicate
                    && validComparisonPredicate((ComparisonPredicate) not.child(0))) {
                for (Expression expr : getAllSubExpressions(not.child(0).child(0))) {
                    context.computeIfAbsent(expr, k -> new HashSet<>()).add(not);
                }
            }
            return null;
        }

        @Override
        public Void visitLike(Like like, Map<Expression, Set<Expression>> context) {
            if (!(like.child(1) instanceof Literal)) {
                return null;
            }
            for (Expression expr : getAllSubExpressions(like.child(0))) {
                context.computeIfAbsent(expr, k -> new HashSet<>()).add(like);
            }
            return null;
        }

        private boolean validComparisonPredicate(ComparisonPredicate comparisonPredicate) {
            return comparisonPredicate.right() instanceof Literal;
        }

        private boolean validInPredicate(InPredicate inPredicate) {
            return inPredicate.isLiteralChildren();
        }
    }

    /* replaceToThis: find all predicates that replaceToThis can deduce (e.g. replaceToThis = b)
     equalSet: the equivalent set of replaceToThis (e.g. equalSet: a=b)
     exprPredicates: expression and all its corresponding predicates (e.g. such as {a: [a<10, a>1], b: [b in (1, 2)]})
     return: all predicates that replaceToThis can deduce (return b<10, b>1) */
    private static <T extends Expression> Set<Expression> getEqualSetAndDoReplace(T replaceToThis, Set<T> equalSet,
            Map<? extends Expression, Set<Expression>> exprPredicates) {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, new Scope(ImmutableList.of()), null, false, false);
        Set<Expression> res = new HashSet<>();
        for (T equals : equalSet) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            replaceMap.put(equals, replaceToThis);
            if (!exprPredicates.containsKey(equals)) {
                continue;
            }
            for (Expression predicate : exprPredicates.get(equals)) {
                Expression newPredicates = Replacer.INSTANCE.replace(predicate, replaceMap);
                res.add(analyzer.analyze(newPredicates).withInferred(true));
            }
        }
        return res;
    }

    /* Extract the equivalence relationship a=b, and when case (d_tinyint as int)=d_int is encountered,
    remove the cast and extract d_tinyint=d_int
    EqualPairs is the output parameter and the equivalent pair of predicate derivation input,
    which is used to ensure that the derivation
    does not generate repeated equivalent conditions, such as a=b and b=a */
    private static AbstractEqualSet<Expression> findEqual(Set<Expression> inputs) {
        AbstractEqualSet.Builder<Expression> fromCastEqualSetBuilder = new ImmutableEqualSet.Builder<>();
        for (Expression input : inputs) {
            if (!(input instanceof EqualTo)) {
                continue;
            }
            EqualTo equalTo = (EqualTo) input;
            Set<Slot> leftInputSlots = equalTo.left().getInputSlots();
            Set<Slot> rightInputSlots = equalTo.right().getInputSlots();
            // not support a=1 b=1 -> a=b, because Sometimes there are cases where the predicates a=1 and a=2
            // are not eliminated in time. After being pulled up, it is wrong to deduce a=b with b=1.
            if (leftInputSlots.isEmpty() || rightInputSlots.isEmpty()) {
                continue;
            }
            getEqualPair((ComparisonPredicate) input)
                    .filter(pair -> pair.first instanceof Slot && pair.second instanceof Slot)
                    .ifPresent(pair -> fromCastEqualSetBuilder.addEqualPair(pair.first, pair.second));
        }
        return fromCastEqualSetBuilder.build();
    }

    /** This is the exposed interface. Inputs are the input predicates for derivation.
     * The return value is the derived predicates*/
    public static Set<Expression> infer(Set<Expression> inputs) {
        AbstractEqualSet<Expression> hasCastEqualSet = findEqual(inputs);
        Set<Expression> targetExprs = hasCastEqualSet.getAllItemSet();
        if (targetExprs.isEmpty()) {
            return ImmutableSet.of();
        }
        Map<Expression, Set<Expression>> exprPredicates = new HashMap<>();
        for (Expression input : inputs) {
            if (input.anyMatch(expr -> !((ExpressionTrait) expr).isDeterministic())
                    || input.getInputSlots().size() != 1) {
                continue;
            }
            input.accept(PredicatesCollector.INSTANCE, exprPredicates);
        }
        Set<Expression> inferPredicates = new HashSet<>();
        if (!exprPredicates.isEmpty()) {
            for (Expression expr : targetExprs) {
                if (expr instanceof Literal) {
                    continue;
                }
                inferPredicates.addAll(getEqualSetAndDoReplace(expr, hasCastEqualSet.calEqualSet(expr),
                        exprPredicates));
            }
        }
        inferPredicates.addAll(deduceTransitiveEquality(hasCastEqualSet, inputs));
        return inferPredicates;
    }

    private static class Replacer extends DefaultExpressionRewriter<Map<Expression, Expression>> {
        public static Replacer INSTANCE = new Replacer();

        public Expression replace(Expression e, Map<Expression, Expression> replaceMap) {
            return e.accept(this, replaceMap);
        }

        @Override
        public Expression visit(Expression expr, Map<Expression, Expression> replaceMap) {
            expr = rewriteChildren(this, expr, replaceMap);
            if (replaceMap.containsKey(expr)) {
                return replaceMap.get(expr);
            }
            return expr;
        }
    }

    private static Optional<Pair<Expression, Expression>> getEqualPair(ComparisonPredicate comparisonPredicate) {
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

    /* This function is used to input a=b b=c to derive a=c, and return a=b b=c a=c.
     This function is not called temporarily */
    private static Set<Expression> deduceTransitiveEquality(AbstractEqualSet<Expression> equalSet,
            Set<Expression> inputs) {
        List<Set<Expression>> equalSetList = equalSet.calEqualSetList();
        Set<Expression> derivedEqualities = new HashSet<>();
        for (Set<Expression> es : equalSetList) {
            List<Expression> el = new ArrayList<>(es);
            for (int i = 0; i < el.size(); i++) {
                Expression left = el.get(i);
                for (int j = i + 1; j < el.size(); j++) {
                    Expression right = el.get(j);
                    EqualTo newEqualTo = new EqualTo(left, right);
                    if (inputs.contains(newEqualTo) || inputs.contains(newEqualTo.commute())) {
                        continue;
                    }
                    if (isSingleTableExpression(newEqualTo)) {
                        continue;
                    }
                    derivedEqualities.add(TypeCoercionUtils.processComparisonPredicate(newEqualTo)
                            .withInferred(true));
                }
            }
        }
        return derivedEqualities;
    }

    private static boolean isSingleTableExpression(Expression expr) {
        Set<String> qualifiers = new HashSet<>();
        for (Slot slot : expr.getInputSlots()) {
            qualifiers.add(String.join(".", slot.getQualifier()));
        }
        return qualifiers.size() == 1;
    }

}
