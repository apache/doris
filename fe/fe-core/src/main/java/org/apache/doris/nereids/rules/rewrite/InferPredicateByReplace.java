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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.PredicateInferUtils;

import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**ReplacePredicate*/
public class InferPredicateByReplace {
    private static List<Expression> getAllSubExpressions(Expression expr) {
        List<Expression> subExpressions = new ArrayList<>();
        getAllSubExpressions(expr, subExpressions);
        return subExpressions;
    }

    private static void getAllSubExpressions(Expression expr, List<Expression> res) {
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

    /** fill map exprPredicates : expression and all its corresponding predicates */
    private static class PredicatesCollector extends ExpressionVisitor<Void, Map<Expression, Set<Expression>>> {
        public static PredicatesCollector INSTANCE = new PredicatesCollector();

        @Override
        public Void visit(Expression expr, Map<Expression, Set<Expression>> context) {
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate inPredicate, Map<Expression, Set<Expression>> context) {
            if (!validInPredicate(inPredicate)) {
                return null;
            }
            for (Expression expr : getAllSubExpressions(inPredicate.getCompareExpr())) {
                context.computeIfAbsent(expr, k -> new LinkedHashSet<>()).add(inPredicate);
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
                context.computeIfAbsent(expr, k -> new LinkedHashSet<>()).add(comparisonPredicate);
            }
            return null;
        }

        @Override
        public Void visitNot(Not not, Map<Expression, Set<Expression>> context) {
            if (not.child(0) instanceof InPredicate && validInPredicate((InPredicate) not.child(0))
                    || not.child(0) instanceof ComparisonPredicate
                    && validComparisonPredicate((ComparisonPredicate) not.child(0))) {
                for (Expression expr : getAllSubExpressions(not.child(0).child(0))) {
                    context.computeIfAbsent(expr, k -> new LinkedHashSet<>()).add(not);
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
                context.computeIfAbsent(expr, k -> new LinkedHashSet<>()).add(like);
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
        ExpressionAnalyzer analyzer = new ReplaceAnalyzer(null, new Scope(ImmutableList.of()), null, false, false);
        Set<Expression> res = new LinkedHashSet<>();
        for (T equals : equalSet) {
            Map<Expression, Expression> replaceMap = new HashMap<>();
            replaceMap.put(equals, replaceToThis);
            if (!exprPredicates.containsKey(equals)) {
                continue;
            }
            for (Expression predicate : exprPredicates.get(equals)) {
                Expression newPredicates = ExpressionUtils.replace(predicate, replaceMap);
                try {
                    Expression analyzed = analyzer.analyze(newPredicates);
                    res.add(analyzed.withInferred(true));
                } catch (Exception e) {
                    // has cast error, just not infer and do nothing
                }
            }
        }
        return res;
    }

    /* Extract the equivalence relationship a=b, and when case (d_tinyint as int)=d_int is encountered,
    remove the cast and extract d_tinyint=d_int
    EqualPairs is the output parameter and the equivalent pair of predicate derivation input,
    which is used to ensure that the derivation
    does not generate repeated equivalent conditions, such as a=b and b=a */
    private static ImmutableEqualSet<Expression> findEqual(Set<Expression> inputs) {
        ImmutableEqualSet.Builder<Expression> fromCastEqualSetBuilder = new ImmutableEqualSet.Builder<>();
        for (Expression input : inputs) {
            if (!(input instanceof EqualTo)) {
                continue;
            }
            EqualTo equalTo = (EqualTo) input;
            Set<Slot> leftInputSlots = equalTo.left().getInputSlots();
            Set<Slot> rightInputSlots = equalTo.right().getInputSlots();
            if (leftInputSlots.isEmpty() && rightInputSlots.isEmpty()) {
                continue;
            }
            PredicateInferUtils.getPairFromCast((ComparisonPredicate) input)
                    .filter(pair -> PredicateInferUtils.isSlotOrLiteral(pair.first)
                            && PredicateInferUtils.isSlotOrLiteral(pair.second))
                    .filter(pair -> !(pair.first instanceof NullLiteral) && !(pair.second instanceof NullLiteral))
                    .ifPresent(pair -> {
                        Expression left = pair.first;
                        Expression right = pair.second;
                        fromCastEqualSetBuilder.addEqualPair(left, right);
                    });
        }
        return fromCastEqualSetBuilder.build();
    }

    /** This is the exposed interface. Inputs are the input predicates for derivation.
     * The return value is the derived predicates*/
    public static Set<Expression> infer(Set<Expression> inputs) {
        ImmutableEqualSet<Expression> hasCastEqualSet = findEqual(inputs);
        Set<Expression> targetExprs = hasCastEqualSet.getAllItemSet();
        if (targetExprs.isEmpty()) {
            return new LinkedHashSet<>(inputs);
        }
        Map<Expression, Set<Expression>> exprPredicates = new HashMap<>();
        for (Expression input : inputs) {
            if (input.anyMatch(expr -> !((ExpressionTrait) expr).isDeterministic())
                    || input.getInputSlots().size() != 1) {
                continue;
            }
            input.accept(PredicatesCollector.INSTANCE, exprPredicates);
        }
        Set<Expression> inferPredicates = new LinkedHashSet<>(inputs);
        if (!exprPredicates.isEmpty()) {
            for (Expression expr : targetExprs) {
                if (expr instanceof Literal) {
                    continue;
                }
                inferPredicates.addAll(getEqualSetAndDoReplace(expr, hasCastEqualSet.calEqualSet(expr),
                        exprPredicates));
            }
        }
        return inferPredicates;
    }

    /** ReplaceAnalyzer is to perform type conversion on the expression after replacement
     * and perform type check on the expression.
     * If there is a cast that will cause an error during execution, an exception should be thrown. */
    private static class ReplaceAnalyzer extends ExpressionAnalyzer {
        private ReplaceAnalyzer(Plan currentPlan, Scope scope,
                @Nullable CascadesContext cascadesContext,
                boolean enableExactMatch, boolean bindSlotInOuterScope) {
            super(currentPlan, scope, cascadesContext, enableExactMatch, bindSlotInOuterScope);
        }

        @Override
        public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
            cast = (Cast) super.visitCast(cast, context);
            if (cast.getDataType().isDecimalV3Type()) {
                DecimalV3Type targetType = (DecimalV3Type) cast.getDataType();
                DecimalV3Type childType = DecimalV3Type.forType(cast.child().getDataType());
                if ((childType.getPrecision() - childType.getScale())
                        > (targetType.getPrecision() - targetType.getScale())
                        || childType.getScale() > targetType.getScale()) {
                    throw new AnalysisException("can not cast from origin type " + cast.child().getDataType()
                            + " to target type=" + targetType);
                }
            } else if (cast.getDataType().isDecimalV2Type()) {
                DecimalV2Type targetType = (DecimalV2Type) cast.getDataType();
                DecimalV2Type childType = DecimalV2Type.forType(cast.child().getDataType());
                if ((childType.getPrecision() - childType.getScale())
                        > (targetType.getPrecision() - targetType.getScale())
                        || childType.getScale() > targetType.getScale()) {
                    throw new AnalysisException("can not cast from origin type " + cast.child().getDataType()
                            + " to target type=" + targetType);
                }
            }
            return cast;
        }
    }
}
