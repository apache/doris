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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Expression rewrite helper class.
 */
public class ExpressionUtils {

    public static final List<Expression> EMPTY_CONDITION = ImmutableList.of();

    public static List<Expression> extractConjunction(Expression expr) {
        return extract(And.class, expr);
    }

    public static Set<Expression> extractConjunctionToSet(Expression expr) {
        Set<Expression> exprSet = Sets.newHashSet();
        extract(And.class, expr, exprSet);
        return exprSet;
    }

    public static List<Expression> extractDisjunction(Expression expr) {
        return extract(Or.class, expr);
    }

    /**
     * Split predicates with `And/Or` form recursively.
     * Some examples for `And`:
     * <p>
     * a and b -> a, b
     * (a and b) and c -> a, b, c
     * (a or b) and (c and d) -> (a or b), c , d
     * <p>
     * Stop recursion when meeting `Or`, so this func will ignore `And` inside `Or`.
     * Warning examples:
     * (a and b) or c -> (a and b) or c
     */
    public static List<Expression> extract(CompoundPredicate expr) {
        return extract(expr.getClass(), expr);
    }

    private static List<Expression> extract(Class<? extends Expression> type, Expression expr) {
        List<Expression> result = Lists.newArrayList();
        extract(type, expr, result);
        return result;
    }

    private static void extract(Class<? extends Expression> type, Expression expr, Collection<Expression> result) {
        if (type.isInstance(expr)) {
            CompoundPredicate predicate = (CompoundPredicate) expr;
            extract(type, predicate.left(), result);
            extract(type, predicate.right(), result);
        } else {
            result.add(expr);
        }
    }

    public static Optional<Expression> optionalAnd(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ExpressionUtils.and(expressions));
        }
    }

    /**
     * And two list.
     */
    public static Optional<Expression> optionalAnd(List<Expression> left, List<Expression> right) {
        if (left.isEmpty() && right.isEmpty()) {
            return Optional.empty();
        } else if (left.isEmpty()) {
            return optionalAnd(right);
        } else if (right.isEmpty()) {
            return optionalAnd(left);
        } else {
            return Optional.of(new And(optionalAnd(left).get(), optionalAnd(right).get()));
        }
    }

    public static Optional<Expression> optionalAnd(Expression... expressions) {
        return optionalAnd(Lists.newArrayList(expressions));
    }

    public static Optional<Expression> optionalAnd(Collection<Expression> collection) {
        return optionalAnd(ImmutableList.copyOf(collection));
    }

    public static Expression and(Collection<Expression> expressions) {
        return combine(And.class, expressions);
    }

    public static Expression and(Expression... expressions) {
        return combine(And.class, Lists.newArrayList(expressions));
    }

    public static Optional<Expression> optionalOr(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ExpressionUtils.or(expressions));
        }
    }

    public static Expression or(Expression... expressions) {
        return combine(Or.class, Lists.newArrayList(expressions));
    }

    public static Expression or(Collection<Expression> expressions) {
        return combine(Or.class, expressions);
    }

    /**
     * Use AND/OR to combine expressions together.
     */
    public static Expression combine(Class<? extends Expression> type, Collection<Expression> expressions) {
        /*
         *             (AB) (CD) E   ((AB)(CD))  E     (((AB)(CD))E)
         *               ▲   ▲   ▲       ▲       ▲          ▲
         *               │   │   │       │       │          │
         * A B C D E ──► A B C D E ──► (AB) (CD) E ──► ((AB)(CD)) E ──► (((AB)(CD))E)
         */
        Preconditions.checkArgument(type == And.class || type == Or.class);
        Objects.requireNonNull(expressions, "expressions is null");

        Expression shortCircuit = (type == And.class ? BooleanLiteral.FALSE : BooleanLiteral.TRUE);
        Expression skip = (type == And.class ? BooleanLiteral.TRUE : BooleanLiteral.FALSE);
        Set<Expression> distinctExpressions = Sets.newLinkedHashSetWithExpectedSize(expressions.size());
        for (Expression expression : expressions) {
            if (expression.equals(shortCircuit)) {
                return shortCircuit;
            } else if (!expression.equals(skip)) {
                distinctExpressions.add(expression);
            }
        }

        return distinctExpressions.stream()
                .reduce(type == And.class ? And::new : Or::new)
                .orElse(BooleanLiteral.of(type == And.class));
    }

    /**
     * Choose the minimum slot from input parameter.
     */
    public static <S extends NamedExpression> S selectMinimumColumn(Collection<S> slots) {
        Preconditions.checkArgument(!slots.isEmpty());
        S minSlot = null;
        for (S slot : slots) {
            if (minSlot == null) {
                minSlot = slot;
            } else {
                int slotDataTypeWidth = slot.getDataType().width();
                if (slotDataTypeWidth < 0) {
                    continue;
                }
                minSlot = slotDataTypeWidth < minSlot.getDataType().width()
                        || minSlot.getDataType().width() <= 0 ? slot : minSlot;
            }
        }
        return minSlot;
    }

    /**
     * Check whether the input expression is a {@link org.apache.doris.nereids.trees.expressions.Slot}
     * or at least one {@link Cast} on a {@link org.apache.doris.nereids.trees.expressions.Slot}
     * <p>
     * for example:
     * - SlotReference to a column:
     * col
     * - Cast on SlotReference:
     * cast(int_col as string)
     * cast(cast(int_col as long) as string)
     *
     * @param expr input expression
     * @return Return Optional[ExprId] of underlying slot reference if input expression is a slot or cast on slot.
     *         Otherwise, return empty optional result.
     */
    public static Optional<ExprId> isSlotOrCastOnSlot(Expression expr) {
        return extractSlotOrCastOnSlot(expr).map(Slot::getExprId);
    }

    /**
     * Check whether the input expression is a {@link org.apache.doris.nereids.trees.expressions.Slot}
     * or at least one {@link Cast} on a {@link org.apache.doris.nereids.trees.expressions.Slot}
     */
    public static Optional<Slot> extractSlotOrCastOnSlot(Expression expr) {
        while (expr instanceof Cast) {
            expr = expr.child(0);
        }

        if (expr instanceof SlotReference) {
            return Optional.of((Slot) expr);
        } else {
            return Optional.empty();
        }
    }

    /**
     * get slot covered by cast
     * example: input: cast(cast(table.columnA)) output: columnA.datatype
     *
     */
    public static DataType getDatatypeCoveredByCast(Expression expr) {
        if (expr instanceof Cast) {
            return getDatatypeCoveredByCast(((Cast) expr).child());
        }
        return expr.getDataType();
    }

    /**
     * judge if expression is slot covered by cast
     * example: cast(cast(table.columnA))
     */
    public static boolean isExpressionSlotCoveredByCast(Expression expr) {
        if (expr instanceof Cast) {
            return isExpressionSlotCoveredByCast(((Cast) expr).child());
        }
        return expr instanceof SlotReference;
    }

    public static boolean isTwoExpressionEqualWithCast(Expression left, Expression right) {
        return ExpressionUtils.extractSlotOrCastOnSlot(left)
            .equals(ExpressionUtils.extractSlotOrCastOnSlot(right));
    }

    /**
     * Replace expression node in the expression tree by `replaceMap` in top-down manner.
     * For example.
     * <pre>
     * input expression: a > 1
     * replaceMap: a -> b + c
     *
     * output:
     * b + c > 1
     * </pre>
     */
    public static Expression replace(Expression expr, Map<? extends Expression, ? extends Expression> replaceMap) {
        return expr.accept(ExpressionReplacer.INSTANCE, replaceMap);
    }

    public static List<Expression> replace(List<Expression> exprs,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        return exprs.stream()
                .map(expr -> replace(expr, replaceMap))
                .collect(ImmutableList.toImmutableList());
    }

    public static Set<Expression> replace(Set<Expression> exprs,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        return exprs.stream()
                .map(expr -> replace(expr, replaceMap))
                .collect(ImmutableSet.toImmutableSet());
    }

    public static <E extends Expression> List<E> rewriteDownShortCircuit(
            List<E> exprs, Function<Expression, Expression> rewriteFunction) {
        return exprs.stream()
                .map(expr -> (E) expr.rewriteDownShortCircuit(rewriteFunction))
                .collect(ImmutableList.toImmutableList());
    }

    private static class ExpressionReplacer
            extends DefaultExpressionRewriter<Map<? extends Expression, ? extends Expression>> {
        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        private ExpressionReplacer() {
        }

        @Override
        public Expression visit(Expression expr, Map<? extends Expression, ? extends Expression> replaceMap) {
            if (replaceMap.containsKey(expr)) {
                return replaceMap.get(expr);
            }
            return super.visit(expr, replaceMap);
        }
    }

    /**
     * merge arguments into an expression array
     *
     * @param arguments instance of Expression or Expression Array
     * @return Expression Array
     */
    public static List<Expression> mergeArguments(Object... arguments) {
        Builder<Expression> builder = ImmutableList.builder();
        for (Object argument : arguments) {
            if (argument instanceof Expression[]) {
                builder.addAll(Arrays.asList((Expression[]) argument));
            } else {
                builder.add((Expression) argument);
            }
        }
        return builder.build();
    }

    public static boolean isAllLiteral(List<Expression> children) {
        return children.stream().allMatch(c -> c instanceof Literal);
    }

    public static boolean matchNumericType(List<Expression> children) {
        return children.stream().allMatch(c -> c.getDataType().isNumericType());
    }

    public static boolean hasNullLiteral(List<Expression> children) {
        return children.stream().anyMatch(c -> c instanceof NullLiteral);
    }

    public static boolean hasOnlyMetricType(List<Expression> children) {
        return children.stream().anyMatch(c -> c.getDataType().isOnlyMetricType());
    }

    public static boolean isAllNullLiteral(List<Expression> children) {
        return children.stream().allMatch(c -> c instanceof NullLiteral);
    }

    /**
     * infer notNulls slot from predicate
     */
    public static Set<Slot> inferNotNullSlots(Set<Expression> predicates, CascadesContext cascadesContext) {
        Set<Slot> notNullSlots = Sets.newHashSet();
        for (Expression predicate : predicates) {
            for (Slot slot : predicate.getInputSlots()) {
                Map<Expression, Expression> replaceMap = new HashMap<>();
                Literal nullLiteral = new NullLiteral(slot.getDataType());
                replaceMap.put(slot, nullLiteral);
                Expression evalExpr = FoldConstantRule.INSTANCE.rewrite(
                        ExpressionUtils.replace(predicate, replaceMap),
                        new ExpressionRewriteContext(cascadesContext));
                if (evalExpr.isNullLiteral() || BooleanLiteral.FALSE.equals(evalExpr)) {
                    notNullSlots.add(slot);
                }
            }
        }
        return notNullSlots;
    }

    /**
     * infer notNulls slot from predicate
     */
    public static Set<Expression> inferNotNull(Set<Expression> predicates, CascadesContext cascadesContext) {
        return inferNotNullSlots(predicates, cascadesContext).stream()
                .map(slot -> {
                    Not isNotNull = new Not(new IsNull(slot));
                    isNotNull.isGeneratedIsNotNull = true;
                    return isNotNull;
                }).collect(Collectors.toSet());
    }

    /**
     * infer notNulls slot from predicate but these slots must be in the given slots.
     */
    public static Set<Expression> inferNotNull(Set<Expression> predicates, Set<Slot> slots,
            CascadesContext cascadesContext) {
        return inferNotNullSlots(predicates, cascadesContext).stream()
                .filter(slots::contains)
                .map(slot -> {
                    Not isNotNull = new Not(new IsNull(slot));
                    isNotNull.isGeneratedIsNotNull = true;
                    return isNotNull;
                }).collect(Collectors.toSet());
    }

    public static <E extends Expression> List<E> flatExpressions(List<List<E>> expressions) {
        return expressions.stream()
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList());
    }

    public static boolean anyMatch(List<? extends Expression> expressions, Predicate<TreeNode<Expression>> predicate) {
        return expressions.stream()
                .anyMatch(expr -> expr.anyMatch(predicate));
    }

    public static boolean noneMatch(List<? extends Expression> expressions, Predicate<TreeNode<Expression>> predicate) {
        return expressions.stream()
                .noneMatch(expr -> expr.anyMatch(predicate));
    }

    public static boolean containsType(List<? extends Expression> expressions, Class type) {
        return anyMatch(expressions, type::isInstance);
    }

    public static <E> Set<E> collect(List<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        return expressions.stream()
                .flatMap(expr -> expr.<Set<E>>collect(predicate).stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    public static <E> Set<E> mutableCollect(List<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        return expressions.stream()
                .flatMap(expr -> expr.<Set<E>>collect(predicate).stream())
                .collect(Collectors.toSet());
    }

    public static <E> List<E> collectAll(List<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        return expressions.stream()
                .flatMap(expr -> expr.<Set<E>>collect(predicate).stream())
                .collect(ImmutableList.toImmutableList());
    }

    public static List<List<Expression>> rollupToGroupingSets(List<Expression> rollupExpressions) {
        List<List<Expression>> groupingSets = Lists.newArrayList();
        for (int end = rollupExpressions.size(); end >= 0; --end) {
            groupingSets.add(rollupExpressions.subList(0, end));
        }
        return groupingSets;
    }

    /**
     * check and maybe commute for predications except not pred.
     */
    public static Optional<Expression> checkAndMaybeCommute(Expression expression) {
        if (expression instanceof Not) {
            return Optional.empty();
        }
        if (expression instanceof InPredicate) {
            InPredicate predicate = ((InPredicate) expression);
            if (!predicate.getCompareExpr().isSlot()) {
                return Optional.empty();
            }
            return Optional.ofNullable(predicate.getOptions().stream()
                    .allMatch(Expression::isLiteral) ? expression : null);
        } else if (expression instanceof ComparisonPredicate) {
            ComparisonPredicate predicate = ((ComparisonPredicate) expression);
            if (predicate.left() instanceof Literal) {
                predicate = predicate.commute();
            }
            return Optional.ofNullable(predicate.left().isSlot() && predicate.right().isLiteral() ? predicate : null);
        } else if (expression instanceof IsNull) {
            return Optional.ofNullable(((IsNull) expression).child().isSlot() ? expression : null);
        }
        return Optional.empty();
    }

    public static List<List<Expression>> cubeToGroupingSets(List<Expression> cubeExpressions) {
        List<List<Expression>> groupingSets = Lists.newArrayList();
        cubeToGroupingSets(cubeExpressions, 0, Lists.newArrayList(), groupingSets);
        return groupingSets;
    }

    private static void cubeToGroupingSets(List<Expression> cubeExpressions, int activeIndex,
            List<Expression> currentGroupingSet, List<List<Expression>> groupingSets) {
        if (activeIndex == cubeExpressions.size()) {
            groupingSets.add(currentGroupingSet);
            return;
        }

        // use current expression
        List<Expression> newCurrentGroupingSet = Lists.newArrayList(currentGroupingSet);
        newCurrentGroupingSet.add(cubeExpressions.get(activeIndex));
        cubeToGroupingSets(cubeExpressions, activeIndex + 1, newCurrentGroupingSet, groupingSets);

        // skip current expression
        cubeToGroupingSets(cubeExpressions, activeIndex + 1, currentGroupingSet, groupingSets);
    }

    /**
     * Get input slot set from list of expressions.
     */
    public static Set<Slot> getInputSlotSet(Collection<? extends Expression> exprs) {
        return exprs.stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    public static boolean checkTypeSkipCast(Expression expression, Class<? extends Expression> cls) {
        while (expression instanceof Cast) {
            expression = ((Cast) expression).child();
        }
        return cls.isInstance(expression);
    }

    public static Expression getExpressionCoveredByCast(Expression expression) {
        while (expression instanceof Cast) {
            expression = ((Cast) expression).child();
        }
        return expression;
    }

    /**
     * To check whether a slot is constant after passing through a filter
     */
    public static boolean checkSlotConstant(Slot slot, Set<Expression> predicates) {
        return predicates.stream().anyMatch(predicate -> {
                    if (predicate instanceof EqualTo) {
                        EqualTo equalTo = (EqualTo) predicate;
                        return (equalTo.left() instanceof Literal && equalTo.right().equals(slot))
                                || (equalTo.right() instanceof Literal && equalTo.left().equals(slot));
                    }
                    return false;
                }
        );
    }
}
