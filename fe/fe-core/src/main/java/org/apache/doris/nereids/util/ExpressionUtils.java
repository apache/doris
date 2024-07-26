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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.MaterializedViewException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
        Set<Expression> exprSet = Sets.newLinkedHashSet();
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

    public static Optional<Pair<Slot, Slot>> extractEqualSlot(Expression expr) {
        if (expr instanceof EqualTo && expr.child(0).isSlot() && expr.child(1).isSlot()) {
            return Optional.of(Pair.of((Slot) expr.child(0), (Slot) expr.child(1)));
        }
        return Optional.empty();
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
        return combineAsLeftDeepTree(And.class, expressions);
    }

    public static Expression and(Expression... expressions) {
        return combineAsLeftDeepTree(And.class, Lists.newArrayList(expressions));
    }

    public static Optional<Expression> optionalOr(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ExpressionUtils.or(expressions));
        }
    }

    public static Expression or(Expression... expressions) {
        return combineAsLeftDeepTree(Or.class, Lists.newArrayList(expressions));
    }

    public static Expression or(Collection<Expression> expressions) {
        return combineAsLeftDeepTree(Or.class, expressions);
    }

    /**
     * Use AND/OR to combine expressions together.
     */
    public static Expression combineAsLeftDeepTree(
            Class<? extends Expression> type, Collection<Expression> expressions) {
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

        if (distinctExpressions.isEmpty()) {
            return BooleanLiteral.of(type == And.class);
        }
        Expression result = null;
        for (Expression expr : distinctExpressions) {
            if (result == null) {
                result = expr;
            } else if (type == And.class) {
                result = new And(result, expr);
            } else {
                result = new Or(result, expr);
            }
        }
        return result;
    }

    public static Expression shuttleExpressionWithLineage(Expression expression, Plan plan, BitSet tableBitSet) {
        return shuttleExpressionWithLineage(Lists.newArrayList(expression),
                plan, ImmutableSet.of(), ImmutableSet.of(), tableBitSet).get(0);
    }

    public static List<? extends Expression> shuttleExpressionWithLineage(List<? extends Expression> expressions,
            Plan plan, BitSet tableBitSet) {
        return shuttleExpressionWithLineage(expressions, plan, ImmutableSet.of(), ImmutableSet.of(), tableBitSet);
    }

    /**
     * Replace the slot in expressions with the lineage identifier from specifiedbaseTable sets or target table types
     * example as following:
     * select a + 10 as a1, d from (
     * select b - 5 as a, d from table
     * );
     * op expression before is: a + 10 as a1, d. after is: b - 5 + 10, d
     * todo to get from plan struct info
     */
    public static List<? extends Expression> shuttleExpressionWithLineage(List<? extends Expression> expressions,
            Plan plan,
            Set<TableType> targetTypes,
            Set<String> tableIdentifiers,
            BitSet tableBitSet) {
        if (expressions.isEmpty()) {
            return ImmutableList.of();
        }
        ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                new ExpressionLineageReplacer.ExpressionReplaceContext(
                        expressions.stream().map(Expression.class::cast).collect(Collectors.toList()),
                        targetTypes,
                        tableIdentifiers,
                        tableBitSet);

        plan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
        // Replace expressions by expression map
        List<Expression> replacedExpressions = replaceContext.getReplacedExpressions();
        if (expressions.size() != replacedExpressions.size()) {
            throw new NereidsException("shuttle expression fail",
                    new MaterializedViewException("shuttle expression fail"));
        }
        return replacedExpressions;
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
     * Generate replaceMap Slot -> Expression from NamedExpression[Expression as name]
     */
    public static Map<Slot, Expression> generateReplaceMap(List<NamedExpression> namedExpressions) {
        Map<Slot, Expression> replaceMap = Maps.newLinkedHashMapWithExpectedSize(namedExpressions.size());
        for (NamedExpression namedExpression : namedExpressions) {
            if (namedExpression instanceof Alias) {
                // Avoid cast to alias, retrieving the first child expression.
                Slot slot = namedExpression.toSlot();
                replaceMap.putIfAbsent(slot, namedExpression.child(0));
            }
        }
        return replaceMap;
    }

    /**
     * replace NameExpression.
     */
    public static NamedExpression replaceNameExpression(NamedExpression expr,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        Expression newExpr = replace(expr, replaceMap);
        if (newExpr instanceof NamedExpression) {
            return (NamedExpression) newExpr;
        } else {
            return new Alias(expr.getExprId(), newExpr, expr.getName());
        }
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
        return expr.rewriteDownShortCircuit(e -> {
            Expression replacedExpr = replaceMap.get(e);
            return replacedExpr == null ? e : replacedExpr;
        });
    }

    public static List<Expression> replace(List<Expression> exprs,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        ImmutableList.Builder<Expression> result = ImmutableList.builderWithExpectedSize(exprs.size());
        for (Expression expr : exprs) {
            result.add(replace(expr, replaceMap));
        }
        return result.build();
    }

    public static Set<Expression> replace(Set<Expression> exprs,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        ImmutableSet.Builder<Expression> result = ImmutableSet.builderWithExpectedSize(exprs.size());
        for (Expression expr : exprs) {
            result.add(replace(expr, replaceMap));
        }
        return result.build();
    }

    /**
     * Replace expression node in the expression tree by `replaceMap` in top-down manner.
     */
    public static List<NamedExpression> replaceNamedExpressions(List<NamedExpression> namedExpressions,
            Map<? extends Expression, ? extends Expression> replaceMap) {
        Builder<NamedExpression> replaceExprs = ImmutableList.builderWithExpectedSize(namedExpressions.size());
        for (NamedExpression namedExpression : namedExpressions) {
            NamedExpression newExpr = replaceNameExpression(namedExpression, replaceMap);
            if (newExpr.getExprId().equals(namedExpression.getExprId())) {
                replaceExprs.add(newExpr);
            } else {
                replaceExprs.add(new Alias(namedExpression.getExprId(), newExpr, namedExpression.getName()));
            }
        }
        return replaceExprs.build();
    }

    public static <E extends Expression> List<E> rewriteDownShortCircuit(
            Collection<E> exprs, Function<Expression, Expression> rewriteFunction) {
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

    /** isAllLiteral */
    public static boolean isAllLiteral(List<Expression> children) {
        for (Expression child : children) {
            if (!(child instanceof Literal)) {
                return false;
            }
        }
        return true;
    }

    /** matchNumericType */
    public static boolean matchNumericType(List<Expression> children) {
        for (Expression child : children) {
            if (!child.getDataType().isNumericType()) {
                return false;
            }
        }
        return true;
    }

    /** matchDateLikeType */
    public static boolean matchDateLikeType(List<Expression> children) {
        for (Expression child : children) {
            if (!child.getDataType().isDateLikeType()) {
                return false;
            }
        }
        return true;
    }

    /** hasNullLiteral */
    public static boolean hasNullLiteral(List<Expression> children) {
        for (Expression child : children) {
            if (child instanceof NullLiteral) {
                return true;
            }
        }
        return false;
    }

    /** hasOnlyMetricType */
    public static boolean hasOnlyMetricType(List<Expression> children) {
        for (Expression child : children) {
            if (child.getDataType().isOnlyMetricType()) {
                return true;
            }
        }
        return false;
    }

    /**
     * canInferNotNullForMarkSlot
     */
    public static boolean canInferNotNullForMarkSlot(Expression predicate, ExpressionRewriteContext ctx) {
        /*
         * assume predicate is from LogicalFilter
         * the idea is replacing each mark join slot with null and false literal then run FoldConstant rule
         * if the evaluate result are:
         * 1. all true
         * 2. all null and false (in logicalFilter, we discard both null and false values)
         * the mark slot can be non-nullable boolean
         * and in semi join, we can safely change the mark conjunct to hash conjunct
         */
        ImmutableList<Literal> literals =
                ImmutableList.of(new NullLiteral(BooleanType.INSTANCE), BooleanLiteral.FALSE);
        List<MarkJoinSlotReference> markJoinSlotReferenceList =
                new ArrayList<>((predicate.collect(MarkJoinSlotReference.class::isInstance)));
        int markSlotSize = markJoinSlotReferenceList.size();
        int maxMarkSlotCount = 4;
        // if the conjunct has mark slot, and maximum 4 mark slots(for performance)
        if (markSlotSize > 0 && markSlotSize <= maxMarkSlotCount) {
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            boolean meetTrue = false;
            boolean meetNullOrFalse = false;
            /*
             * markSlotSize = 1 -> loopCount = 2  ---- 0, 1
             * markSlotSize = 2 -> loopCount = 4  ---- 00, 01, 10, 11
             * markSlotSize = 3 -> loopCount = 8  ---- 000, 001, 010, 011, 100, 101, 110, 111
             * markSlotSize = 4 -> loopCount = 16 ---- 0000, 0001, ... 1111
             */
            int loopCount = 2 << markSlotSize;
            for (int i = 0; i < loopCount; ++i) {
                replaceMap.clear();
                /*
                 * replace each mark slot with null or false
                 * literals.get(0) -> NullLiteral(BooleanType.INSTANCE)
                 * literals.get(1) -> BooleanLiteral.FALSE
                 */
                for (int j = 0; j < markSlotSize; ++j) {
                    replaceMap.put(markJoinSlotReferenceList.get(j), literals.get((i >> j) & 1));
                }
                Expression evalResult = FoldConstantRule.evaluate(
                        ExpressionUtils.replace(predicate, replaceMap),
                        ctx
                );

                if (evalResult.equals(BooleanLiteral.TRUE)) {
                    if (meetNullOrFalse) {
                        return false;
                    } else {
                        meetTrue = true;
                    }
                } else if ((isNullOrFalse(evalResult))) {
                    if (meetTrue) {
                        return false;
                    } else {
                        meetNullOrFalse = true;
                    }
                }
            }
        }
        return true;
    }

    private static boolean isNullOrFalse(Expression expression) {
        return expression.isNullLiteral() || expression.equals(BooleanLiteral.FALSE);
    }

    /**
     * infer notNulls slot from predicate
     */
    public static Set<Slot> inferNotNullSlots(Set<Expression> predicates, CascadesContext cascadesContext) {
        ImmutableSet.Builder<Slot> notNullSlots = ImmutableSet.builderWithExpectedSize(predicates.size());
        for (Expression predicate : predicates) {
            for (Slot slot : predicate.getInputSlots()) {
                Map<Expression, Expression> replaceMap = new HashMap<>();
                Literal nullLiteral = new NullLiteral(slot.getDataType());
                replaceMap.put(slot, nullLiteral);
                Expression evalExpr = FoldConstantRule.evaluate(
                        ExpressionUtils.replace(predicate, replaceMap),
                        new ExpressionRewriteContext(cascadesContext)
                );
                if (evalExpr.isNullLiteral() || BooleanLiteral.FALSE.equals(evalExpr)) {
                    notNullSlots.add(slot);
                }
            }
        }
        return notNullSlots.build();
    }

    /**
     * infer notNulls slot from predicate
     */
    public static Set<Expression> inferNotNull(Set<Expression> predicates, CascadesContext cascadesContext) {
        ImmutableSet.Builder<Expression> newPredicates = ImmutableSet.builderWithExpectedSize(predicates.size());
        for (Slot slot : inferNotNullSlots(predicates, cascadesContext)) {
            newPredicates.add(new Not(new IsNull(slot), false));
        }
        return newPredicates.build();
    }

    /**
     * infer notNulls slot from predicate but these slots must be in the given slots.
     */
    public static Set<Expression> inferNotNull(Set<Expression> predicates, Set<Slot> slots,
            CascadesContext cascadesContext) {
        ImmutableSet.Builder<Expression> newPredicates = ImmutableSet.builderWithExpectedSize(predicates.size());
        for (Slot slot : inferNotNullSlots(predicates, cascadesContext)) {
            if (slots.contains(slot)) {
                newPredicates.add(new Not(new IsNull(slot), true));
            }
        }
        return newPredicates.build();
    }

    /** flatExpressions */
    public static <E extends Expression> List<E> flatExpressions(List<List<E>> expressionLists) {
        int num = 0;
        for (List<E> expressionList : expressionLists) {
            num += expressionList.size();
        }

        ImmutableList.Builder<E> flatten = ImmutableList.builderWithExpectedSize(num);
        for (List<E> expressionList : expressionLists) {
            flatten.addAll(expressionList);
        }
        return flatten.build();
    }

    /** containsType */
    public static boolean containsType(Collection<? extends Expression> expressions, Class type) {
        for (Expression expression : expressions) {
            if (expression.anyMatch(expr -> expr.anyMatch(type::isInstance))) {
                return true;
            }
        }
        return false;
    }

    /** allMatch */
    public static boolean allMatch(
            Collection<? extends Expression> expressions, Predicate<Expression> predicate) {
        for (Expression expression : expressions) {
            if (!predicate.test(expression)) {
                return false;
            }
        }
        return true;
    }

    /** anyMatch */
    public static boolean anyMatch(
            Collection<? extends Expression> expressions, Predicate<Expression> predicate) {
        for (Expression expression : expressions) {
            if (predicate.test(expression)) {
                return true;
            }
        }
        return false;
    }

    /** deapAnyMatch */
    public static boolean deapAnyMatch(
            Collection<? extends Expression> expressions, Predicate<TreeNode<Expression>> predicate) {
        for (Expression expression : expressions) {
            if (expression.anyMatch(expr -> expr.anyMatch(predicate))) {
                return true;
            }
        }
        return false;
    }

    /** deapNoneMatch */
    public static boolean deapNoneMatch(
            Collection<? extends Expression> expressions, Predicate<TreeNode<Expression>> predicate) {
        for (Expression expression : expressions) {
            if (expression.anyMatch(expr -> expr.anyMatch(predicate))) {
                return false;
            }
        }
        return true;
    }

    public static <E> Set<E> collect(Collection<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        ImmutableSet.Builder<E> set = ImmutableSet.builder();
        for (Expression expr : expressions) {
            set.addAll(expr.collectToList(predicate));
        }
        return set.build();
    }

    public static <E> List<E> collectToList(Collection<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        ImmutableList.Builder<E> list = ImmutableList.builder();
        for (Expression expr : expressions) {
            list.addAll(expr.collectToList(predicate));
        }
        return list.build();
    }

    /**
     * extract uniform slot for the given predicate, such as a = 1 and b = 2
     */
    public static ImmutableSet<Slot> extractUniformSlot(Expression expression) {
        ImmutableSet.Builder<Slot> builder = new ImmutableSet.Builder<>();
        if (expression instanceof And) {
            builder.addAll(extractUniformSlot(expression.child(0)));
            builder.addAll(extractUniformSlot(expression.child(1)));
        }
        if (expression instanceof EqualTo) {
            if (isInjective(expression.child(0)) && expression.child(1).isConstant()) {
                builder.add((Slot) expression.child(0));
            }
        }
        return builder.build();
    }

    // TODO: Add more injective functions
    public static boolean isInjective(Expression expression) {
        return expression instanceof Slot;
    }

    // if the input is unique,  the output of agg is unique, too
    public static boolean isInjectiveAgg(Expression agg) {
        return agg instanceof Sum || agg instanceof Avg || agg instanceof Max || agg instanceof Min;
    }

    public static <E> Set<E> mutableCollect(List<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        Set<E> set = new HashSet<>();
        for (Expression expr : expressions) {
            set.addAll(expr.collect(predicate));
        }
        return set;
    }

    /** collectAll */
    public static <E> List<E> collectAll(Collection<? extends Expression> expressions,
            Predicate<TreeNode<Expression>> predicate) {
        switch (expressions.size()) {
            case 0: return ImmutableList.of();
            default: {
                ImmutableList.Builder<E> result = ImmutableList.builder();
                for (Expression expr : expressions) {
                    result.addAll((Set) expr.collect(predicate));
                }
                return result.build();
            }
        }
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
        Set<Slot> set = new HashSet<>();
        for (Expression expr : exprs) {
            set.addAll(expr.getInputSlots());
        }
        return set;
    }

    public static Expression getExpressionCoveredByCast(Expression expression) {
        while (expression instanceof Cast) {
            expression = ((Cast) expression).child();
        }
        return expression;
    }

    /**
     * the expressions can be used as runtime filter targets
     */
    public static Expression getSingleNumericSlotOrExpressionCoveredByCast(Expression expression) {
        if (expression.getInputSlots().size() == 1) {
            Slot slot = expression.getInputSlots().iterator().next();
            if (slot.getDataType() instanceof NumericType) {
                return expression.getInputSlots().iterator().next();
            }
        }
        // for other datatype, only support cast.
        // example: T1 join T2 on subStr(T1.a, 1,4) = subStr(T2.a, 1,4)
        // the cost of subStr is too high, and hence we do not generate RF subStr(T2.a, 1,4)->subStr(T1.a, 1,4)
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

    /**
     * Check the expression is inferred or not, if inferred return true, nor return false
     */
    public static boolean isInferred(Expression expression) {
        return expression.accept(new DefaultExpressionVisitor<Boolean, Void>() {

            @Override
            public Boolean visit(Expression expr, Void context) {
                boolean inferred = expr.isInferred();
                if (expr.isInferred() || expr.children().isEmpty()) {
                    return inferred;
                }
                inferred = true;
                for (Expression child : expr.children()) {
                    inferred = inferred && child.accept(this, context);
                }
                return inferred;
            }
        }, null);
    }

    /** distinctSlotByName */
    public static List<Slot> distinctSlotByName(List<Slot> slots) {
        Set<String> existSlotNames = new HashSet<>(slots.size() * 2);
        Builder<Slot> distinctSlots = ImmutableList.builderWithExpectedSize(slots.size());
        for (Slot slot : slots) {
            String name = slot.getName();
            if (existSlotNames.add(name)) {
                distinctSlots.add(slot);
            }
        }
        return distinctSlots.build();
    }

    /** containsWindowExpression */
    public static boolean containsWindowExpression(List<NamedExpression> expressions) {
        for (NamedExpression expression : expressions) {
            if (expression.anyMatch(WindowExpression.class::isInstance)) {
                return true;
            }
        }
        return false;
    }

    /** filter */
    public static <E extends Expression> List<E> filter(List<? extends Expression> expressions, Class<E> clazz) {
        ImmutableList.Builder<E> result = ImmutableList.builderWithExpectedSize(expressions.size());
        for (Expression expression : expressions) {
            if (clazz.isInstance(expression)) {
                result.add((E) expression);
            }
        }
        return result.build();
    }
}
