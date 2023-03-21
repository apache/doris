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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.rules.OneRangePartitionEvaluator.EvaluateRangeInput;
import org.apache.doris.nereids.rules.expression.rewrite.rules.OneRangePartitionEvaluator.EvaluateRangeResult;
import org.apache.doris.nereids.rules.expression.rewrite.rules.PartitionRangeExpander.PartitionSlotType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/** OneRangePartitionEvaluator */
public class OneRangePartitionEvaluator
        extends ExpressionVisitor<EvaluateRangeResult, EvaluateRangeInput>
        implements OnePartitionEvaluator {
    private final long partitionId;
    private List<Slot> partitionSlots;
    private RangePartitionItem partitionItem;
    private ExpressionRewriteContext expressionRewriteContext;
    private List<PartitionSlotType> partitionSlotTypes;
    private List<Literal> lowers;
    private List<Literal> uppers;
    private List<List<Expression>> inputs;

    /** OneRangePartitionEvaluator */
    public OneRangePartitionEvaluator(long partitionId, List<Slot> partitionSlots,
            RangePartitionItem partitionItem, CascadesContext cascadesContext) {
        this.partitionId = partitionId;
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
        this.expressionRewriteContext = new ExpressionRewriteContext(
                Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null"));

        Range<PartitionKey> range = partitionItem.getItems();
        this.lowers = toNereidsLiteral(range.lowerEndpoint().getKeys());
        this.uppers = toNereidsLiteral(range.upperEndpoint().getKeys());

        PartitionRangeExpander expander = new PartitionRangeExpander();
        this.partitionSlotTypes = expander.computePartitionSlotTypes(lowers, uppers);
        List<List<Expression>> expandInputs = expander.tryExpandRange(
                partitionSlots, lowers, uppers, partitionSlotTypes);
        // after expand range, we will get 2 dimension list like list:
        // part_col1: [1], part_col2:[4, 5, 6], we should combine it to
        // [1, 4], [1, 5], [1, 6] as inputs
        this.inputs = Utils.allCombinations(expandInputs);
    }

    @Override
    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        return inputs.stream()
                .map(input -> OnePartitionEvaluator.fillSlotRangesToInputs(IntStream.range(0, partitionSlots.size())
                        .mapToObj(index -> {
                            Slot partitionSlot = partitionSlots.get(index);
                            // partitionSlot will be replaced to this expression
                            Expression expression = input.get(index);
                            ColumnRange slotRange = null;
                            PartitionSlotType partitionSlotType = partitionSlotTypes.get(index);
                            if (expression instanceof Literal) {
                                // const or expanded range
                                slotRange = ColumnRange.singleton((Literal) expression);
                            } else {
                                // un expanded range
                                switch (partitionSlotType) {
                                    case RANGE:
                                        boolean isLastPartitionColumn = index + 1 == partitionSlots.size();
                                        slotRange = ColumnRange.range(lowers.get(index), BoundType.CLOSED,
                                                uppers.get(index), isLastPartitionColumn ? BoundType.OPEN : BoundType.CLOSED);
                                        break;
                                    case OTHER:
                                        // unknown range at the beginning
                                        slotRange = ColumnRange.all();
                                        break;
                                    default:
                                        throw new AnalysisException("Unknown partition slot type: " + partitionSlotType);
                                }
                            }
                            ImmutableMap<Slot, ColumnRange> slotToRange = ImmutableMap.of(partitionSlot, slotRange);
                            return Pair.of(partitionSlot, new PartitionSlotInput(expression, slotToRange));
                        }).collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value))
                )).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        Map<Slot, ColumnRange> defaultColumnRanges = currentInputs.values().iterator().next().columnRanges;
        EvaluateRangeResult result = expression.accept(
                this, new EvaluateRangeInput(defaultColumnRanges, currentInputs));
        return result.result;
    }

    @Override
    public EvaluateRangeResult visit(Expression expr, EvaluateRangeInput context) {
        EvaluateRangeResult result = evaluateChildrenThenThis(expr, context);

        // NOTE: if children exist empty range return false
        //       !!! this is different from `returnFalseIfExistEmptyRange` !!!
        expr = result.result;
        if (expr.getDataType() instanceof BooleanType && !(expr instanceof Literal)
                && result.childrenResult.stream().anyMatch(childResult ->
                childResult.columnRanges.values().stream().anyMatch(ColumnRange::isEmptyRange))) {
            return new EvaluateRangeResult(BooleanLiteral.FALSE, result.columnRanges, result.childrenResult);
        }
        return result;
    }

    @Override
    public EvaluateRangeResult visitSlot(Slot slot, EvaluateRangeInput context) {
        // try to replace partition slot to literal
        PartitionSlotInput slotResult = context.slotToInput.get(slot);
        return slotResult == null
                ? new EvaluateRangeResult(slot, context.defaultColumnRanges, ImmutableList.of())
                : new EvaluateRangeResult(slotResult.result, slotResult.columnRanges, ImmutableList.of());
    }

    @Override
    public EvaluateRangeResult visitGreaterThan(GreaterThan greaterThan, EvaluateRangeInput context) {
        EvaluateRangeResult result = evaluateChildrenThenThis(greaterThan, context);
        if (!(result.result instanceof GreaterThan)) {
            return result;
        }
        greaterThan = (GreaterThan) result.result;
        if (greaterThan.left() instanceof Slot && greaterThan.right() instanceof Literal) {
            Slot slot = (Slot) greaterThan.left();
            if (context.defaultColumnRanges.containsKey(slot)) {
                Map<Slot, ColumnRange> leftColumnRanges = result.childrenResult.get(0).columnRanges;
                ColumnRange greaterThenRange = ColumnRange.greaterThan((Literal) greaterThan.right());
                result = intersectSlotRange(result, leftColumnRanges, slot, greaterThenRange);
            }
        } else if (greaterThan.left() instanceof Literal && greaterThan.right() instanceof Slot) {
            Slot slot = (Slot) greaterThan.right();
            if (context.defaultColumnRanges.containsKey(slot)) {
                Map<Slot, ColumnRange> rightColumnRanges = result.childrenResult.get(1).columnRanges;
                ColumnRange lessThenRange = ColumnRange.lessThen((Literal) greaterThan.left());
                result = intersectSlotRange(result, rightColumnRanges, slot, lessThenRange);
            }
        }
        return result;
    }

    @Override
    public EvaluateRangeResult visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, EvaluateRangeInput context) {
        return super.visitGreaterThanEqual(greaterThanEqual, context);
    }

    @Override
    public EvaluateRangeResult visitLessThan(LessThan lessThan, EvaluateRangeInput context) {
        return super.visitLessThan(lessThan, context);
    }

    @Override
    public EvaluateRangeResult visitLessThanEqual(LessThanEqual lessThanEqual, EvaluateRangeInput context) {
        return super.visitLessThanEqual(lessThanEqual, context);
    }

    @Override
    public EvaluateRangeResult visitEqualTo(EqualTo equalTo, EvaluateRangeInput context) {
        return super.visitEqualTo(equalTo, context);
    }

    @Override
    public EvaluateRangeResult visitBetween(Between between, EvaluateRangeInput context) {
        return super.visitBetween(between, context);
    }

    @Override
    public EvaluateRangeResult visitInPredicate(InPredicate inPredicate, EvaluateRangeInput context) {
        return super.visitInPredicate(inPredicate, context);
    }

    @Override
    public EvaluateRangeResult visitAnd(And and, EvaluateRangeInput context) {
        EvaluateRangeResult result = evaluateChildrenThenThis(and, context);

        result = mergeRanges(result.result, result.childrenResult.get(0), result.childrenResult.get(1),
                (leftRange, rightRange) -> leftRange.intersect(rightRange));

        result = returnFalseIfExistEmptyRange(result);
        if (result.result.equals(BooleanLiteral.FALSE)) {
            return result;
        }

        // shrink range and prune the other type: if previous column is literal and equals to the bound
        result = determinateRangeOfOtherType(result, lowers, true);
        result = determinateRangeOfOtherType(result, uppers, false);
        return result;
    }

    @Override
    public EvaluateRangeResult visitOr(Or or, EvaluateRangeInput context) {
        EvaluateRangeResult result = evaluateChildrenThenThis(or, context);

        result = mergeRanges(result.result, result.childrenResult.get(0), result.childrenResult.get(1),
                (leftRange, rightRange) -> leftRange.union(rightRange));
        return returnFalseIfExistEmptyRange(result);
    }

    @Override
    public EvaluateRangeResult visitNot(Not not, EvaluateRangeInput context) {
        EvaluateRangeResult result = evaluateChildrenThenThis(not, context);

        Map<Slot, ColumnRange> newRanges = result.childrenResult.get(0).columnRanges.entrySet()
                .stream()
                .map(slotToRange -> Pair.of(slotToRange.getKey(), slotToRange.getValue().complete()))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
        result = new EvaluateRangeResult(result.result, newRanges, result.childrenResult);
        return returnFalseIfExistEmptyRange(result);
    }

    private EvaluateRangeResult evaluateChildrenThenThis(Expression expr, EvaluateRangeInput context) {
        // evaluate children
        List<Expression> newChildren = new ArrayList<>();
        List<EvaluateRangeResult> childrenResults = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Expression child : expr.children()) {
            EvaluateRangeResult childResult = child.accept(this, context);
            if (childResult.result != child) {
                hasNewChildren = true;
            }
            childrenResults.add(childResult);
            newChildren.add(childResult.result);
        }
        if (hasNewChildren) {
            expr = expr.withChildren(newChildren);
        }

        // evaluate this
        expr = FoldConstantRuleOnFE.INSTANCE.visit(expr, expressionRewriteContext);
        return new EvaluateRangeResult(expr, context.defaultColumnRanges, childrenResults);
    }

    private EvaluateRangeResult returnFalseIfExistEmptyRange(EvaluateRangeResult result) {
        Expression expr = result.result;
        if (expr.getDataType() instanceof BooleanType && !(expr instanceof Literal)
                && result.columnRanges.values().stream().anyMatch(ColumnRange::isEmptyRange)) {
            return new EvaluateRangeResult(BooleanLiteral.FALSE, result.columnRanges, result.childrenResult);
        }
        return result;
    }

    private EvaluateRangeResult intersectSlotRange(EvaluateRangeResult originResult,
            Map<Slot, ColumnRange> columnRanges, Slot slot, ColumnRange otherRange) {
        ColumnRange columnRange = columnRanges.get(slot);
        ColumnRange intersect = columnRange.intersect(otherRange);

        Map<Slot, ColumnRange> newColumnRanges = ImmutableMap.<Slot, ColumnRange>builder()
                .putAll(columnRanges)
                .put(slot, intersect)
                .build();

        if (intersect.isEmptyRange()) {
            return new EvaluateRangeResult(BooleanLiteral.FALSE, newColumnRanges, originResult.childrenResult);
        } else {
            return new EvaluateRangeResult(originResult.result, newColumnRanges, originResult.childrenResult);
        }
    }

    private EvaluateRangeResult determinateRangeOfOtherType(
            EvaluateRangeResult context, List<Literal> partitionBound, boolean isLowerBound) {
        if (context.result instanceof Literal) {
            return context;
        }

        Slot qualifiedSlot = null;
        ColumnRange qualifiedRange = null;
        for (int i = 0; i < partitionSlotTypes.size(); i++) {
            PartitionSlotType partitionSlotType = partitionSlotTypes.get(i);
            Slot slot = partitionSlots.get(i);
            switch (partitionSlotType) {
                case CONST: continue;
                case RANGE:
                    ColumnRange columnRange = context.columnRanges.get(slot);
                    if (!columnRange.isSingleton()
                            || !columnRange.getLowerBound().equals(ColumnBound.singleton(partitionBound.get(i)))) {
                        return context;
                    }
                    continue;
                case OTHER:
                    columnRange = context.columnRanges.get(slot);
                    if (columnRange.isSingleton()
                            && columnRange.getLowerBound().equals(ColumnBound.singleton(partitionBound.get(i)))) {
                        continue;
                    }

                    qualifiedSlot = slot;
                    if (isLowerBound) {
                        qualifiedRange = ColumnRange.atLeast(partitionBound.get(i));
                    } else {
                        qualifiedRange = i + 1 == partitionSlots.size()
                                ? ColumnRange.lessThen(partitionBound.get(i))
                                : ColumnRange.atMost(partitionBound.get(i));
                    }
                    break;
                default:
                    throw new AnalysisException("Unknown partition slot type: " + partitionSlotType);
            }
        }

        if (qualifiedSlot != null) {
            ColumnRange origin = context.columnRanges.get(qualifiedSlot);
            ColumnRange newRange = origin.intersect(qualifiedRange);

            Map<Slot, ColumnRange> newRanges = ImmutableMap.<Slot, ColumnRange>builder()
                    .putAll(context.columnRanges)
                    .put(qualifiedSlot, newRange)
                    .build();

            if (newRange.isEmptyRange()) {
                return new EvaluateRangeResult(BooleanLiteral.FALSE, newRanges, context.childrenResult);
            } else {
                return new EvaluateRangeResult(context.result, newRanges, context.childrenResult);
            }
        }
        return context;
    }

    private EvaluateRangeResult mergeRanges(
            Expression originResult, EvaluateRangeResult left, EvaluateRangeResult right,
            BiFunction<ColumnRange, ColumnRange, ColumnRange> mergeFunction) {

        Map<Slot, ColumnRange> leftRanges = left.columnRanges;
        Map<Slot, ColumnRange> rightRanges = right.columnRanges;

        Set<Slot> slots = ImmutableSet.<Slot>builder()
                .addAll(leftRanges.keySet())
                .addAll(rightRanges.keySet())
                .build();

        Map<Slot, ColumnRange> mergedRange = slots.stream()
                .map(slot -> Pair.of(slot, mergeFunction.apply(leftRanges.get(slot), rightRanges.get(slot))))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
        return new EvaluateRangeResult(originResult, mergedRange, ImmutableList.of(left, right));
    }

    private List<Literal> toNereidsLiteral(List<LiteralExpr> legacyLiterals) {
        return legacyLiterals.stream()
                .map(Literal::fromLegacyLiteral)
                .collect(ImmutableList.toImmutableList());
    }

    /** EvaluateRangeInput */
    public static class EvaluateRangeInput {
        private Map<Slot, ColumnRange> defaultColumnRanges;
        private Map<Slot, PartitionSlotInput> slotToInput;

        public EvaluateRangeInput(Map<Slot, ColumnRange> defaultColumnRanges,
                Map<Slot, PartitionSlotInput> slotToInput) {
            this.defaultColumnRanges = defaultColumnRanges;
            this.slotToInput = slotToInput;
        }
    }

    /** EvaluateRangeResult */
    public static class EvaluateRangeResult {
        private final Expression result;
        private final Map<Slot, ColumnRange> columnRanges;
        private final List<EvaluateRangeResult> childrenResult;

        public EvaluateRangeResult(Expression result, Map<Slot, ColumnRange> columnRanges,
                List<EvaluateRangeResult> childrenResult) {
            this.result = result;
            this.columnRanges = columnRanges;
            this.childrenResult = childrenResult;
        }
    }

    private static class EvaluateChildContext {
        private Map<Slot, ColumnRange> columnRanges;

        public EvaluateChildContext(Map<Slot, ColumnRange> columnRanges) {
            this.columnRanges = columnRanges;
        }
    }
}
