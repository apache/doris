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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rewrite.rules.PartitionRangeExpander.PartitionSlotType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/** OneRangePartitionEvaluator */
public class OneRangePartitionEvaluator implements OnePartitionEvaluator {
    private List<Slot> partitionSlots;
    private RangePartitionItem partitionItem;
    private List<PartitionSlotType> partitionSlotTypes;
    private List<Literal> lowers;
    private List<Literal> uppers;
    private List<List<Expression>> inputs;

    /** OneRangePartitionEvaluator */
    public OneRangePartitionEvaluator(List<Slot> partitionSlots, RangePartitionItem partitionItem) {
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");

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
    public List<Map<Slot, EvaluatePartitionContext>> getOnePartitionInputs() {
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
                    return Pair.of(partitionSlot, new EvaluatePartitionContext(expression, slotToRange));
                }).collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value))
            )).collect(ImmutableList.toImmutableList());
    }

    @Override
    public EvaluatePartitionContext processContext(Expression originResult,
            List<EvaluatePartitionContext> children, Map<Slot, EvaluatePartitionContext> currentInputs) {
        Map<Slot, ColumnRange> currentInput = currentInputs.entrySet().iterator().next().getValue().columnRanges;

        EvaluatePartitionContext resultContext;
        if (originResult instanceof Slot) {
            // replace slot to literal input
            EvaluatePartitionContext evaluatePartitionContext = currentInputs.get(originResult);
            resultContext = evaluatePartitionContext == null
                    ? new EvaluatePartitionContext(originResult, currentInput)
                    : evaluatePartitionContext;
        } else if (originResult instanceof And) {
            resultContext = intersect(originResult, children.get(0), children.get(1));
        } else if (originResult instanceof Or) {
            resultContext = union(originResult, children.get(0), children.get(1));
        } else if (originResult instanceof Not) {
            resultContext = not(originResult, children.get(0));
        } else {
            resultContext = new EvaluatePartitionContext(originResult, currentInput);
        }

        return evaluate(resultContext);
    }

    private EvaluatePartitionContext evaluate(EvaluatePartitionContext context) {
        if (context.result instanceof Literal) {
            return context;
        }

        // prune the range type: if partition range mismatch, return false
        if (context.columnRanges.values().stream().anyMatch(ColumnRange::isEmptyRange)) {
            return new EvaluatePartitionContext(BooleanLiteral.FALSE, context.columnRanges);
        }

        // shrink range and prune the other type: if previous column is literal and equals to the bound
        EvaluatePartitionContext resultContext = determinateRangeOfOtherType(context, lowers, true);
        return determinateRangeOfOtherType(resultContext, uppers, false);
    }

    private EvaluatePartitionContext determinateRangeOfOtherType(
            EvaluatePartitionContext context, List<Literal> partitionBound, boolean isLowerBound) {
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
                        qualifiedRange = ColumnRange.lessThen(partitionBound.get(i));
                    }
                    break;
                default:
                    throw new AnalysisException("Unknown partition slot type: " + partitionSlotType);
            }
        }

        if (qualifiedSlot != null) {
            ColumnRange origin = context.columnRanges.get(qualifiedSlot);
            ColumnRange newRange = origin.intersect(qualifiedRange);

            Map<Slot, ColumnRange> newRanges = ImmutableMap.<Slot,ColumnRange>builder()
                    .putAll(context.columnRanges)
                    .put(qualifiedSlot, newRange)
                    .build();

            if (newRange.isEmptyRange()) {
                return new EvaluatePartitionContext(BooleanLiteral.FALSE, newRanges);
            } else {
                return new EvaluatePartitionContext(context.result, newRanges);
            }
        }
        return context;
    }

    private EvaluatePartitionContext intersect(Expression originResult,
            EvaluatePartitionContext left, EvaluatePartitionContext right) {
        return mergeRanges(originResult, left, right, (leftRange, rightRange) ->
            leftRange.intersect(rightRange)
        );
    }

    private EvaluatePartitionContext union(Expression originResult,
            EvaluatePartitionContext left, EvaluatePartitionContext right) {
        return mergeRanges(originResult, left, right, (leftRange, rightRange) ->
            leftRange.union(rightRange)
        );
    }

    private EvaluatePartitionContext not(Expression originResult, EvaluatePartitionContext child) {
        Map<Slot, ColumnRange> newRanges = child.columnRanges.entrySet()
                .stream()
                .map(slotToRange -> Pair.of(slotToRange.getKey(), slotToRange.getValue().complete()))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
        return new EvaluatePartitionContext(originResult, newRanges);
    }

    private EvaluatePartitionContext mergeRanges(
            Expression originResult, EvaluatePartitionContext left, EvaluatePartitionContext right,
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
        return new EvaluatePartitionContext(originResult, mergedRange);
    }

    private List<Literal> toNereidsLiteral(List<LiteralExpr> legacyLiterals) {
        return legacyLiterals.stream()
                .map(Literal::fromLegacyLiteral)
                .collect(ImmutableList.toImmutableList());
    }
}
