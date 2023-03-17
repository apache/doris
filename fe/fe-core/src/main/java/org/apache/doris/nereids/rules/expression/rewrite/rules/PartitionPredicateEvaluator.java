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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.PartitionLiteral;
import org.apache.doris.nereids.trees.expressions.literal.PartitionLiteral.PartitionWithLiteral;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** PartitionPredicateEvaluator */
public class PartitionPredicateEvaluator extends FoldConstantRuleOnFE implements PartitionEvaluator {
    private final long partitionId;
    private PartitionItem partitionItem;
    private Map<Slot, Integer> slotToPartitionColumnIndex;
    private List<? extends Literal> partitionLiterals;

    public PartitionPredicateEvaluator(Map<Slot, Integer> slotToPartitionColumnIndex,
            List<? extends Literal> partitionLiterals, long partitionId, PartitionItem partitionItem) {
        this.slotToPartitionColumnIndex = slotToPartitionColumnIndex;
        this.partitionLiterals = partitionLiterals;
        this.partitionId = partitionId;
        this.partitionItem = partitionItem;
    }

    /** evaluate */
    public static List<Long> evaluate(
            List<Slot> partitionSlots, List<PartitionWithLiteral> partitionWithLiterals, Expression predicate,
            CascadesContext cascadesContext) {
        Map<Slot, Integer> slotToPartitionColumnIndex = IntStream.range(0, partitionSlots.size())
                .mapToObj(index -> Pair.of(partitionSlots.get(index), index))
                .collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));

        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);

        List<PartitionEvaluator> evaluators = toEvaluators(slotToPartitionColumnIndex, partitionWithLiterals);
        return evaluators.stream()
                .filter(evaluator -> {
                    Expression result = evaluator.rewrite(predicate, context);
                    return !BooleanLiteral.FALSE.equals(result);
                })
                .map(evaluator -> evaluator.getPartitionId())
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression visitSlot(Slot slot, ExpressionRewriteContext context) {
        // replace partition slot to literal as input parameters
        Integer index = slotToPartitionColumnIndex.get(slot);
        return index == null ? slot : partitionLiterals.get(index);
    }

    private static List<PartitionEvaluator> toEvaluators(
            Map<Slot, Integer> slotToPartitionColumnIndex, List<PartitionWithLiteral> partitionWithLiterals) {

        long evaluatorCount = 0;
        List<PartitionEvaluator> evaluators = Lists.newArrayList();
        for (PartitionWithLiteral partitionWithLiteral : partitionWithLiterals) {
            long partitionId = partitionWithLiteral.partitionId;
            PartitionItem partitionItem = partitionWithLiteral.partitionItem;
            boolean expended = false;
            try {
                int expandLiteralCount = getExpandLiteralCount(partitionWithLiteral.partitionLiterals);
                if (evaluatorCount + expandLiteralCount < 10000) {
                    List<PartitionEvaluator> evaluatorsInOnePartition =
                            expendLiteralsToEvaluators(slotToPartitionColumnIndex, partitionWithLiteral);
                    OnePartitionEvaluator combineEvaluator = new OnePartitionEvaluator(
                            partitionId, partitionItem, evaluatorsInOnePartition);
                    evaluators.add(combineEvaluator);
                    evaluatorCount += expandLiteralCount;
                    expended = true;
                }
            } catch (Throwable t) {
                // expend failed
            }
            if (!expended) {
                // 1. evaluate too many times, no more pruning because occupying resources of fe
                // 2. expend failed, always scan the partition for safety
                evaluatorCount++;
                evaluators.add(new AlwaysSelectPartition(partitionId, partitionItem));
            }
        }
        return evaluators;
    }

    /**
     * if one partition has multi enumerable values, try to expend it.
     *
     * for example, list partition value c1:[1, 2, 3], c2 [1, 2] will be expend to
     * [1, 1], [1, 2], [2, 1], [2, 2], [3, 1], [3, 2], each will be use the input parameter of evaluator.
     *
     * note, sometimes the range can expend too, e.g. the type of partition column c1/c2 is int
     * and the range [1, 5) means it can expand to [1, 2, 3, 4], this action can provide
     * stronger pruning capabilities.
     *
     * if can not expand the range partition, e.g. datetime type partition, we keep RangePartitionLiteral
     * as the input parameter of evaluator, it can support some simple capabilities of evaluate, e.g.
     * binary compare like this `date '2020-01-01 10:00:00' < RangePartitionLiteral`, but can not evaluate
     * if contains some complex operator like BinaryArithmetic, and finally, if return some Expressions which
     * not equals to BooleanLiteral.FALSE, we will scan this partition.
     */
    private static List<PartitionEvaluator> expendLiteralsToEvaluators(
            Map<Slot, Integer> slotToPartitionColumnIndex, PartitionWithLiteral partitionWithLiteral) throws Exception {
        List<List<Literal>> expendLiterals = Lists.newArrayList();
        for (PartitionLiteral literal : partitionWithLiteral.partitionLiterals) {
            expendLiterals.add(ImmutableList.copyOf(literal.expendLiterals()));
        }

        List<List<Literal>> allCombinations = Utils.allCombinations(expendLiterals);

        return allCombinations.stream()
                .map(partitionLiterals -> new PartitionPredicateEvaluator(slotToPartitionColumnIndex,
                        partitionLiterals, partitionWithLiteral.partitionId, partitionWithLiteral.partitionItem)
                ).collect(ImmutableList.toImmutableList());
    }

    private static int getExpandLiteralCount(List<PartitionLiteral> literals) throws Exception {
        int count = 1;
        for (PartitionLiteral literal : literals) {
            long valueCount = literal.valueCount();
            count *= (valueCount <= 0 ? 1 : valueCount);
        }
        return count;
    }

    @Override
    public PartitionItem getPartitionItem() {
        return partitionItem;
    }

    @Override
    public long getPartitionId() {
        return partitionId;
    }

    private static class AlwaysSelectPartition implements PartitionEvaluator {
        private final long partitionId;
        private final PartitionItem partitionItem;

        private AlwaysSelectPartition(long partitionId, PartitionItem partitionItem) {
            this.partitionId = partitionId;
            this.partitionItem = partitionItem;
        }

        @Override
        public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
            return BooleanLiteral.TRUE;
        }

        @Override
        public PartitionItem getPartitionItem() {
            return partitionItem;
        }

        @Override
        public long getPartitionId() {
            return partitionId;
        }
    }

    // one partition can expand to lots of combination partition literals,
    // but as long as one of evaluate return not equals to BooleanLiteral.FALSE,
    // the remaining ones do not need to be executed, so we need use OnePartitionEvaluator
    // to wrap all evaluator belong to a partition
    private static class OnePartitionEvaluator implements PartitionEvaluator {
        private final long partitionId;
        private final PartitionItem partitionItem;
        private final List<PartitionEvaluator> evaluators;

        public OnePartitionEvaluator(long partitionId, PartitionItem partitionItem,
                List<PartitionEvaluator> evaluators) {
            this.partitionId = partitionId;
            this.partitionItem = partitionItem;
            this.evaluators = evaluators;
        }

        @Override
        public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
            boolean needScan = evaluators.stream()
                    .map(evaluator -> evaluator.rewrite(expr, ctx))
                    .anyMatch(result -> !BooleanLiteral.FALSE.equals(result));
            return needScan ? BooleanLiteral.TRUE : BooleanLiteral.FALSE;
        }

        @Override
        public PartitionItem getPartitionItem() {
            return partitionItem;
        }

        @Override
        public long getPartitionId() {
            return partitionId;
        }
    }
}
