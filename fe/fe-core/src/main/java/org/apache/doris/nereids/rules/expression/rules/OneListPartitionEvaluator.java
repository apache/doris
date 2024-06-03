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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** OneListPartitionInputs */
public class OneListPartitionEvaluator
        extends DefaultExpressionRewriter<Map<Slot, PartitionSlotInput>> implements OnePartitionEvaluator {
    private final long partitionId;
    private final List<Slot> partitionSlots;
    private final ListPartitionItem partitionItem;
    private final ExpressionRewriteContext expressionRewriteContext;

    public OneListPartitionEvaluator(long partitionId, List<Slot> partitionSlots,
            ListPartitionItem partitionItem, CascadesContext cascadesContext) {
        this.partitionId = partitionId;
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
        this.expressionRewriteContext = new ExpressionRewriteContext(
                Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null"));
    }

    @Override
    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        return partitionItem.getItems().stream()
            .map(keys -> {
                List<Literal> literals = keys.getKeys()
                        .stream()
                        .map(literal -> Literal.fromLegacyLiteral(literal, literal.getType()))
                        .collect(ImmutableList.toImmutableList());

                return IntStream.range(0, partitionSlots.size())
                        .mapToObj(index -> {
                            Slot partitionSlot = partitionSlots.get(index);
                            // partitionSlot will be replaced to this literal
                            Literal literal = literals.get(index);
                            // list partition don't need to compute the slot's range,
                            // so we pass through an empty map
                            return Pair.of(partitionSlot, new PartitionSlotInput(literal, ImmutableMap.of()));
                        }).collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
            }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression visit(Expression expr, Map<Slot, PartitionSlotInput> context) {
        expr = super.visit(expr, context);
        if (!(expr instanceof Literal)) {
            // just forward to fold constant rule
            return FoldConstantRuleOnFE.evaluate(expr, expressionRewriteContext);
        }
        return expr;
    }

    @Override
    public Expression visitSlot(Slot slot, Map<Slot, PartitionSlotInput> context) {
        // replace partition slot to literal
        PartitionSlotInput partitionSlotInput = context.get(slot);
        return partitionSlotInput == null ? slot : partitionSlotInput.result;
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        return expression.accept(this, currentInputs);
    }

    @Override
    public boolean isDefaultPartition() {
        return partitionItem.isDefaultPartition();
    }
}
