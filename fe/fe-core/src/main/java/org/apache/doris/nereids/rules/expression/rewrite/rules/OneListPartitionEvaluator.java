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

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** OneListPartitionInputs */
public class OneListPartitionEvaluator implements OnePartitionEvaluator {
    private List<Slot> partitionSlots;
    private ListPartitionItem partitionItem;

    public OneListPartitionEvaluator(List<Slot> partitionSlots, ListPartitionItem partitionItem) {
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
    }

    @Override
    public List<Map<Slot, EvaluatePartitionContext>> getOnePartitionInputs() {
        return partitionItem.getItems().stream()
            .map(keys -> {
                List<Literal> literals = keys.getKeys()
                        .stream()
                        .map(Literal::fromLegacyLiteral)
                        .collect(ImmutableList.toImmutableList());

                return IntStream.range(0, partitionSlots.size())
                        .mapToObj(index -> {
                            Slot partitionSlot = partitionSlots.get(index);
                            // partitionSlot will be replaced to this literal
                            Literal literal = literals.get(index);
                            // list partition don't need to compute the slot's range,
                            // so we pass through an empty map
                            return Pair.of(partitionSlot, new EvaluatePartitionContext(literal, ImmutableMap.of()));
                        }).collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
            }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public EvaluatePartitionContext processContext(Expression originResult,
            List<EvaluatePartitionContext> children, Map<Slot, EvaluatePartitionContext> currentInputs) {
        // simply return originResult, and don't update range.
        return new EvaluatePartitionContext(originResult, ImmutableMap.of());
    }
}
