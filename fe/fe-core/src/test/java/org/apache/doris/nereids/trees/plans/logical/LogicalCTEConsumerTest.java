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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.rules.rewrite.AdjustNullable;
import org.apache.doris.nereids.rules.rewrite.EliminateGroupByKey;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class LogicalCTEConsumerTest {
    private LogicalCTEConsumer newConsumer() {
        Slot key = new SlotReference("x", IntegerType.INSTANCE, false);
        Slot payload = new SlotReference("y", IntegerType.INSTANCE, false);
        LogicalEmptyRelation producer = new LogicalEmptyRelation(new RelationId(0),
                ImmutableList.of(key, payload, key.withName("z")));
        return new LogicalCTEConsumer(new RelationId(1), new CTEId(0), "q", producer);
    }

    private void assertColumnOrder(LogicalCTEConsumer consumer) {
        List<Slot> output = consumer.getOutput();
        Assertions.assertEquals(ImmutableList.of("x", "y", "z"), output.stream()
                .map(Slot::getName).collect(Collectors.toList()));
        Assertions.assertEquals(consumer.getProducerSlot(output.get(0)).getExprId(),
                consumer.getProducerSlot(output.get(2)).getExprId());
        Assertions.assertNotEquals(consumer.getProducerSlot(output.get(0)).getExprId(),
                consumer.getProducerSlot(output.get(1)).getExprId());
    }

    @Test
    void testRepeatedProducerSlotKeepsColumnOrder() {
        LogicalCTEConsumer consumer = newConsumer();
        assertColumnOrder(consumer);
        assertColumnOrder((LogicalCTEConsumer) consumer.withGroupExpression(Optional.empty()));
        assertColumnOrder((LogicalCTEConsumer) consumer.withGroupExprLogicalPropChildren(
                Optional.empty(), Optional.empty(), ImmutableList.of()));
    }

    @Test
    void testPruneOutputsKeepsColumnOrder() {
        LogicalCTEConsumer consumer = newConsumer();
        List<Slot> output = ImmutableList.copyOf(consumer.getConsumerToProducerOutputMap().keySet());
        LogicalCTEConsumer pruned = (LogicalCTEConsumer) consumer.pruneOutputs(
                ImmutableList.of(output.get(1), output.get(2)));
        Assertions.assertEquals(ImmutableList.of("y", "z"), pruned.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList()));
    }

    @Test
    void testAdjustNullableKeepsColumnOrder() {
        LogicalCTEConsumer consumer = newConsumer();
        Slot producer = consumer.getConsumerToProducerOutputMap().values().iterator().next();
        Map<ExprId, Slot> replaceMap = new HashMap<>();
        replaceMap.put(producer.getExprId(), producer.withNullable(true));
        LogicalCTEConsumer updated = (LogicalCTEConsumer) new AdjustNullable(false)
                .visitLogicalCTEConsumer(consumer, replaceMap);
        assertColumnOrder(updated);
        Assertions.assertTrue(updated.getOutput().get(0).nullable());
        Assertions.assertTrue(updated.getOutput().get(2).nullable());
        Assertions.assertFalse(updated.getOutput().get(1).nullable());
    }

    @Test
    void testReplaceProducerIdKeepsColumnOrder() {
        LogicalCTEConsumer consumer = newConsumer();
        Slot producer = consumer.getConsumerToProducerOutputMap().values().iterator().next();
        ExprId newId = new ExprId(999);
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(producer.getExprId(), newId);
        LogicalCTEConsumer updated = (LogicalCTEConsumer) new EliminateGroupByKey()
                .visitLogicalCTEConsumer(consumer, replaceMap);
        assertColumnOrder(updated);
        Assertions.assertEquals(newId, updated.getProducerSlot(updated.getOutput().get(0)).getExprId());
        Assertions.assertEquals(newId, updated.getProducerSlot(updated.getOutput().get(2)).getExprId());
    }
}
