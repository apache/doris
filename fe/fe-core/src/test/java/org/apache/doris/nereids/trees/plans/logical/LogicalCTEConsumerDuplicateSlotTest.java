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

import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Test for LogicalCTEConsumer constructor with duplicate slot handling.
 *
 * This test verifies the constructor:
 * public LogicalCTEConsumer(RelationId relationId, CTEId cteId, String name, LogicalPlan producerPlan)
 *
 * Specifically tests the comment:
 * "apply distinct on producerPlan.getOutput() to avoid mis-matching between cToPBuilder and pToCBuilder"
 */
public class LogicalCTEConsumerDuplicateSlotTest {

    /**
     * Test that LogicalCTEConsumer constructor correctly handles duplicate slots in producer output.
     *
     * Problem scenario without distinct():
     * - producerPlan.getOutput() = [a#1, a#1] (duplicate slots)
     * - generateConsumerSlot() creates: a#1->b#2, a#1->b#3
     * - Result: cToP map overwrites, only keeps last mapping
     *
     * Solution with distinct():
     * - Remove duplicates first, then create 1:1 mapping
     */
    @Test
    void testConstructorWithDuplicateSlots() {
        // Create duplicate producer slots
        SlotReference duplicateSlot = new SlotReference(
                new ExprId(1), "col_a", IntegerType.INSTANCE, true, ImmutableList.of());

        MockProducerPlan producerPlan = new MockProducerPlan(ImmutableList.of(duplicateSlot, duplicateSlot));

        // Test the constructor
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                new RelationId(1), new CTEId(1), "test_cte", producerPlan);

        // Verify distinct() was applied - should have only 1 mapping
        Map<Slot, Slot> consumerToProducer = consumer.getConsumerToProducerOutputMap();
        Multimap<Slot, Slot> producerToConsumer = consumer.getProducerToConsumerOutputMap();

        Assertions.assertEquals(1, consumerToProducer.size(),
                "Should have 1 mapping after distinct()");
        Assertions.assertEquals(1, producerToConsumer.keySet().size(),
                "Should have 1 producer key after distinct()");
        Assertions.assertEquals(1, consumer.getOutput().size(),
                "Consumer output should have 1 slot");

        // Verify mapping consistency
        Slot consumerSlot = consumer.getOutput().get(0);
        Slot mappedProducerSlot = consumerToProducer.get(consumerSlot);
        Assertions.assertEquals(duplicateSlot, mappedProducerSlot);
        Assertions.assertTrue(producerToConsumer.get(duplicateSlot).contains(consumerSlot));
    }

    /**
     * Test that constructor works correctly with unique producer slots.
     */
    @Test
    void testConstructorWithUniqueSlots() {
        // Create unique producer slots
        SlotReference slotA = new SlotReference(new ExprId(1), "col_a", IntegerType.INSTANCE, true, ImmutableList.of());
        //SlotReference slotB = new SlotReference(new ExprId(2), "col_b", IntegerType.INSTANCE, true, ImmutableList.of());

        MockProducerPlan producerPlan = new MockProducerPlan(ImmutableList.of(slotA, slotA));

        // Test the constructor
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                new RelationId(1), new CTEId(1), "test_cte", producerPlan);

        Map<Slot, Slot> consumerToProducer = consumer.getConsumerToProducerOutputMap();
        Multimap<Slot, Slot> producerToConsumer = consumer.getProducerToConsumerOutputMap();

        Assertions.assertEquals(1, consumerToProducer.size());
        Assertions.assertEquals(1, producerToConsumer.keySet().size());
        Assertions.assertEquals(1, consumer.getOutput().size());
    }

    /**
     * Simple mock producer plan for testing
     */
    private static class MockProducerPlan extends LogicalProject<Plan> {
        private final List<Slot> customOutput;

        MockProducerPlan(List<Slot> output) {
            super(ImmutableList.of(), new LogicalOneRowRelation(new RelationId(0), ImmutableList.of()));
            this.customOutput = output;
        }

        @Override
        public List<Slot> computeOutput() {
            return customOutput;
        }
    }
}
