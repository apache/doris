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

import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * A CTE producer body is rewritten in its own subtree context, while the consumers, which map to the producer
 * outputs by exprId, were rewritten before it, so RewriteCteChildren refreshes the two maps of the consumers with
 * the exprIds the rules of the producer body published.
 */
public class CteConsumerOutputsRefresherTest {
    private static final CTEId CTE_ID = new CTEId(0);
    private static final ExprId PRODUCER_K = new ExprId(0);
    private static final ExprId PRODUCER_V = new ExprId(1);
    private static final ExprId CONSUMER_K = new ExprId(10);
    private static final ExprId CONSUMER_V = new ExprId(11);

    @Test
    void testRefreshFollowsTheReplacementChain() {
        // k is renamed twice, e.g. by two rules or by two passes of the same rule
        Map<ExprId, ExprId> remap = ImmutableMap.of(PRODUCER_K, new ExprId(2), new ExprId(2), new ExprId(3));
        Slot newProducerK = slot(new ExprId(3), "k");
        LogicalCTEConsumer consumer = consumer(slot(PRODUCER_K, "k"), slot(PRODUCER_V, "v"));

        LogicalCTEConsumer refreshed = refresh(consumer, remap, ImmutableList.of(newProducerK, slot(PRODUCER_V, "v")));
        Assertions.assertEquals(newProducerK, refreshed.getConsumerToProducerOutputMap().get(slot(CONSUMER_K, "k")));
        Assertions.assertEquals(ImmutableList.of(slot(CONSUMER_K, "k")),
                ImmutableList.copyOf(refreshed.getProducerToConsumerOutputMap().get(newProducerK)));
        // v was not renamed, it keeps pointing to the same producer slot
        Assertions.assertEquals(slot(PRODUCER_V, "v"),
                refreshed.getConsumerToProducerOutputMap().get(slot(CONSUMER_V, "v")));
    }

    @Test
    void testRenamedColumnWhichIsPrunedAfterwardsKeepsItsSlot() {
        // the rule renamed k, but the rewrite of the producer body pruned the column afterwards, so the producer
        // does not output the new exprId any more and there is nothing to map for it
        Map<ExprId, ExprId> remap = ImmutableMap.of(PRODUCER_K, new ExprId(3));
        Slot producerV = slot(PRODUCER_V, "v");
        LogicalCTEConsumer consumer = consumer(slot(PRODUCER_K, "k"), producerV);

        LogicalCTEConsumer refreshed = refresh(consumer, remap, ImmutableList.of(producerV));
        Assertions.assertEquals(slot(PRODUCER_K, "k"),
                refreshed.getConsumerToProducerOutputMap().get(slot(CONSUMER_K, "k")));
    }

    @Test
    void testConsumerOfAnotherCteIsNotTouched() {
        Slot producerK = slot(PRODUCER_K, "k");
        LogicalCTEConsumer otherConsumer = new LogicalCTEConsumer(new RelationId(1), new CTEId(1), "other",
                ImmutableMap.of(slot(CONSUMER_K, "k"), producerK),
                ImmutableMultimap.of(producerK, slot(CONSUMER_K, "k")));
        LogicalCTEConsumer refreshed = (LogicalCTEConsumer) otherConsumer.accept(
                new RewriteCteChildren.CteConsumerOutputsRefresher(CTE_ID, ImmutableMap.of(PRODUCER_K, new ExprId(3)),
                        ImmutableMap.of(new ExprId(3), slot(new ExprId(3), "k"))), null);
        Assertions.assertSame(otherConsumer, refreshed);
    }

    private static LogicalCTEConsumer refresh(LogicalCTEConsumer consumer, Map<ExprId, ExprId> remap,
            java.util.List<Slot> producerOutputs) {
        Map<ExprId, Slot> producerOutputSlots = producerOutputs.stream()
                .collect(Collectors.toMap(Slot::getExprId, slot -> slot));
        return (LogicalCTEConsumer) consumer.accept(new RewriteCteChildren.CteConsumerOutputsRefresher(
                CTE_ID, remap, producerOutputSlots), null);
    }

    private static LogicalCTEConsumer consumer(Slot producerK, Slot producerV) {
        return new LogicalCTEConsumer(new RelationId(0), CTE_ID, "c",
                ImmutableMap.of(slot(CONSUMER_K, "k"), producerK, slot(CONSUMER_V, "v"), producerV),
                ImmutableMultimap.of(producerK, slot(CONSUMER_K, "k"), producerV, slot(CONSUMER_V, "v")));
    }

    private static Slot slot(ExprId exprId, String name) {
        return new SlotReference(exprId, name, IntegerType.INSTANCE, true, ImmutableList.of("test", "t"));
    }
}
