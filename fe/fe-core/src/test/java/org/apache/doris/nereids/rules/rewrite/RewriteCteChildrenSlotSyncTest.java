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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests that CTEConsumer slot maps are synced after producer rewrite
 * changes output ExprIds (e.g. EliminateGroupByKey any_value wrapping).
 */
class RewriteCteChildrenSlotSyncTest {

    @Test
    void testConsumerSlotMapSyncedWhenProducerOutputExprIdChanged() {
        // ---- Setup: two scans with different ExprIds in their output ----
        LogicalOlapScan oldScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan newScan = PlanConstructor.newLogicalOlapScan(1, "t1", 0);
        CTEId cteId = new CTEId(1);

        // Original producer wraps oldScan (old output ExprIds)
        LogicalCTEProducer<LogicalOlapScan> originalProducer = new LogicalCTEProducer<>(cteId, oldScan);

        // Consumer slot maps reference original producer output (old ExprIds)
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte1", originalProducer);
        Map<Slot, Slot> originalConsumerToProducer = consumer.getConsumerToProducerOutputMap();

        // CTEAnchor: original producer (left) + consumer side (right)
        LogicalCTEAnchor<LogicalCTEProducer<LogicalOlapScan>, LogicalCTEConsumer> cteAnchor =
                new LogicalCTEAnchor<>(cteId, originalProducer, consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new ConnectContext(), cteAnchor);

        // Simulate that the producer was already rewritten (cached) with newScan,
        // whose output has different ExprIds than oldScan.
        cascadesContext.getStatementContext().getRewrittenCteProducer().put(cteId, newScan);

        // Consumer cache: the consumer still has stale slot maps (pointing to old ExprIds)
        cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteId, consumer);
        cascadesContext.getCteIdToConsumers().put(cteId, ImmutableSet.of(consumer));

        // ---- Execute ----
        RewriteCteChildren rewriter = new RewriteCteChildren(ImmutableList.of(), false);
        Plan result = rewriter.visitLogicalCTEAnchor(cteAnchor, cascadesContext);

        // ---- Verify ----
        Assertions.assertInstanceOf(LogicalCTEAnchor.class, result);
        LogicalCTEAnchor<?, ?> resultAnchor = (LogicalCTEAnchor<?, ?>) result;

        // Producer side should have the new output ExprIds (from newScan)
        List<Slot> producerOutput = resultAnchor.child(0).getOutput();
        Assertions.assertEquals(newScan.getOutput().size(), producerOutput.size());
        for (int i = 0; i < producerOutput.size(); i++) {
            Assertions.assertEquals(newScan.getOutput().get(i).getExprId(),
                    producerOutput.get(i).getExprId(),
                    "Producer output ExprId at position " + i + " should match newScan");
        }

        // Consumer slot maps should now reference the new producer ExprIds
        LogicalPlan consumerSide = (LogicalPlan) resultAnchor.child(1);
        Assertions.assertInstanceOf(LogicalCTEConsumer.class, consumerSide);
        LogicalCTEConsumer resultConsumer = (LogicalCTEConsumer) consumerSide;

        Map<Slot, Slot> updatedConsumerToProducer = resultConsumer.getConsumerToProducerOutputMap();
        Assertions.assertEquals(originalConsumerToProducer.size(), updatedConsumerToProducer.size());

        for (Map.Entry<Slot, Slot> entry : updatedConsumerToProducer.entrySet()) {
            Slot consumerSlot = entry.getKey();
            Slot producerSlot = entry.getValue();
            ExprId producerExprId = producerSlot.getExprId();

            // Each consumer slot's producer reference must exist in the new producer output
            boolean found = producerOutput.stream()
                    .anyMatch(s -> s.getExprId().equals(producerExprId));
            Assertions.assertTrue(found,
                    "Consumer slot " + consumerSlot + " references producer ExprId "
                    + producerExprId + " which is not in producer output");
        }
    }

    @Test
    void testConsumerSlotMapUnchangedWhenProducerOutputExprIdNotChanged() {
        // Setup: same scan used for both old and new producer output (no ExprId change)
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        CTEId cteId = new CTEId(2);

        LogicalCTEProducer<LogicalOlapScan> originalProducer = new LogicalCTEProducer<>(cteId, scan);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte2", originalProducer);
        LogicalCTEAnchor<LogicalCTEProducer<LogicalOlapScan>, LogicalCTEConsumer> cteAnchor =
                new LogicalCTEAnchor<>(cteId, originalProducer, consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new ConnectContext(), cteAnchor);

        // Cache the SAME scan as "rewritten" producer (no ExprId change)
        cascadesContext.getStatementContext().getRewrittenCteProducer().put(cteId, scan);
        cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteId, consumer);
        cascadesContext.getCteIdToConsumers().put(cteId, ImmutableSet.of(consumer));

        RewriteCteChildren rewriter = new RewriteCteChildren(ImmutableList.of(), false);
        Plan result = rewriter.visitLogicalCTEAnchor(cteAnchor, cascadesContext);

        // Consumer slot maps should be unchanged (no ExprId change in producer output)
        LogicalCTEAnchor<?, ?> resultAnchor = (LogicalCTEAnchor<?, ?>) result;
        LogicalCTEConsumer resultConsumer = (LogicalCTEConsumer) resultAnchor.child(1);

        // The consumer instance should be the SAME (no new instance created since no change)
        Assertions.assertSame(consumer, resultConsumer,
                "Consumer should be unchanged when producer output ExprIds don't change");
    }

    @Test
    void testConsumerSlotMapSyncedWhenProducerOutputPrunedAndExprIdChanged() {
        // Producer originally outputs [id, name, x]. Consumers only need [name, x], so
        // visitLogicalCTEProducer prunes id (arity 3 -> 2), while EliminateGroupByKey wraps
        // name with any_value() and assigns it a fresh ExprId. The slot-map sync must
        // propagate name's new ExprId even though the producer output arity changed.
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias xAlias = new Alias(new ExprId(100), idSlot, "x");
        LogicalProject<LogicalOlapScan> oldProducerChild = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(idSlot, nameSlot, xAlias), scan);
        CTEId cteId = new CTEId(3);
        LogicalCTEProducer<LogicalProject<LogicalOlapScan>> originalProducer =
                new LogicalCTEProducer<>(cteId, oldProducerChild);

        // Two retained consumers referencing the original 3-slot producer output
        LogicalCTEConsumer consumer1 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte3", originalProducer);
        LogicalCTEConsumer consumer2 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte3", originalProducer);
        LogicalPlan consumerSide = new LogicalJoin<>(JoinType.CROSS_JOIN,
                consumer1, consumer2, new JoinReorderContext());

        LogicalCTEAnchor<LogicalCTEProducer<LogicalProject<LogicalOlapScan>>, LogicalPlan> cteAnchor =
                new LogicalCTEAnchor<>(cteId, originalProducer, consumerSide);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(
                new ConnectContext(), cteAnchor);

        // Simulate the rewritten producer: id pruned, name wrapped by any_value (fresh ExprId)
        Slot newNameSlot = (Slot) nameSlot.withExprId(new ExprId(200));
        LogicalProject<LogicalOlapScan> newProducerChild = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(newNameSlot, xAlias.toSlot()), scan);
        cascadesContext.getStatementContext().getRewrittenCteProducer().put(cteId, newProducerChild);
        cascadesContext.getStatementContext().getRewrittenCteConsumer().put(cteId, consumerSide);
        cascadesContext.getCteIdToConsumers().put(cteId, ImmutableSet.of(consumer1, consumer2));
        // Consumers only need [name, x]: id is pruned from the producer output
        cascadesContext.getStatementContext().getCteIdToOutputIds().put(cteId,
                ImmutableSet.of(nameSlot, xAlias.toSlot()));

        // ---- Execute ----
        RewriteCteChildren rewriter = new RewriteCteChildren(ImmutableList.of(), false);
        Plan result = rewriter.visitLogicalCTEAnchor(cteAnchor, cascadesContext);

        // ---- Verify ----
        Assertions.assertInstanceOf(LogicalCTEAnchor.class, result);
        LogicalCTEAnchor<?, ?> resultAnchor = (LogicalCTEAnchor<?, ?>) result;

        // Producer side has the pruned output [name(new ExprId), x]
        List<Slot> producerOutput = resultAnchor.child(0).getOutput();
        Assertions.assertEquals(2, producerOutput.size());
        Assertions.assertEquals(new ExprId(200), producerOutput.get(0).getExprId());
        Assertions.assertEquals(new ExprId(100), producerOutput.get(1).getExprId());

        // Both consumers must reference the new producer ExprId for name
        List<LogicalCTEConsumer> resultConsumers = new ArrayList<>();
        resultAnchor.child(1).foreach(p -> {
            if (p instanceof LogicalCTEConsumer) {
                resultConsumers.add((LogicalCTEConsumer) p);
            }
            return false;
        });
        Assertions.assertEquals(2, resultConsumers.size());
        for (LogicalCTEConsumer resultConsumer : resultConsumers) {
            Map<Slot, Slot> consumerToProducer = resultConsumer.getConsumerToProducerOutputMap();
            Assertions.assertTrue(consumerToProducer.values().stream()
                            .anyMatch(s -> s.getExprId().equals(new ExprId(200))),
                    "Consumer should reference the new producer ExprId for name");
            Assertions.assertFalse(consumerToProducer.values().stream()
                            .anyMatch(s -> s.getExprId().equals(nameSlot.getExprId())),
                    "Consumer should no longer reference the old producer ExprId for name");
            Assertions.assertTrue(consumerToProducer.values().stream()
                            .anyMatch(s -> s.getExprId().equals(new ExprId(100))),
                    "Consumer should still reference the unchanged producer ExprId for x");
        }
    }

}
