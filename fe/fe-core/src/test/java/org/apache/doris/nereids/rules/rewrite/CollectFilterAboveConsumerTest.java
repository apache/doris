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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link CollectFilterAboveConsumer}.
 */
class CollectFilterAboveConsumerTest {

    /**
     * Filter has two conjuncts: one deterministic, one containing a unique (non-idempotent) function.
     * After applying the rule, only the deterministic conjunct should be collected into
     * consumerIdToFilter. Collecting the unique-function conjunct would cause it to be pushed into
     * the CTE producer while still remaining on the consumer side, resulting in double execution
     * (bug DORIS-25202).
     */
    @Test
    void testDoNotCollectUniqueFunctionConjunct() {
        ConnectContext connectContext = new ConnectContext();
        LogicalOlapScan producerPlan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), new CTEId(1), "cte1", producerPlan);

        Slot idSlot = consumer.getOutput().get(0);
        Expression deterministic = new EqualTo(idSlot, new IntegerLiteral(1));
        Expression uniqueFn = new GreaterThan(new Random(), new DoubleLiteral(0.5));
        LogicalFilter<LogicalCTEConsumer> filter = new LogicalFilter<>(
                ImmutableSet.of(deterministic, uniqueFn), consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, filter);
        Rule rule = new CollectFilterAboveConsumer().build();
        rule.transform(filter, cascadesContext);

        Map<RelationId, Set<Expression>> collected = cascadesContext.getStatementContext()
                .getConsumerIdToFilters();
        Set<Expression> filters = collected.get(consumer.getRelationId());
        Assertions.assertNotNull(filters, "deterministic conjunct must be collected");
        // Only the deterministic conjunct (after slot-rewriting to producer slot) should be present,
        // with the unique-function conjunct skipped.
        Assertions.assertEquals(1, filters.size(),
                "exactly one conjunct (the deterministic one) should be collected, "
                        + "unique-function conjunct must NOT be collected");
        Expression onlyCollected = filters.iterator().next();
        Assertions.assertFalse(onlyCollected.containsUniqueFunction(),
                "collected conjunct must not contain a unique function");
    }
}
