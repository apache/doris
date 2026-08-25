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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests producer-side CTE limit construction in {@link RewriteCteChildren}.
 */
class RewriteCteChildrenLimitPushdownTest {

    @Test
    void testPushMaxConsumerLimitToProducer() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        CTEId cteId = new CTEId(1);
        LogicalCTEProducer<LogicalOlapScan> producer = new LogicalCTEProducer<>(cteId, scan);
        LogicalCTEConsumer consumer1 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte1", producer);
        LogicalCTEConsumer consumer2 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte1", producer);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), producer);
        cascadesContext.getCteIdToConsumers().put(cteId, ImmutableSet.of(consumer1, consumer2));
        cascadesContext.putConsumerIdToLimitRows(consumer1.getRelationId(), 10L);
        cascadesContext.putConsumerIdToLimitRows(consumer2.getRelationId(), 20L);

        Plan rewritten = new RewriteCteChildren(ImmutableList.of(), false)
                .visitLogicalCTEProducer(producer, cascadesContext);

        LogicalCTEProducer<?> rewrittenProducer = (LogicalCTEProducer<?>) rewritten;
        Assertions.assertInstanceOf(LogicalLimit.class, rewrittenProducer.child());
        LogicalLimit<?> limit = (LogicalLimit<?>) rewrittenProducer.child();
        Assertions.assertEquals(20L, limit.getLimit());
        Assertions.assertEquals(0L, limit.getOffset());
        Assertions.assertEquals(LimitPhase.ORIGIN, limit.getPhase());
    }

    @Test
    void testSkipProducerLimitWhenAnyConsumerHasNoLimit() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        CTEId cteId = new CTEId(2);
        LogicalCTEProducer<LogicalOlapScan> producer = new LogicalCTEProducer<>(cteId, scan);
        LogicalCTEConsumer consumer1 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte2", producer);
        LogicalCTEConsumer consumer2 = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), cteId, "cte2", producer);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), producer);
        cascadesContext.getCteIdToConsumers().put(cteId, ImmutableSet.of(consumer1, consumer2));
        cascadesContext.putConsumerIdToLimitRows(consumer1.getRelationId(), 10L);

        Plan rewritten = new RewriteCteChildren(ImmutableList.of(), false)
                .visitLogicalCTEProducer(producer, cascadesContext);

        LogicalCTEProducer<?> rewrittenProducer = (LogicalCTEProducer<?>) rewritten;
        Assertions.assertSame(scan, rewrittenProducer.child());
    }
}
