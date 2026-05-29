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
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link CollectLimitAboveConsumer}.
 */
class CollectLimitAboveConsumerTest {

    @Test
    void testCollectDirectLimitRowsNeeded() {
        LogicalOlapScan producerPlan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), new CTEId(1), "cte1", producerPlan);
        LogicalLimit<LogicalCTEConsumer> limit = new LogicalLimit<>(10, 5, LimitPhase.ORIGIN, consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), limit);
        Rule rule = new CollectLimitAboveConsumer().buildRules().get(0);
        rule.transform(limit, cascadesContext);

        Map<RelationId, Long> collected = cascadesContext.getStatementContext().getConsumerIdToLimitRows();
        Assertions.assertEquals(15L, collected.get(consumer.getRelationId()));
    }

    @Test
    void testCollectLocalLimitRowsNeededWithoutAddingOffsetAgain() {
        LogicalOlapScan producerPlan = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), new CTEId(2), "cte2", producerPlan);
        LogicalLimit<LogicalCTEConsumer> limit = new LogicalLimit<>(15, 0, LimitPhase.LOCAL, consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), limit);
        Rule rule = new CollectLimitAboveConsumer().buildRules().get(0);
        rule.transform(limit, cascadesContext);

        Map<RelationId, Long> collected = cascadesContext.getStatementContext().getConsumerIdToLimitRows();
        Assertions.assertEquals(15L, collected.get(consumer.getRelationId()));
    }

    @Test
    void testKeepMaxRowsNeededWhenConsumerIsCollectedMultipleTimes() {
        LogicalOlapScan producerPlan = PlanConstructor.newLogicalOlapScan(10, "t_merge", 0);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), new CTEId(10), "cte_merge", producerPlan);
        LogicalLimit<LogicalCTEConsumer> highLimit = new LogicalLimit<>(20, 0, LimitPhase.ORIGIN, consumer);
        LogicalLimit<LogicalCTEConsumer> lowLimit = new LogicalLimit<>(3, 0, LimitPhase.ORIGIN, consumer);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), highLimit);
        Rule rule = new CollectLimitAboveConsumer().buildRules().get(0);
        rule.transform(highLimit, cascadesContext);
        rule.transform(lowLimit, cascadesContext);

        Map<RelationId, Long> collected = cascadesContext.getStatementContext().getConsumerIdToLimitRows();
        Assertions.assertEquals(20L, collected.get(consumer.getRelationId()));
    }

    @Test
    void testCollectLimitAboveProjectRowsNeeded() {
        LogicalOlapScan producerPlan = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                PlanConstructor.getNextRelationId(), new CTEId(3), "cte3", producerPlan);
        LogicalProject<LogicalCTEConsumer> project = new LogicalProject<>(
                ImmutableList.copyOf(consumer.getOutput()), consumer);
        LogicalLimit<LogicalProject<LogicalCTEConsumer>> limit = new LogicalLimit<>(
                7, 0, LimitPhase.LOCAL, project);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(new ConnectContext(), limit);
        List<Rule> rules = new CollectLimitAboveConsumer().buildRules();
        rules.get(1).transform(limit, cascadesContext);

        Map<RelationId, Long> collected = cascadesContext.getStatementContext().getConsumerIdToLimitRows();
        Assertions.assertEquals(7L, collected.get(consumer.getRelationId()));
    }
}
