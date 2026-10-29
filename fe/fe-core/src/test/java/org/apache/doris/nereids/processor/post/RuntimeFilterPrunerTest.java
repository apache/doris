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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

class RuntimeFilterPrunerTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("rf_pruner_test");
        useDatabase("rf_pruner_test");
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(true);
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.BLOOM.getValue());

        createTable("CREATE TABLE rf_prune_fact (id INT, agent_code INT, username VARCHAR(32)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        // Merge-on-read keeps a hidden delete-sign Filter above the agent scan.
        createTable("CREATE TABLE rf_prune_agent (agent_code INT, bloc_id INT) "
                + "UNIQUE KEY(agent_code) DISTRIBUTED BY HASH(agent_code) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='false')");
        createTable("CREATE TABLE rf_prune_bloc (id INT, bloc_code VARCHAR(32)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPreserveRuntimeFilterThroughHiddenFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code "
                        + "JOIN [broadcast] rf_prune_bloc b ON e.bloc_id = b.id "
                        + "WHERE b.bloc_code = 'keep' AND f.username LIKE '%user%'", knownStats);

        // b.id -> e.bloc_id must keep e.agent_code -> f.agent_code effective, even though
        // e has no visible predicate and its static join-column statistics are non-selective.
        List<RuntimeFilter> filters = context.getNereidsRuntimeFilter();
        Assertions.assertEquals(2, filters.size());
        Assertions.assertTrue(filters.stream().flatMap(filter -> filter.getTargetSlots().stream())
                .anyMatch(slot -> slot.getName().equals("agent_code")));
        Assertions.assertTrue(filters.stream().flatMap(filter -> filter.getTargetSlots().stream())
                .anyMatch(slot -> slot.getName().equals("bloc_id")));
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHiddenFilterAloneDoesNotKeepIneffectiveRuntimeFilter(boolean knownStats) {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code", knownStats);

        Assertions.assertTrue(context.getNereidsRuntimeFilter().isEmpty());
        Assertions.assertEquals(1, context.prunedRF.size());
    }

    @Test
    void testVisibleFilterKeepsRuntimeFilter() {
        RuntimeFilterContext context = getRuntimeFilterContext(
                "SELECT f.id FROM rf_prune_fact f "
                        + "JOIN [broadcast] rf_prune_agent e ON f.agent_code = e.agent_code "
                        + "WHERE e.bloc_id = 1", true);

        Assertions.assertEquals(1, context.getNereidsRuntimeFilter().size());
        Assertions.assertTrue(context.prunedRF.isEmpty());
    }

    private RuntimeFilterContext getRuntimeFilterContext(String sql, boolean knownStats) {
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql).rewrite().optimize();
        PhysicalPlan plan = checker.getBestPlanTree();
        ColumnStatistic nonSelectiveStats = new ColumnStatisticBuilder(100)
                .setNdv(100)
                .setNumNulls(0)
                .setAvgSizeByte(4)
                .setMinValue(1)
                .setMaxValue(100)
                .build();
        if (knownStats) {
            Set<PhysicalHashJoin<? extends Plan, ? extends Plan>> joins =
                    plan.collect(PhysicalHashJoin.class::isInstance);
            for (PhysicalHashJoin<? extends Plan, ? extends Plan> join : joins) {
                EqualTo equalTo = (EqualTo) join.getEqualToConjuncts().get(0);
                Slot probeSlot = equalTo.child(0).getInputSlots().iterator().next();
                Slot buildSlot = equalTo.child(1).getInputSlots().iterator().next();
                join.left().getStats().addColumnStats(probeSlot, nonSelectiveStats);
                join.right().getStats().addColumnStats(buildSlot, nonSelectiveStats);
            }
        }
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        return checker.getCascadesContext().getRuntimeFilterContext();
    }
}
