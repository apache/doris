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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SingleTabletGatherPlanTest extends TestWithFeService {
    private static final String SCALAR = "select count(case when cast(v as boolean) then 1 end), sum(v) "
            + "from single_tablet_gather_plan.t";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("single_tablet_gather_plan");
        createTable("create table single_tablet_gather_plan.t (k int, v int null) "
                + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
        createTable("create table single_tablet_gather_plan.multi (k int, v int null) "
                + "duplicate key(k) distributed by hash(k) buckets 3 properties('replication_num'='1')");
        createTable("create table single_tablet_gather_plan.parts (k int, v int null) duplicate key(k) "
                + "partition by range(k) (partition p1 values less than (3), partition p2 values less than (10)) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
    }

    @BeforeEach
    void resetSession() {
        SessionVariable session = connectContext.getSessionVariable();
        session.enableSingleTabletGather = true;
        session.enableQueryCache = false;
        session.parallelPipelineTaskNum = 1;
        session.aggPhase = 0;
        session.setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testSingleFragmentAndProjection() {
        assertSingleFragment(SCALAR);
        assertSingleFragment("select count(case when b then 1 end) from "
                + "(select cast(v as boolean) b from single_tablet_gather_plan.t where v > 0) s");
    }

    @Test
    void testFallbackGates() {
        SessionVariable session = connectContext.getSessionVariable();
        session.enableSingleTabletGather = false;
        assertExchange(SCALAR);
        session.enableSingleTabletGather = true;
        session.parallelPipelineTaskNum = 2;
        assertExchange(SCALAR);
        session.parallelPipelineTaskNum = 1;
        session.enableQueryCache = true;
        assertExchange(SCALAR);
    }

    @Test
    void testTabletAndPartitionPruning() {
        assertExchange(SCALAR.replace(".t", ".multi"));
        assertSingleFragment(SCALAR.replace(".t", ".multi") + " where k=1");
        assertExchange(SCALAR.replace(".t", ".parts"));
        assertSingleFragment(SCALAR.replace(".t", ".parts") + " where k < 3");
    }

    @Test
    void testFragmentMergingBoundaries() {
        // Both sides have storage-bound singleton outputs. They cannot be merged without exchanges.
        PlanChecker.from(connectContext).checkPlannerResult("select a.c+b.c from "
                + "(select count(v) c from single_tablet_gather_plan.t) a cross join "
                + "(select count(v) c from single_tablet_gather_plan.t) b", planner -> {
                    Assertions.assertTrue(planner.getFragments().size() > 1);
                });
        PlanChecker.from(connectContext).checkPlannerResult("select count(case when v > 0 then 1 end) "
                + "from single_tablet_gather_plan.t union all select count(case when v > 0 then 1 end) "
                + "from single_tablet_gather_plan.t", planner -> {
                    Assertions.assertTrue(planner.getFragments().size() > 1);
                });
        PlanChecker.from(connectContext).checkPlannerResult("select a.v from "
                + "(select v from single_tablet_gather_plan.t order by v limit 2) a cross join "
                + "(select v from single_tablet_gather_plan.t order by v limit 2) b", planner -> {
                    Assertions.assertTrue(planner.getFragments().size() > 1);
                });
    }

    @Test
    void testRecursiveUnionBoundary() {
        PlanChecker.from(connectContext).checkPlannerResult("with recursive s as ("
                + "select k, v, cast(1 as int) depth from single_tablet_gather_plan.t union all "
                + "select k, v, cast(depth+1 as int) from s where depth < 3) select count(v) from s",
                planner -> Assertions.assertTrue(planner.getFragments().size() > 1));
    }

    private void assertSingleFragment(String sql) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            Assertions.assertEquals(1, planner.getFragments().size(), planner.getOptimizedPlan().treeString());
            PlanFragment fragment = planner.getFragments().get(0);
            Assertions.assertEquals(DataPartition.UNPARTITIONED, fragment.getDataPartition());
            Assertions.assertEquals(1, fragment.getParallelExecNum());
            Assertions.assertFalse(fragment.getPlanRoot().anyMatch(node -> node instanceof ExchangeNode));
        });
    }

    private void assertExchange(String sql) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            Assertions.assertTrue(planner.getFragments().size() > 1, planner.getOptimizedPlan().treeString());
        });
    }
}
