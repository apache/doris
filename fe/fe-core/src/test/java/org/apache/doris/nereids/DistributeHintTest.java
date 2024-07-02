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

package org.apache.doris.nereids;

import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MatchingUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DistributeHintTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setParallelResultSink(false);

        createTable("CREATE TABLE `t1` (\n"
                + "  `a` int(11) NULL,\n"
                + "  `b` int(11) NULL,\n"
                + "  `c` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`a`, `b`, `c`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`b`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `x` int(11) NULL,\n"
                + "  `y` int(11) NULL,\n"
                + "  `z` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`x`, `y`, `z`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`y`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t3` (\n"
                + "  `x` int(11) NULL,\n"
                + "  `y` int(11) NULL,\n"
                + "  `z` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`x`, `y`, `z`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`y`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @Test
    public void testBroadcastJoinHint() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "select * from t1 join [broadcast] t2 on t1.a=t2.x",
                planner -> checkPlannerResult(planner, DistributionMode.BROADCAST)
        );
    }

    @Test
    public void testShuffleJoinHint() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "select * from t1 join [shuffle] t2 on t1.a=t2.x",
                planner -> checkPlannerResult(planner, DistributionMode.PARTITIONED)
        );
    }

    @Test
    public void testHintWithReorderCrossJoin() {
        String sql = "select t1.a , t2.x, t.x from "
                + "t1 join [shuffle] t2, (select x from t3) t where t1.a=t.x and t2.x=t.x";
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            Plan plan = planner.getOptimizedPlan();
            MatchingUtils.assertMatches(plan,
                    physicalResultSink(
                            physicalDistribute(
                                    physicalHashJoin(
                                            physicalDistribute(physicalHashJoin(physicalProject(), physicalDistribute()))
                                                    .when(dis -> {
                                                        DistributionSpec spec = dis.getDistributionSpec();
                                                        Assertions.assertInstanceOf(DistributionSpecHash.class, spec);
                                                        DistributionSpecHash hashSpec = (DistributionSpecHash) spec;
                                                        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED,
                                                                hashSpec.getShuffleType());
                                                        return true;
                                                    }),
                                            physicalDistribute()
                                    ).when(join -> join.getDistributeHint().distributeType
                                            == DistributeType.SHUFFLE_RIGHT)
                            )
                    ));
        });
    }

    private void checkPlannerResult(NereidsPlanner planner, DistributionMode mode) {
        List<PlanFragment> fragments = planner.getFragments();
        Set<HashJoinNode> hashJoins = new HashSet<>();
        for (PlanFragment fragment : fragments) {
            PlanNode plan = fragment.getPlanRoot();
            plan.collect(HashJoinNode.class, hashJoins);
        }

        Assertions.assertEquals(1, hashJoins.size());
        HashJoinNode join = hashJoins.iterator().next();
        Assertions.assertEquals(mode, join.getDistributionMode());
    }
}
