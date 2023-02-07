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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SemiJoinSemiJoinTransposeTest {
    public static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    public static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    public static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSemiJoinLogicalTransposeCommute() {
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTranspose.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan join = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    Assertions.assertTrue(join instanceof LogicalJoin);
                    Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, ((LogicalJoin<?, ?>) join).getJoinType());
                    Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN,
                            ((LogicalJoin<?, ?>) ((LogicalJoin<?, ?>) join).left()).getJoinType());
                });
    }
}
