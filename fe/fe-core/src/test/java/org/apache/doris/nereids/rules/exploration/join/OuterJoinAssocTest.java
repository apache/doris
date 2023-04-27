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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

class OuterJoinAssocTest implements MemoPatternMatchSupported {
    LogicalOlapScan scan1;
    LogicalOlapScan scan2;
    LogicalOlapScan scan3;

    public OuterJoinAssocTest() throws Exception {
        // clear id so that slot id keep consistent every running
        StatementScopeIdGenerator.clear();
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    }

    @Test
    public void testInnerLeft() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(2, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyExploration(OuterJoinAssoc.INSTANCE.build())
                .matchesExploration(
                        logicalJoin(
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                logicalJoin()
                        ).when(top -> Objects.equals(top.getHashJoinConjuncts().toString(), "[(id#0 = id#2)]"))
                );
    }

    @Test
    public void testLeftLeft() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(2, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyExploration(OuterJoinAssoc.INSTANCE.build())
                .matchesExploration(
                        logicalJoin(
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                logicalJoin()
                        ).when(top -> Objects.equals(top.getHashJoinConjuncts().toString(), "[(id#0 = id#2)]"))
                );
    }

    @Test
    public void rejectNull() {
        IsNull isNull = new IsNull(scan3.getOutput().get(0));
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .join(scan3, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(), ImmutableList.of(isNull)) // t3.id is not null
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyExploration(OuterJoinAssoc.INSTANCE.build())
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }
}
