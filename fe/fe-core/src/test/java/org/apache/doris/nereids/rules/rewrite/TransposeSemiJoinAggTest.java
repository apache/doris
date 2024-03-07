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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class TransposeSemiJoinAggTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void simple() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .aggAllUsingIndex(ImmutableList.of(0, 1), ImmutableList.of(0, 1))
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new TransposeSemiJoinAgg())
                .matchesFromRoot(
                        logicalAggregate(
                                leftSemiLogicalJoin()
                        )
                );
    }

    @Test
    void rejectScalarAgg() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .agg(ImmutableList.of(), ImmutableList.of((new Sum(scan1.getOutput().get(0))).alias("sum")))
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new TransposeSemiJoinAgg())
                .matchesFromRoot(leftSemiLogicalJoin(logicalAggregate(), logicalOlapScan()));
    }

}
