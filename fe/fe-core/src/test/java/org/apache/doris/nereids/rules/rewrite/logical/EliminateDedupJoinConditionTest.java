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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class EliminateDedupJoinConditionTest implements PatternMatchSupported {
    @Test
    void testEliminate() {
        LogicalPlan plan = new LogicalPlanBuilder(PlanConstructor.scan1)
                .join(PlanConstructor.scan2, JoinType.INNER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(0, 0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .matches(
                        logicalJoin().when(join -> join.getHashJoinConjuncts().size() == 2)
                )
                .applyTopDown(new EliminateDedupJoinCondition())
                .matches(
                        logicalJoin().when(join -> join.getHashJoinConjuncts().size() == 1)
                );
    }
}
