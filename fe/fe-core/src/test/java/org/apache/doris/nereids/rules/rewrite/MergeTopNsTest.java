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

import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * MergeConsecutiveProjects ut
 */
class MergeTopNsTest implements MemoPatternMatchSupported {
    LogicalOlapScan score = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);

    @Test
    void testMergeSameOrderBy() {
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .topN(10, 0, ImmutableList.of(0, 1))
                .topN(20, 0, ImmutableList.of(0, 1))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeTopNs())
                .matches(
                        logicalTopN(logicalOlapScan()).when(topN -> topN.getLimit() == 10)
                );
    }

    @Test
    void testMergeSubOrderBy() {
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .topN(10, 0, ImmutableList.of(0))
                .topN(20, 0, ImmutableList.of(0, 1))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeTopNs())
                .matches(
                        logicalTopN(logicalOlapScan()).when(topN -> topN.getLimit() == 10)
                );
    }

    @Test
    void testOffset() {
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .topN(10, 3, ImmutableList.of(0))
                .topN(20, 1, ImmutableList.of(0, 1))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeTopNs())
                .matches(
                        logicalTopN(logicalOlapScan()).when(topN -> topN.getLimit() == 10 && topN.getOffset() == 4)
                );
    }

}
