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
import org.junit.jupiter.api.Assertions;
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
    void testMergeChildMoreOrderKeys() {
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .topN(20, 0, ImmutableList.of(0, 1))
                .topN(10, 0, ImmutableList.of(0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeTopNs())
                .matches(
                        logicalTopN(logicalOlapScan()).when(topN -> {
                            Assertions.assertEquals(10, topN.getLimit());
                            Assertions.assertEquals(0, topN.getOffset());
                            Assertions.assertEquals(2, topN.getOrderKeys().size());
                            Assertions.assertEquals(score.getOutput().get(0), topN.getOrderKeys().get(0).getExpr());
                            Assertions.assertEquals(score.getOutput().get(1), topN.getOrderKeys().get(1).getExpr());
                            return true;
                        })
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
                        logicalTopN(logicalOlapScan()).when(topN -> topN.getLimit() == 9 && topN.getOffset() == 4)
                );
    }

    @Test
    void testParentOffsetReducesChildLimit() {
        // DORIS-26301: when the parent (upper) TopN has a non-zero offset, the merged limit must be
        // clamped by (childLimit - parentOffset); otherwise rows beyond the child's limit leak through.
        // child : ORDER BY k LIMIT 5           (limit=5, offset=0) -> yields 5 rows
        // parent: ORDER BY k LIMIT 3 OFFSET 4  (limit=3, offset=4) -> skips 4, only 1 row remains
        // merged: offset = 4, limit = min(3, max(5 - 4, 0)) = 1
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .topN(5, 0, ImmutableList.of(0))
                .topN(3, 4, ImmutableList.of(0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeTopNs())
                .matches(
                        logicalTopN(logicalOlapScan()).when(topN -> topN.getLimit() == 1 && topN.getOffset() == 4)
                );
    }

}
