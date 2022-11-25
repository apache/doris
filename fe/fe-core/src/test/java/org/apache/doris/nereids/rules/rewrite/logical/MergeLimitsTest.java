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

import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

public class MergeLimitsTest implements PatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    public void parentIncludeChild() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(10, 0)
                .limit(1, 0)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 1)
                );
    }

    @Test
    public void parentIntersectChild() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(10, 0)
                .limit(10, 5)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 5 && limit.getOffset() == 5)
                );
    }

    @Test
    public void childOverParent() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(10, 0)
                .limit(10, 10)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 0 && limit.getOffset() == 10)
                );

        LogicalPlan plan1 = new LogicalPlanBuilder(scan1)
                .limit(10, 0)
                .limit(3, 11)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan1)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 0 && limit.getOffset() == 11)
                );
    }

    @Test
    public void testChildNotLimit() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(-1, 0)
                .limit(10, 10)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 10 && limit.getOffset() == 10)
                );
    }

    @Test
    public void testParentNotLimit() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(10, 0)
                .limit(-1, 5)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == 5 && limit.getOffset() == 5)
                );
    }

    @Test
    public void testBothNotLimit() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .limit(-1, 3)
                .limit(-1, 5)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeLimits())
                .matchesFromRoot(
                        logicalLimit(logicalOlapScan())
                                .when(limit -> limit.getLimit() == -1 && limit.getOffset() == 8)
                );
    }
}
