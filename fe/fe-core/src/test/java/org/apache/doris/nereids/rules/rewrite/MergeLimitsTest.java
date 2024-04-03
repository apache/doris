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

import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MergeLimits}.
 */
class MergeLimitsTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testMergeLimits() {
        LogicalPlan logicalLimit = new LogicalPlanBuilder(scan1)
                .limit(3, 5)
                .limit(2, 0)
                .limit(10, 0).build();

        PlanChecker.from(new ConnectContext(), logicalLimit).applyTopDown(new MergeLimits())
                .matches(
                        logicalLimit().when(limit -> limit.getLimit() == 2).when(limit -> limit.getOffset() == 5));
    }

    @Test
    void testMergeLimitsWithLocalUpperAndGlobalBottom() {
        LogicalPlan logicalLimit = new LogicalLimit<>(2, 0, LimitPhase.LOCAL,
                new LogicalLimit<>(10, 0, LimitPhase.GLOBAL, scan1));

        PlanChecker.from(new ConnectContext(), logicalLimit).applyTopDown(new MergeLimits())
                .matches(
                        logicalLimit().when(limit -> limit.getLimit() == 2)
                                .when(limit -> limit.getPhase() == LimitPhase.GLOBAL));
    }

    @Test
    void testMergeLimitsWithLocalUpperAndLocalBottom() {
        LogicalPlan logicalLimit = new LogicalLimit<>(2, 0, LimitPhase.LOCAL,
                new LogicalLimit<>(10, 0, LimitPhase.LOCAL, scan1));

        PlanChecker.from(new ConnectContext(), logicalLimit).applyTopDown(new MergeLimits())
                .matches(
                        logicalLimit().when(limit -> limit.getLimit() == 2)
                                .when(limit -> limit.getPhase() == LimitPhase.LOCAL));
    }

    @Test
    void testMergeLimitsWithGlobalUpperAndGlobalBottom() {
        LogicalPlan logicalLimit = new LogicalLimit<>(2, 0, LimitPhase.GLOBAL,
                new LogicalLimit<>(10, 0, LimitPhase.GLOBAL, scan1));

        PlanChecker.from(new ConnectContext(), logicalLimit).applyTopDown(new MergeLimits())
                .matches(
                        logicalLimit().when(limit -> limit.getLimit() == 2)
                                .when(limit -> limit.getPhase() == LimitPhase.GLOBAL));
    }

    @Test
    void testMergeLimitsWithGlobalUpperAndLocalBottom() {
        LogicalPlan logicalLimit = new LogicalLimit<>(2, 0, LimitPhase.GLOBAL,
                new LogicalLimit<>(10, 0, LimitPhase.LOCAL, scan1));

        PlanChecker.from(new ConnectContext(), logicalLimit).applyTopDown(new MergeLimits())
                .matches(
                        logicalLimit(
                                logicalLimit()
                                        .when(limit -> limit.getLimit() == 10)
                                        .when(limit -> limit.getPhase() == LimitPhase.LOCAL))
                                .when(limit -> limit.getLimit() == 2)
                                .when(limit -> limit.getPhase() == LimitPhase.GLOBAL));
    }
}
