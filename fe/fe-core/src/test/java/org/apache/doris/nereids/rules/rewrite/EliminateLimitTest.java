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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

/**
 * Tests for the elimination of {@link LogicalLimit}.
 */
class EliminateLimitTest implements MemoPatternMatchSupported {
    @Test
    void testEliminateLimit() {
        LogicalPlan limit = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(0, 0).build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), limit)
                .applyTopDown(new EliminateLimit())
                .matches(logicalEmptyRelation());
    }

    @Test
    void testLimitSort() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalPlan limit = new LogicalPlanBuilder(scan).sort(
                        scan.getOutput().stream().map(c -> new OrderKey(c, true, true)).collect(Collectors.toList()))
                .limit(1, 1).build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), limit)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .rewrite()
                .matches(logicalTopN());
    }
}
