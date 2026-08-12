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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class PushDownProjectThroughLimitTest implements MemoPatternMatchSupported {

    @Test
    void testPushdownProjectThroughLimit() {
        LogicalPlan project = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .limit(1, 1)
                .project(ImmutableList.of(0)) // id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownProjectThroughLimit())
                .matches(
                        logicalLimit(logicalProject())
                );
    }

    /**
     * A project that computes a NoneMovableFunction (assert_true) must not be pushed below the
     * limit: the limit prunes rows, so assert_true would be evaluated on fewer rows.
     */
    @Test
    void testDoNotPushProjectWithNoneMovableFunction() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Alias assertAlias = new Alias(new AssertTrue(
                new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0)),
                new StringLiteral("msg")), "x");
        LogicalPlan project = new LogicalPlanBuilder(scan)
                .limit(1, 1)
                .projectExprs(ImmutableList.of(assertAlias, scan.getOutput().get(0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownProjectThroughLimit())
                .matches(
                        logicalProject(
                                logicalLimit(logicalOlapScan())
                        )
                );
    }
}
