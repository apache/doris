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

class JoinExchangeBothProjectTest implements MemoPatternMatchSupported {
    @Test
    public void testSimple() {
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);

        LogicalPlan plan = new LogicalPlanBuilder(
                new LogicalPlanBuilder(scan1)
                        .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                        .project(ImmutableList.of(0, 2))
                        .build())
                .join(
                        new LogicalPlanBuilder(scan3)
                                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .project(ImmutableList.of(0, 2))
                                .build(),
                        JoinType.INNER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(1, 1)))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(JoinExchangeBothProject.INSTANCE.build())
                .matchesExploration(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                )
                            ),
                            logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t4"))
                                )
                            )
                        )
                    )
                );
    }
}
