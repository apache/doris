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

class TransposeSemiJoinLogicalJoinProjectTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void pushLeft() {
        /*-
         *     topSemiJoin                    project
         *      /     \                         |
         *   project   C                    newTopJoin
         *      |            ->            /         \
         *  bottomJoin            newBottomSemiJoin   B
         *   /    \                    /      \
         *  A      B                  A        C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))  // t1.id = t2.id
                .project(ImmutableList.of(0))
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))  // t1.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyTopDown(new TransposeSemiJoinLogicalJoinProject())
                .matchesFromRoot(
                        logicalProject(
                                innerLogicalJoin(
                                        leftSemiLogicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        ),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                )
                        )
                );
    }

    @Test
    void pushRight() {
        /*-
         *     topSemiJoin                  project
         *       /     \                       |
         *    project   C                  newTopJoin
         *       |                        /         \
         *  bottomJoin  C     -->       A     newBottomSemiJoin
         *    /    \                              /      \
         *   A      B                             B       C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .project(ImmutableList.of(0, 2)) // t1.id, t2.id
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(1, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyTopDown(new TransposeSemiJoinLogicalJoinProject())
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        leftSemiLogicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        )
                                )
                        )
                );
    }
}
