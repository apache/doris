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
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

class InnerJoinLeftAssociateTest implements PatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSimple() {
        /*
         * LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#4 = id#0)], otherJoinConjuncts=[] )
         * |--LogicalOlapScan ( qualified=db.t1, output=[id#4, name#5], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         * +--LogicalJoin ( type=INNER_JOIN, hashJoinConjuncts=[(id#0 = id#2)], otherJoinConjuncts=[] )
         *    |--LogicalOlapScan ( qualified=db.t2, output=[id#0, name#1], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         *    +--LogicalOlapScan ( qualified=db.t3, output=[id#2, name#3], candidateIndexIds=[], selectedIndexId=-1, preAgg=ON )
         */
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(
                        new LogicalPlanBuilder(scan2)
                                .hashJoinUsing(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 0)
                )
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLeftAssociate.INSTANCE.build())
                .printlnOrigin()
                .matchesExploration(
                        logicalJoin(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                        )
                );
    }
}
