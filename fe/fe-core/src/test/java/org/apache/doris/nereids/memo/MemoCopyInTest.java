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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoCopyInTest implements PatternMatchSupported {
    LogicalJoin<LogicalOlapScan, LogicalOlapScan> logicalJoinAB = new LogicalJoin<>(JoinType.INNER_JOIN,
            PlanConstructor.newLogicalOlapScan(0, "A", 0), PlanConstructor.newLogicalOlapScan(1, "B", 0));
    LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> logicalJoinABC = new LogicalJoin<>(
            JoinType.INNER_JOIN, logicalJoinAB, PlanConstructor.newLogicalOlapScan(2, "C", 0));

    /**
     * Original:
     * Group 0: LogicalOlapScan C
     * Group 1: LogicalOlapScan B
     * Group 2: LogicalOlapScan A
     * Group 3: Join(Group 1, Group 2)
     * Group 4: Join(Group 0, Group 3)
     * <p>
     * Then:
     * Copy In Join(Group 2, Group 1) into Group 3
     * <p>
     * Expected:
     * Group 0: LogicalOlapScan C
     * Group 1: LogicalOlapScan B
     * Group 2: LogicalOlapScan A
     * Group 3: Join(Group 1, Group 2), Join(Group 2, Group 1)
     * Group 4: Join(Group 0, Group 3)
     */
    @Test
    public void testInsertSameGroup() {
        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalJoinABC)
                .transform(
                        // swap join's children
                        logicalJoin(logicalOlapScan(), logicalOlapScan()).then(joinBA ->
                                new LogicalProject<>(Lists.newArrayList(joinBA.getOutput()),
                                        new LogicalJoin<>(JoinType.INNER_JOIN, joinBA.right(), joinBA.left()))
                        ))
                .checkGroupNum(6)
                .checkGroupExpressionNum(7)
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                    GroupExpression joinABC = root.getLogicalExpression();
                    Assertions.assertEquals(2, joinABC.child(0).getLogicalExpressions().size());
                    Assertions.assertEquals(1, joinABC.child(1).getLogicalExpressions().size());
                    GroupExpression joinAB = joinABC.child(0).getLogicalExpressions().get(0);
                    GroupExpression project = joinABC.child(0).getLogicalExpressions().get(1);
                    GroupExpression joinBA = project.child(0).getLogicalExpression();
                    Assertions.assertTrue(joinAB.getPlan() instanceof LogicalJoin);
                    Assertions.assertTrue(joinBA.getPlan() instanceof LogicalJoin);
                });

    }

    // TODO: test mergeGroup().
}
