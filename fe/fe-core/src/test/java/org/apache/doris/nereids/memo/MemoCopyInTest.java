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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoCopyInTest implements PatternMatchSupported {
    /**
     * Original:
     * Group 0: UnboundRelation C
     * Group 1: UnboundRelation B
     * Group 2: UnboundRelation A
     * Group 3: Join(Group 1, Group 2)
     * Group 4: Join(Group 0, Group 3)
     * Group 5: Filter(Group 4)
     *
     * Then:
     * Copy In Join(Group 2, Group 1) into Group 3
     *
     * Expected:
     * Group 0: UnboundRelation C
     * Group 1: UnboundRelation B
     * Group 2: UnboundRelation A
     * Group 3: Join(Group 1, Group 2), Join(Group 1, Group 2)
     * Group 4: Join(Group 0, Group 3)
     * Group 5: Filter(Group 4)
     */
    @Test
    public void testMergeGroup() {
        UnboundRelation unboundRelationA = new UnboundRelation(Lists.newArrayList("A"));
        UnboundRelation unboundRelationB = new UnboundRelation(Lists.newArrayList("B"));
        UnboundRelation unboundRelationC = new UnboundRelation(Lists.newArrayList("C"));
        LogicalJoin logicalJoinBA = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationB, unboundRelationA);
        LogicalJoin logicalJoinCBA = new LogicalJoin<>(JoinType.INNER_JOIN, unboundRelationC, logicalJoinBA);
        LogicalFilter logicalFilter = new LogicalFilter<>(new BooleanLiteral(true), logicalJoinCBA);

        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalFilter)
                .checkGroupNum(6)
                .transform(
                        // swap join's children
                        logicalJoin(unboundRelation(), unboundRelation()).then(joinBA ->
                            new LogicalJoin<>(JoinType.INNER_JOIN, joinBA.right(), joinBA.left())
                ))
                .checkGroupNum(6)
                .checkGroupExpressionNum(7)
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                    GroupExpression filter = root.getLogicalExpression();
                    GroupExpression joinCBA = filter.child(0).getLogicalExpression();
                    Assertions.assertEquals(1, joinCBA.child(0).getLogicalExpressions().size());
                    Assertions.assertEquals(2, joinCBA.child(1).getLogicalExpressions().size());
                    GroupExpression joinBA = joinCBA.child(1).getLogicalExpressions().get(0);
                    GroupExpression joinAB = joinCBA.child(1).getLogicalExpressions().get(1);
                    Assertions.assertTrue(joinAB.getPlan() instanceof LogicalJoin);
                    Assertions.assertTrue(joinBA.getPlan() instanceof LogicalJoin);
                });

    }
}
