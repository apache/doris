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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import com.google.common.base.Supplier;
import org.junit.jupiter.api.Assertions;

public class MatchingUtils {

    public static void assertMatches(Plan plan, PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = new Memo(null, plan);
        if (plan instanceof PhysicalPlan) {
            assertMatches(memo, () -> new GroupExpressionMatching(patternDesc.pattern,
                    memo.getRoot().getPhysicalExpressions().get(0)).iterator().hasNext(),
                    () -> plan.treeString());
        } else if (plan instanceof LogicalPlan) {
            assertMatches(memo, () -> new GroupExpressionMatching(patternDesc.pattern,
                    memo.getRoot().getLogicalExpression()).iterator().hasNext(),
                    () -> plan.treeString());
        } else {
            throw new IllegalStateException("Input plan should be LogicalPlan or PhysicalPlan, but meet " + plan);
        }
    }

    private static void assertMatches(Memo memo, Supplier<Boolean> asserter, Supplier<String> planString) {
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan:\n" + planString.get() + "\n"
        );
    }

    public static boolean topDownFindMatching(Group group, Pattern<? extends Plan> pattern) {
        for (GroupExpression logicalExpr : group.getLogicalExpressions()) {
            if (topDownFindMatch(logicalExpr, pattern)) {
                return true;
            }
        }

        for (GroupExpression physicalExpr : group.getPhysicalExpressions()) {
            if (topDownFindMatch(physicalExpr, pattern)) {
                return true;
            }
        }
        return false;
    }

    public static boolean topDownFindMatch(GroupExpression groupExpression, Pattern<? extends Plan> pattern) {
        GroupExpressionMatching matchingResult = new GroupExpressionMatching(pattern, groupExpression);
        if (matchingResult.iterator().hasNext()) {
            return true;
        } else {
            for (Group childGroup : groupExpression.children()) {
                boolean checkResult = topDownFindMatching(childGroup, pattern);
                if (checkResult) {
                    return true;
                }
            }
        }
        return false;
    }
}
