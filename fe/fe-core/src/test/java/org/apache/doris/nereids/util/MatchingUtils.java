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
import org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import com.google.common.base.Supplier;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

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
        GroupExpressionIterator iterator;
        try {
            iterator = matchingResult.iterator();
        } catch (Throwable throwable) {
            // if assert in pattern should return false
            return false;
        }
        if (iterator.hasNext()) {
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

    /**
     * Diagnostic version: find the best partial match in the memo and return
     * a description of where/why the match failed. Returns null if match succeeds.
     */
    public static String topDownFindMatchingDiagnostic(Group group, Pattern<? extends Plan> pattern) {
        // First try to match from root - this is the most common case in tests.
        // We run the diagnostic on the copied-out plan from each root group expression.
        String bestDiagnostic = null;

        for (GroupExpression logicalExpr : group.getLogicalExpressions()) {
            Plan copiedPlan = copyOutFromGroupExpression(logicalExpr);
            String diagnostic = ((Pattern<Plan>) pattern).matchPlanTreeDiagnostic(copiedPlan, "root");
            if (diagnostic == null) {
                return null; // match found
            }
            if (bestDiagnostic == null) {
                bestDiagnostic = diagnostic;
            }
        }

        for (GroupExpression physicalExpr : group.getPhysicalExpressions()) {
            Plan copiedPlan = copyOutFromGroupExpression(physicalExpr);
            String diagnostic = ((Pattern<Plan>) pattern).matchPlanTreeDiagnostic(copiedPlan, "root");
            if (diagnostic == null) {
                return null;
            }
            if (bestDiagnostic == null) {
                bestDiagnostic = diagnostic;
            }
        }

        return bestDiagnostic != null ? bestDiagnostic : "no group expressions found in root group";
    }

    private static Plan copyOutFromGroupExpression(GroupExpression groupExpression) {
        Plan plan = groupExpression.getPlan();
        List<Plan> children = new ArrayList<>();
        for (Group childGroup : groupExpression.children()) {
            // pick first logical expression, fallback to first physical
            if (!childGroup.getLogicalExpressions().isEmpty()) {
                children.add(copyOutFromGroupExpression(childGroup.getLogicalExpressions().get(0)));
            } else if (!childGroup.getPhysicalExpressions().isEmpty()) {
                children.add(copyOutFromGroupExpression(childGroup.getPhysicalExpressions().get(0)));
            }
        }
        if (!children.isEmpty()) {
            return plan.withChildren(children);
        }
        return plan;
    }
}
