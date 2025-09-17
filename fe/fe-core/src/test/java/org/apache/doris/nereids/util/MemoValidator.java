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

import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import org.junit.jupiter.api.Assertions;

import java.util.IdentityHashMap;

/**
 * check the memo whether is a valid memo.
 * 1. after init the memo, the memo must hava one logical group expressions per group, and no physical group expression.
 * 2. group/group expression should not traverse twice, this logical can detect whether the group has a ring.
 */
public class MemoValidator {

    public static MemoValidator validateInitState(Memo memo, Plan initPlan) {
        for (Group group : memo.getGroups()) {
            // every group has no physical groupExpression
            Assertions.assertEquals(0, group.getPhysicalExpressions().size());
        }
        MemoValidator validator = validate(memo);
        if (initPlan != null) {
            if (initPlan instanceof UnboundResultSink || initPlan instanceof LogicalSelectHint) {
                return validator;
            }
            Assertions.assertEquals(initPlan, memo.getRoot().getLogicalExpression().getPlan());
        }
        return validator;
    }

    /* basic validate memo init state*/
    public static MemoValidator validate(Memo memo) {
        MemoValidator memoValidator = new MemoValidator();
        Group root = memo.getRoot();
        CycleDetector cycleDetector = new CycleDetector();
        cycleDetector.hasCycle(root);
        return memoValidator;
    }

    private static final class CycleDetector {
        private final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
        private final IdentityHashMap<Object, Boolean> visiting = new IdentityHashMap<>();

        public boolean hasCycle(Group startGroup) {
            return detectCycle(startGroup);
        }

        private boolean detectCycle(Object node) {
            Assertions.assertFalse(visiting.containsKey(node),
                    "Group or group expression " + node + " already exists");
            if (visited.containsKey(node)) {
                return false;
            }
            visiting.put(node, true);
            visited.put(node, true);

            boolean hasCycle = false;
            if (node instanceof Group) {
                hasCycle = checkGroupDependencies((Group) node);
            } else if (node instanceof GroupExpression) {
                hasCycle = checkExpressionDependencies((GroupExpression) node);
            }

            visiting.remove(node);
            return hasCycle;
        }

        private boolean checkGroupDependencies(Group group) {
            for (GroupExpression expr : group.getLogicalExpressions()) {
                if (detectCycle(expr)) {
                    return true;
                }
            }
            return false;
        }

        private boolean checkExpressionDependencies(GroupExpression expr) {
            for (Group childGroup : expr.children()) {
                if (detectCycle(childGroup)) {
                    return true;
                }
            }
            return false;
        }
    }
}
