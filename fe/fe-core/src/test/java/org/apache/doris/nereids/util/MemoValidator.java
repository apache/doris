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
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;

import java.util.IdentityHashMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 * check the memo whether is a valid memo.
 * 1. after init the memo, the memo must hava one logical group expressions per group, and no physical group expression.
 * 2. group/group expression should not traverse twice, this logical can detect whether the group has a ring.
 */
public class MemoValidator {
    public final Set<GroupId> visitedGroupIds = Sets.newLinkedHashSet();
    public final IdentityHashMap<Group, Void> visitedGroups = new IdentityHashMap<>();
    public final IdentityHashMap<GroupExpression, Void> visitedExpressions = new IdentityHashMap<>();

    public static MemoValidator validateInitState(Memo memo, Plan initPlan) {
        Assertions.assertEquals(memo.getGroups().size(), memo.getGroupExpressions().size());

        for (Group group : memo.getGroups()) {
            // every group has one logical groupExpression and no physical groupExpression
            Assertions.assertEquals(1, group.getLogicalExpressions().size());
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

    /* basic validate */
    public static MemoValidator validate(Memo memo) {
        MemoValidator memoValidator = new MemoValidator();

        Group root = memo.getRoot();
        memoValidator.validate(root);

        for (Group group : memo.getGroups()) {
            Assertions.assertTrue(memoValidator.visitedGroups.containsKey(group),
                    "Exist unreachable group: " + group.getGroupId());
        }

        for (Entry<GroupExpression, GroupExpression> entry : memo.getGroupExpressions().entrySet()) {
            Assertions.assertTrue(memoValidator.visitedExpressions.containsKey(entry.getKey()),
                    "Exist unreachable groupExpression: " + entry.getKey() + ", groupId: "
                            + entry.getKey().getOwnerGroup().getGroupId());
        }

        return memoValidator;
    }

    private void validate(Group group) {
        GroupId groupId = group.getGroupId();
        Assertions.assertFalse(visitedGroupIds.contains(groupId),
                "GroupId " + groupId + " already exists, group tree has a ring");
        visitedGroupIds.add(groupId);

        Assertions.assertFalse(visitedGroups.containsKey(group), "Group " + group
                + " already exists, group tree has a ring");
        visitedGroups.put(group, null);

        for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
            Assertions.assertEquals(group, logicalExpression.getOwnerGroup());
            validate(logicalExpression);
        }

        for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
            Assertions.assertEquals(group, physicalExpression.getOwnerGroup());
            validate(physicalExpression);
        }
    }

    private void validate(GroupExpression groupExpression) {
        Assertions.assertFalse(visitedExpressions.containsKey(groupExpression),
                "GroupExpression " + groupExpression + " already exists");
        visitedExpressions.put(groupExpression, null);

        for (Group child : groupExpression.children()) {
            validate(child);
        }
    }
}
