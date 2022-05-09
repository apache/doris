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

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Representation for memo in cascades optimizer.
 */
public class Memo {
    private final List<Group> groups = Lists.newArrayList();
    private final Set<GroupExpression> groupExpressions = Sets.newHashSet();
    private Group rootSet;

    public void initialize(LogicalPlan plan) {
        rootSet = newGroupExpression(plan, null).getParent();
    }

    public Group getRootSet() {
        return rootSet;
    }

    /**
     * Add plan to Memo.
     *
     * @param plan   {@link Plan} to be added
     * @param target target group to add plan. null to generate new Group
     * @return Reference of plan in Memo
     */
    // TODO: need to merge PlanRefSet if new PlanRef is same with some one already in memo
    public GroupExpression newGroupExpression(Plan<?> plan, Group target) {
        List<GroupExpression> childGroupExpr = Lists.newArrayList();
        for (Plan<?> childrenPlan : plan.children()) {
            childGroupExpr.add(newGroupExpression(childrenPlan, null));
        }
        GroupExpression newGroupExpression = new GroupExpression(plan);
        for (GroupExpression childReference : childGroupExpr) {
            newGroupExpression.addChild(childReference.getParent());
        }

        return insertGroupExpression(newGroupExpression, target);
    }

    private GroupExpression insertGroupExpression(GroupExpression groupExpression, Group target) {
        if (groupExpressions.contains(groupExpression)) {
            return groupExpression;
        }

        groupExpressions.add(groupExpression);

        if (target != null) {
            target.addGroupExpression(groupExpression);
        } else {
            Group group = new Group(groupExpression);
            groups.add(group);
        }
        return groupExpression;
    }

    private void mergeGroup(Group group1, Group group2) {

    }
}
