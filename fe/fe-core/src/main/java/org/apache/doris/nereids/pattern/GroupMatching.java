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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.List;

/**
 * Get all pattern matching subtree in query plan from a group.
 */
public class GroupMatching {
    /**
     * Get all pattern matching subtree in query plan from a group.
     */
    public static List<Plan> getAllMatchingPlans(Pattern pattern, Group group) {
        List<Plan> matchingPlans = new ArrayList<>();
        if (pattern.isGroup() || pattern.isMultiGroup()) {
            GroupPlan groupPlan = new GroupPlan(group);
            if (((Pattern<Plan>) pattern).matchPredicates(groupPlan)) {
                matchingPlans.add(groupPlan);
            }
        } else {
            for (GroupExpression groupExpression : group.getLogicalExpressions()) {
                for (Plan plan : new GroupExpressionMatching(pattern, groupExpression)) {
                    matchingPlans.add(plan);
                }
            }
            for (GroupExpression groupExpression : group.getPhysicalExpressions()) {
                for (Plan plan : new GroupExpressionMatching(pattern, groupExpression)) {
                    matchingPlans.add(plan);
                }
            }
        }
        return matchingPlans;
    }
}
