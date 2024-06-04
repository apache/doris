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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pull up Project under Limit.
 */
public class PullUpProjectUnderLimit extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalLimit(logicalProject(logicalJoin().when(j -> j.getJoinType().isLeftRightOuterOrCrossJoin()))
                .whenNot(p -> p.isAllSlots()))
                .then(limit -> {
                    LogicalProject<LogicalJoin<Plan, Plan>> project = limit.child();
                    Set<Slot> allUsedSlots = project.getProjects().stream().flatMap(ne -> ne.getInputSlots().stream())
                            .collect(Collectors.toSet());
                    Set<Slot> outputSet = project.child().getOutputSet();
                    if (outputSet.size() == allUsedSlots.size()) {
                        Preconditions.checkState(outputSet.equals(allUsedSlots));
                        return project.withChildren(limit.withChildren(project.child()));
                    } else {
                        Plan columnProject = PlanUtils.projectOrSelf(ImmutableList.copyOf(allUsedSlots),
                                project.child());
                        return project.withChildren(limit.withChildren(columnProject));
                    }
                }).toRule(RuleType.PULL_UP_PROJECT_UNDER_LIMIT);
    }
}
