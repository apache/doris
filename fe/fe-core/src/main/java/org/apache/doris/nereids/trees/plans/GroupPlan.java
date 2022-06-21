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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.plans.logical.GroupPlanOperator;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.statistics.ExprStats;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.List;
import java.util.Optional;

/**
 * A virtual node that represents a sequence plan in a Group.
 * Used in {@link org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator},
 * as a place-holder when do match root.
 */
public class GroupPlan extends LogicalLeafPlan<GroupPlanOperator> {
    private final Group group;

    public GroupPlan(Group group) {
        super(new GroupPlanOperator(), Optional.empty(), Optional.of(group.getLogicalProperties()));
        this.group = group;
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    @Override
    public NodeType getType() {
        return NodeType.GROUP;
    }

    public Group getGroup() {
        return group;
    }

    @Override
    public GroupPlan withOutput(List<Slot> output) {
        throw new IllegalStateException("GroupPlan can not invoke withOutput()");
    }

    @Override
    public GroupPlan withChildren(List<Plan> children) {
        throw new IllegalStateException("GroupPlan can not invoke withChildren()");
    }

    @Override
    public List<StatsDeriveResult> getChildrenStats() {
        throw new RuntimeException("GroupPlan can not invoke getChildrenStats()");
    }

    @Override
    public StatsDeriveResult getStatsDeriveResult() {
        throw new RuntimeException("GroupPlan can not invoke getStatsDeriveResult()");
    }

    @Override
    public StatisticalType getStatisticalType() {
        throw new RuntimeException("GroupPlan can not invoke getStatisticalType()");
    }

    @Override
    public void setStatsDeriveResult(StatsDeriveResult result) {
        throw new RuntimeException("GroupPlan can not invoke setStatsDeriveResult()");
    }

    @Override
    public long getLimit() {
        throw new RuntimeException("GroupPlan can not invoke getLimit()");
    }

    @Override
    public List<? extends ExprStats> getConjuncts() {
        throw new RuntimeException("GroupPlan can not invoke getConjuncts()");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new RuntimeException("GroupPlan can not invoke withGroupExpression()");
    }
}
