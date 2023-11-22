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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * A virtual node that represents a sequence plan in a Group.
 * Used in {@link org.apache.doris.nereids.pattern.GroupExpressionMatching.GroupExpressionIterator},
 * as a place-holder when do match root.
 */
public class GroupPlan extends LogicalLeaf {

    private final Group group;

    public GroupPlan(Group group) {
        super(PlanType.GROUP_PLAN, Optional.empty(), Suppliers.ofInstance(group.getLogicalProperties()), true);
        this.group = group;
    }

    @Override
    public Optional<GroupExpression> getGroupExpression() {
        return Optional.empty();
    }

    public Group getGroup() {
        return group;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Statistics getStats() {
        throw new IllegalStateException("GroupPlan can not invoke getStats()");
    }

    @Override
    public GroupPlan withChildren(List<Plan> children) {
        throw new IllegalStateException("GroupPlan can not invoke withChildren()");
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        throw new IllegalStateException("GroupPlan can not invoke withGroupExpression()");
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        throw new IllegalStateException("GroupPlan can not invoke withGroupExprLogicalPropChildren()");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new IllegalStateException("GroupPlan can not compute output."
                + " You should invoke GroupPlan.getOutput()");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGroupPlan(this, context);
    }

    @Override
    public String toString() {
        return "GroupPlan( " + group.getGroupId() + " )";
    }

}
