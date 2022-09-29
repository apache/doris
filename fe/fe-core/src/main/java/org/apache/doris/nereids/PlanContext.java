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

package org.apache.doris.nereids;

import org.apache.doris.common.Id;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Context for plan.
 * Abstraction for group expressions and stand-alone expressions/DAGs.
 * A ExpressionHandle is attached to either {@link Plan} or {@link GroupExpression}.
 * Inspired by GPORCA-CExpressionHandle.
 */
public class PlanContext {
    // array of children's derived stats
    private final List<StatsDeriveResult> childrenStats;
    // attached group expression
    private final GroupExpression groupExpression;

    /**
     * Constructor for PlanContext.
     */
    public PlanContext(GroupExpression groupExpression) {
        this.groupExpression = groupExpression;
        childrenStats = Lists.newArrayListWithCapacity(groupExpression.children().size());

        for (Group group : groupExpression.children()) {
            childrenStats.add(group.getStatistics());
        }
    }

    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    public List<StatsDeriveResult> getChildrenStats() {
        return childrenStats;
    }

    public StatsDeriveResult getStatisticsWithCheck() {
        StatsDeriveResult statistics = groupExpression.getOwnerGroup().getStatistics();
        Preconditions.checkNotNull(statistics);
        return statistics;
    }

    public LogicalProperties childLogicalPropertyAt(int index) {
        return groupExpression.child(index).getLogicalProperties();
    }

    public List<Slot> getChildOutputSlots(int index) {
        return childLogicalPropertyAt(index).getOutput();
    }

    public List<Id> getChildOutputIds(int index) {
        return childLogicalPropertyAt(index).getOutputExprIds();
    }

    /**
     * Get child statistics.
     */
    public StatsDeriveResult getChildStatistics(int index) {
        StatsDeriveResult statistics = childrenStats.get(index);
        Preconditions.checkNotNull(statistics);
        return statistics;
    }
}
