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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.Set;

/**
 * Rewrite pattern:
 *
 * <pre>
 *   Aggregate
 *     Project
 *       InnerJoin(left, right)
 * </pre>
 *
 * into
 *
 * <pre>
 *   Aggregate
 *     Project
 *       SemiJoin(left, right)
 * </pre>
 *
 * when the project only references slots from a single join child and the
 * aggregate simply
 * forwards those slots (i.e. no real aggregation expressions). The rewritten
 * join becomes
 * LEFT_SEMI or RIGHT_SEMI depending on which side supplies all projected slots.
 */
public class AggInnerJoinToSemiJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalProject(logicalJoin()))
                .when(agg -> patternCheck(agg))
                .then(agg -> transform(agg))
                .toRule(RuleType.AGG_INNER_JOIN_TO_SEMI_JOIN);
    }

    Plan transform(LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg) {
        LogicalProject<LogicalJoin<Plan, Plan>> project = agg.child();
        LogicalJoin<Plan, Plan> join = project.child();
        Set<Slot> leftOutput = join.left().getOutputSet();
        Set<Slot> rightOutput = join.right().getOutputSet();
        Set<Slot> projectSlots = project.getInputSlots();
        LogicalJoin<Plan, Plan> newJoin;
        if (leftOutput.containsAll(projectSlots)) {
            newJoin = join.withJoinType(JoinType.LEFT_SEMI_JOIN);
        } else if (rightOutput.containsAll(projectSlots)) {
            newJoin = join.withJoinType(JoinType.RIGHT_SEMI_JOIN);
        } else {
            return null;
        }
        return agg.withChildren(project.withChildren(newJoin));
    }

    boolean patternCheck(LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg) {
        LogicalProject<LogicalJoin<Plan, Plan>> project = agg.child();
        LogicalJoin<Plan, Plan> join = project.child();
        // this is an inner join
        if (join.getJoinType() != JoinType.INNER_JOIN || join.isMarkJoin()) {
            return false;
        }
        // join only output left/right slots
        Set<Slot> leftOutput = join.left().getOutputSet();
        Set<Slot> rightOutput = join.right().getOutputSet();
        Set<Slot> projectSlots = project.getInputSlots();
        if (!leftOutput.containsAll(projectSlots) && !rightOutput.containsAll(projectSlots)) {
            return false;
        }

        // no aggregate functions
        for (Expression expr : agg.getOutputExpressions()) {
            if (!(expr instanceof Slot)) {
                return false;
            }
        }
        return true;

    }
}
