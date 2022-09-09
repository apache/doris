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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * prune join children output.
 * pattern: project(join())
 * table a: k1,k2,k3,v1
 * table b: k1,k2,v1,v2
 * select a.k1,b.k2 from a join b on a.k1 = b.k1 where a.k3 > 1
 * plan tree:
 * project(a.k1,b.k2)
 * |
 * join(k1,k2,k3,v1,k1,k2,v1,v2)
 * /       \
 * scan(a) scan(b)
 * transformed:
 * project(a.k1,b.k2)
 * |
 * join(k1,k2,k3,v1,k1,k2,v1,v2)
 * /                     \
 * project(a.k1,a.k3)   project(b.k2,b.k1)
 * |                     |
 * scan                scan
 */
public class PruneJoinChildrenColumns
        extends AbstractPushDownProjectRule<LogicalJoin<GroupPlan, GroupPlan>> {

    public PruneJoinChildrenColumns() {
        setRuleType(RuleType.COLUMN_PRUNE_JOIN_CHILD);
        setTarget(logicalJoin());
    }

    @Override
    protected Plan pushDownProject(LogicalJoin<GroupPlan, GroupPlan> joinPlan,
            Set<Slot> references) {

        Set<ExprId> exprIds = Stream.of(references, joinPlan.getInputSlots())
                .flatMap(Set::stream)
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());

        List<NamedExpression> leftInputs = joinPlan.left().getOutput().stream()
                .filter(r -> exprIds.contains(r.getExprId())).collect(Collectors.toList());
        List<NamedExpression> rightInputs = joinPlan.right().getOutput().stream()
                .filter(r -> exprIds.contains(r.getExprId())).collect(Collectors.toList());

        if (leftInputs.isEmpty()) {
            leftInputs.add(ExpressionUtils.selectMinimumColumn(joinPlan.left().getOutput()));
        }
        if (rightInputs.isEmpty()) {
            rightInputs.add(ExpressionUtils.selectMinimumColumn(joinPlan.right().getOutput()));
        }

        Plan leftPlan = joinPlan.left();
        Plan rightPlan = joinPlan.right();

        if (leftInputs.size() != leftPlan.getOutput().size()) {
            leftPlan = new LogicalProject<>(leftInputs, leftPlan);
        }

        if (rightInputs.size() != rightPlan.getOutput().size()) {
            rightPlan = new LogicalProject<>(rightInputs, rightPlan);
        }
        return joinPlan.withChildren(ImmutableList.of(leftPlan, rightPlan));
    }
}
