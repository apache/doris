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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * project(A.*)
 *  - filter(B.slot is null)
 *    - LeftOuterJoin(A, B)
 * ==============================>
 * project(A.*)
 *    - LeftAntiJoin(A, B)
 */
public class ConvertOuterJoinToAntiJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalFilter(logicalJoin()
                .when(join -> join.getJoinType().isOuterJoin())))
                .then(this::toAntiJoin)
        .toRule(RuleType.CONVERT_OUTER_JOIN_TO_ANTI);
    }

    private Plan toAntiJoin(LogicalProject<LogicalFilter<LogicalJoin<Plan, Plan>>> project) {
        LogicalFilter<LogicalJoin<Plan, Plan>> filter = project.child();
        LogicalJoin<Plan, Plan> join = filter.child();

        boolean leftOutput = join.left().getOutputSet().containsAll(project.getInputSlots());
        boolean rightOutput = join.right().getOutputSet().containsAll(project.getInputSlots());

        if (!leftOutput && !rightOutput) {
            return null;
        }

        Set<Slot> alwaysNullSlots = filter.getConjuncts().stream()
                .filter(p -> TypeUtils.isNull(p).isPresent())
                .flatMap(p -> p.getInputSlots().stream())
                .collect(Collectors.toSet());
        Set<Slot> leftAlwaysNullSlots = join.left().getOutputSet().stream()
                .filter(s -> alwaysNullSlots.contains(s) && !s.nullable())
                .collect(Collectors.toSet());
        Set<Slot> rightAlwaysNullSlots = join.right().getOutputSet().stream()
                .filter(s -> alwaysNullSlots.contains(s) && !s.nullable())
                .collect(Collectors.toSet());

        Plan res = project;
        if (join.getJoinType().isLeftOuterJoin() && !rightAlwaysNullSlots.isEmpty() && leftOutput) {
            // When there is right slot always null, we can turn left outer join to left anti join
            Set<Expression> predicates = filter.getExpressions().stream()
                    .filter(p -> !(TypeUtils.isNull(p).isPresent()
                            && rightAlwaysNullSlots.containsAll(p.getInputSlots())))
                    .collect(ImmutableSet.toImmutableSet());
            boolean containRightSlot = predicates.stream()
                    .anyMatch(s -> join.right().getOutputSet().containsAll(s.getInputSlots()));
            if (!containRightSlot) {
                res = join.withJoinType(JoinType.LEFT_ANTI_JOIN);
                res = predicates.isEmpty() ? res : filter.withConjuncts(predicates).withChildren(res);
                res = project.withChildren(res);
            }
        }
        if (join.getJoinType().isRightOuterJoin() && !leftAlwaysNullSlots.isEmpty() && rightOutput) {
            Set<Expression> predicates = filter.getExpressions().stream()
                    .filter(p -> !(TypeUtils.isNull(p).isPresent()
                            && leftAlwaysNullSlots.containsAll(p.getInputSlots())))
                    .collect(ImmutableSet.toImmutableSet());
            boolean containLeftSlot = predicates.stream()
                    .anyMatch(s -> join.left().getOutputSet().containsAll(s.getInputSlots()));
            if (!containLeftSlot) {
                res = join.withJoinType(JoinType.RIGHT_ANTI_JOIN);
                res = predicates.isEmpty() ? res : filter.withConjuncts(predicates).withChildren(res);
                res = project.withChildren(res);
            }
        }
        return res;
    }
}
