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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.TypeUtils;

import java.util.List;
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
        return logicalFilter(logicalJoin()
                .when(join -> join.getJoinType().isOuterJoin()))
                .then(this::toAntiJoin)
        .toRule(RuleType.CONVERT_OUTER_JOIN_TO_ANTI);
    }

    private Plan toAntiJoin(LogicalFilter<LogicalJoin<Plan, Plan>> filter) {
        LogicalJoin<Plan, Plan> join = filter.child();

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

        Plan newJoin = null;
        if (join.getJoinType().isLeftOuterJoin() && !rightAlwaysNullSlots.isEmpty()) {
            newJoin = join.withJoinTypeAndContext(JoinType.LEFT_ANTI_JOIN, join.getJoinReorderContext());
        }
        if (join.getJoinType().isRightOuterJoin() && !leftAlwaysNullSlots.isEmpty()) {
            newJoin = join.withJoinTypeAndContext(JoinType.RIGHT_ANTI_JOIN, join.getJoinReorderContext());
        }
        if (newJoin == null) {
            return null;
        }

        if (!newJoin.getOutputSet().containsAll(filter.getInputSlots())) {
            // if there are slots that don't belong to join output, we use null alias to replace them
            // such as:
            //   project(A.id, null as B.id)
            //       -  (A left anti join B)
            Set<Slot> joinOutput = newJoin.getOutputSet();
            List<NamedExpression> projects = filter.getOutput().stream()
                    .map(s -> {
                        if (joinOutput.contains(s)) {
                            return s;
                        } else {
                            return new Alias(s.getExprId(), new NullLiteral(s.getDataType()), s.getName());
                        }
                    }).collect(Collectors.toList());
            newJoin = new LogicalProject<>(projects, newJoin);
        }
        return filter.withChildren(newJoin);
    }
}
