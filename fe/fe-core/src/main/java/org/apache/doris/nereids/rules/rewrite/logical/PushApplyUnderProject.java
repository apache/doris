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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;

/**
 * Adjust the order of Project and apply in correlated subqueries.
 *
 * before:
 *              apply
 *         /              \
 * Input(output:b)    Project(output:a)
 *                         |
 *                       child
 *
 * after:
 *          Project(b,(if the Subquery is Scalar add 'a' as the output column))
 *                  |
 *                apply
 *          /               \
 * Input(output:b)          child
 */
public class PushApplyUnderProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(group(), logicalProject(any()))
                .when(LogicalApply::isCorrelated)
                .whenNot(apply -> apply.right().child() instanceof LogicalFilter && apply.isIn())
                .whenNot(LogicalApply::alreadyExecutedEliminateFilter)
                .then(apply -> {
                    LogicalProject<Plan> project = apply.right();
                    LogicalApply newCorrelate = new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryExpr(),
                            apply.getCorrelationFilter(), apply.left(), project.child());
                    List<Slot> newSlots = new ArrayList<>();
                    newSlots.addAll(apply.left().getOutput());
                    if (apply.getSubqueryExpr() instanceof ScalarSubquery) {
                        newSlots.add(apply.right().getOutput().get(0));
                    }
                    return new LogicalProject(newSlots, newCorrelate);
                }).toRule(RuleType.PUSH_APPLY_UNDER_PROJECT);
    }
}
