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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Infer Distinct from SetOperator;
 * Example:
 * <pre>
 *                    Intersect
 *   Intersect ->        |
 *                  Agg for Distinct
 * </pre>
 */
public class InferSetOperatorDistinct extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSetOperation()
                .when(operation -> operation.getQualifier() == Qualifier.DISTINCT)
                .when(operation -> operation.children().stream().allMatch(this::rejectNLJ))
                .then(setOperation -> {
                    if (setOperation.children().stream().anyMatch(child -> child instanceof LogicalAggregate)) {
                        return null;
                    }

                    List<Plan> newChildren = setOperation.children().stream()
                            .map(child -> isAgg(child) ? child
                                    : new LogicalAggregate<>(ImmutableList.copyOf(child.getOutput()), true, child))
                            .collect(ImmutableList.toImmutableList());
                    if (newChildren.equals(setOperation.children())) {
                        return null;
                    }
                    return setOperation.withChildren(newChildren);
                }).toRule(RuleType.INFER_SET_OPERATOR_DISTINCT);
    }

    private boolean isAgg(Plan plan) {
        return plan instanceof LogicalAggregate || (plan instanceof LogicalProject && plan.child(
                0) instanceof LogicalAggregate);
    }

    // if children exist NLJ, we can't infer distinct
    // because NLJ could generate bitmap runtime filter. and it will execute failed when we do infer distinct.
    private boolean rejectNLJ(Plan plan) {
        if (plan instanceof LogicalProject) {
            plan = plan.child(0);
        }
        if (plan instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
            return join.getOtherJoinConjuncts().isEmpty();
        }
        return true;
    }
}
