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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exchange apply and filter.
 * Keep the non correlated predicate in apply and raise the correlated predicate.
 *
 * before:
 *              apply
 *          /              \
 * Input(output:b)    Filter(Correlated predicate/UnCorrelated predicate)
 *
 * after:
 *          Filter(Correlated predicate)
 *                      |
 *                    apply
 *                /            \
 *      Input(output:b)    Filter(UnCorrelated predicate)
 */
public class UnCorrelatedApplyFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalFilter()).when(LogicalApply::isCorrelated).then(apply -> {
            LogicalFilter<Plan> filter = apply.right();
            Set<Expression> conjuncts = filter.getConjuncts();
            Map<Boolean, List<Expression>> split = Utils.splitCorrelatedConjuncts(
                    conjuncts, apply.getCorrelationSlot());
            List<Expression> correlatedPredicate = split.get(true);
            List<Expression> unCorrelatedPredicate = split.get(false);

            // the representative has experienced the rule and added the correlated predicate to the apply node
            if (correlatedPredicate.isEmpty()) {
                return apply;
            }

            Plan child = PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child());
            return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryExpr(),
                    ExpressionUtils.optionalAnd(correlatedPredicate), apply.getMarkJoinSlotReference(),
                    apply.isNeedAddSubOutputToProjects(),
                    apply.isInProject(), apply.isMarkJoinSlotNotNull(), apply.left(), child);
        }).toRule(RuleType.UN_CORRELATED_APPLY_FILTER);
    }
}
