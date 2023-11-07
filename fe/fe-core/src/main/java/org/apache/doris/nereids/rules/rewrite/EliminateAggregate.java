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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import java.util.List;

/** EliminateAggregate */
public class EliminateAggregate extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalAggregate()).then(outerAgg -> {
            LogicalAggregate<Plan> innerAgg = outerAgg.child();

            if (!isSame(outerAgg.getGroupByExpressions(), innerAgg.getGroupByExpressions())) {
                return outerAgg;
            }
            if (!onlyHasSlots(outerAgg.getOutputExpressions())) {
                return outerAgg;
            }
            List<NamedExpression> prunedInnerAggOutput = (List<NamedExpression>) Project.findProject(
                    outerAgg.getOutputSet(), innerAgg.getOutputExpressions());
            return innerAgg.withAggOutput(prunedInnerAggOutput);
        }).toRule(RuleType.ELIMINATE_AGGREGATE);
    }

    private boolean isSame(List<Expression> list1, List<Expression> list2) {
        return list1.size() == list2.size() && list2.containsAll(list1);
    }

    private boolean onlyHasSlots(List<? extends Expression> exprs) {
        return exprs.stream().allMatch(SlotReference.class::isInstance);
    }
}
