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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * TopN-Distinct
 * -> Union All
 * -> child plan1
 * -> child plan2
 * -> child plan3
 *
 * rewritten to
 *
 * TopN-Distinct
 * -> Union All
 *   -> TopN-Distinct
 *     -> child plan1
 *   -> TopN-Distinct
 *     -> child plan2
 *   -> TopN-Distinct
 *     -> child plan3
 * </pre>
 */
public class PushDownTopNDistinctThroughUnion implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalAggregate(logicalUnion().when(union -> union.getQualifier() == Qualifier.ALL))
                        .when(agg -> agg.isDistinct()))
                        .then(topN -> {
                            LogicalAggregate<LogicalUnion> agg = topN.child();
                            LogicalUnion union = agg.child();
                            List<Plan> newChildren = new ArrayList<>();
                            for (Plan child : union.children()) {
                                Map<Expression, Expression> replaceMap = new HashMap<>();
                                for (int i = 0; i < union.getOutputs().size(); ++i) {
                                    NamedExpression output = union.getOutputs().get(i);
                                    replaceMap.put(output, child.getOutput().get(i));
                                }
                                List<OrderKey> orderKeys = topN.getOrderKeys().stream()
                                        .map(orderKey -> orderKey.withExpression(
                                                ExpressionUtils.replace(orderKey.getExpr(), replaceMap)))
                                        .collect(ImmutableList.toImmutableList());
                                newChildren.add(new LogicalTopN<>(orderKeys, topN.getLimit() + topN.getOffset(), 0,
                                        PlanUtils.distinct(child)));
                            }
                            if (union.children().equals(newChildren)) {
                                return null;
                            }
                            return topN.withChildren(agg.withChildren(union.withChildren(newChildren)));
                        })
                        .toRule(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_UNION)
        );
    }
}
