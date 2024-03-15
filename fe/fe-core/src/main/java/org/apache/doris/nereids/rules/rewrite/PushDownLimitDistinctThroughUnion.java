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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <pre>
 * LIMIT-Distinct
 * -> Union All
 * -> child plan1
 * -> child plan2
 * -> child plan3
 *
 * rewritten to
 *
 * LIMIT-Distinct
 * -> Union All
 *   -> LIMIT-Distinct
 *     -> child plan1
 *   -> LIMIT-Distinct
 *     -> LIMIT plan2
 *   -> TopN-Distinct
 *     -> LIMIT plan3
 * </pre>
 */
public class PushDownLimitDistinctThroughUnion implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalLimit(logicalAggregate(logicalUnion().when(union -> union.getQualifier() == Qualifier.ALL))
                        .when(agg -> agg.isDistinct()))
                        .then(limit -> {
                            LogicalAggregate<LogicalUnion> agg = limit.child();
                            LogicalUnion union = agg.child();

                            List<Plan> newChildren = new ArrayList<>();
                            for (Plan child : union.children()) {
                                Map<Expression, Expression> replaceMap = new HashMap<>();
                                for (int i = 0; i < union.getOutputs().size(); ++i) {
                                    NamedExpression output = union.getOutputs().get(i);
                                    replaceMap.put(output, child.getOutput().get(i));
                                }

                                List<Expression> newGroupBy = agg.getGroupByExpressions().stream()
                                        .map(expr -> ExpressionUtils.replace(expr, replaceMap))
                                        .collect(Collectors.toList());
                                List<NamedExpression> newOutputs = agg.getOutputs().stream()
                                        .map(expr -> ExpressionUtils.replaceNameExpression(expr, replaceMap))
                                        .collect(Collectors.toList());

                                LogicalAggregate<Plan> newAgg = new LogicalAggregate<>(newGroupBy, newOutputs, child);
                                LogicalLimit<Plan> newLimit = limit.withLimitChild(limit.getLimit() + limit.getOffset(),
                                        0, newAgg);

                                newChildren.add(newLimit);
                            }

                            if (union.children().equals(newChildren)) {
                                return null;
                            }
                            return limit.withChildren(agg.withChildren(union.withChildren(newChildren)));
                        })
                        .toRule(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_UNION)
        );
    }
}
