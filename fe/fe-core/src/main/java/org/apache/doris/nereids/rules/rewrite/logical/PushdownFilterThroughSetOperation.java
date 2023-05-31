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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convert the expression in the filter into the output column corresponding to the child node and push it down.
 */
public class PushdownFilterThroughSetOperation extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalSetOperation())
                .when(f -> f.child().getQualifier() == Qualifier.ALL)
                .then(filter -> {
                    LogicalSetOperation setOperation = filter.child();

                    List<Plan> newChildren = new ArrayList<>();
                    boolean allOneRowRelation = true;
                    boolean hasOneRowRelation = false;
                    for (Plan child : setOperation.children()) {
                        if (child instanceof OneRowRelation) {
                            // We shouldn't push down the 'filter' to 'oneRowRelation'.
                            hasOneRowRelation = true;
                            newChildren.add(child);
                            continue;
                        } else {
                            allOneRowRelation = false;
                        }
                        Map<Expression, Expression> replaceMap = new HashMap<>();
                        for (int i = 0; i < setOperation.getOutputs().size(); ++i) {
                            NamedExpression output = setOperation.getOutputs().get(i);
                            replaceMap.put(output, child.getOutput().get(i));
                        }

                        Set<Expression> newFilterPredicates = filter.getConjuncts().stream().map(conjunct ->
                                ExpressionUtils.replace(conjunct, replaceMap)).collect(ImmutableSet.toImmutableSet());
                        newChildren.add(new LogicalFilter<>(newFilterPredicates, child));
                    }

                    if (allOneRowRelation) {
                        return filter;
                    }

                    if (hasOneRowRelation) {
                        // If there are some `OneRowRelation` exists, we need to keep the `filter`.
                        return filter.withChildren(((LogicalUnion) setOperation).withNewChildren(newChildren));
                    }
                    return setOperation.withNewChildren(newChildren);
                }).toRule(RuleType.PUSHDOWN_FILTER_THROUGH_SET_OPERATION);
    }
}
