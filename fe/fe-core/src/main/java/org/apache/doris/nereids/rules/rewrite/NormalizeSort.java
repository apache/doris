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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * SortNode on BE always output order keys because BE needs them to do merge sort. So we normalize LogicalSort as BE
 * expected to materialize order key before sort by bottom project and then prune the useless column after sort by
 * top project.
 */
public class NormalizeSort extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSort().whenNot(this::allOrderKeyIsSlot)
                .then(sort -> {
                    List<NamedExpression> newProjects = Lists.newArrayList();

                    Builder<OrderKey> newOrderKeys =
                            ImmutableList.builderWithExpectedSize(sort.getOrderKeys().size());
                    for (OrderKey orderKey : sort.getOrderKeys()) {
                        Expression expr = orderKey.getExpr();
                        if (!(expr instanceof Slot)) {
                            Alias alias = new Alias(expr);
                            newProjects.add(alias);
                            expr = alias.toSlot();
                            newOrderKeys.add(orderKey.withExpression(expr));
                        } else {
                            newOrderKeys.add(orderKey);
                        }
                    }

                    List<Slot> childOutput = sort.child().getOutput();
                    List<NamedExpression> bottomProjections = ImmutableList.<NamedExpression>builderWithExpectedSize(
                            childOutput.size() + newProjects.size())
                            .addAll(childOutput)
                            .addAll(newProjects)
                            .build();

                    List<NamedExpression> topProjections = (List) sort.getOutput();
                    return new LogicalProject<>(topProjections, sort.withOrderKeysAndChild(
                            newOrderKeys.build(),
                            new LogicalProject<>(bottomProjections, sort.child())));
                }).toRule(RuleType.NORMALIZE_SORT);
    }

    private boolean allOrderKeyIsSlot(LogicalSort<Plan> sort) {
        for (OrderKey orderKey : sort.getOrderKeys()) {
            if (!(orderKey.getExpr() instanceof Slot)) {
                return false;
            }
        }
        return true;
    }
}
