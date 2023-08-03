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
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Stream;

/**
 * SortNode on BE always output order keys because BE needs them to do merge sort. So we normalize LogicalSort as BE
 * expected to materialize order key before sort by bottom project and then prune the useless column after sort by
 * top project.
 */
public class NormalizeSort extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSort().whenNot(sort -> sort.getOrderKeys().stream()
                        .map(OrderKey::getExpr).allMatch(Slot.class::isInstance))
                .then(sort -> {
                    List<NamedExpression> newProjects = Lists.newArrayList();
                    List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                            .map(orderKey -> {
                                Expression expr = orderKey.getExpr();
                                if (!(expr instanceof Slot)) {
                                    Alias alias = new Alias(expr, expr.toSql());
                                    newProjects.add(alias);
                                    expr = alias.toSlot();
                                }
                                return orderKey.withExpression(expr);
                            }).collect(ImmutableList.toImmutableList());
                    List<NamedExpression> bottomProjections = Stream.concat(
                            sort.child().getOutput().stream(),
                            newProjects.stream()
                    ).collect(ImmutableList.toImmutableList());
                    List<NamedExpression> topProjections = sort.getOutput().stream()
                            .map(NamedExpression.class::cast)
                            .collect(ImmutableList.toImmutableList());
                    return new LogicalProject<>(topProjections, sort.withOrderKeysAndChild(newOrderKeys,
                            new LogicalProject<>(bottomProjections, sort.child())));
                }).toRule(RuleType.NORMALIZE_SORT);
    }
}
