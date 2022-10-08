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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * push down expression which is not slot reference
 */
public class PushdownExpressionsInHashCondition extends OneRewriteRuleFactory {
    /*
     * rewrite example:
     *       join(t1.a + 1 = t2.b + 2)                            join(c = d)
     *             /       \                                         /    \
     *            /         \                                       /      \
     *           /           \             ====>                   /        \
     *          /             \                                   /          \
     *  olapScan(t1)     olapScan(t2)            project(t1.a + 1 as c)  project(t2.b + 2 as d)
     *                                                        |                   |
     *                                                        |                   |
     *                                                        |                   |
     *                                                        |                   |
     *                                                     olapScan(t1)        olapScan(t2)
     *TODO: now t1.a + t2.a = t1.b is not in hashJoinConjuncts. The rule will not handle it.
     */
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getHashJoinConjuncts().stream().anyMatch(equalTo ->
                        equalTo.children().stream().anyMatch(e -> !(e instanceof Slot))))
                .then(join -> {
                    List<List<Expression>> exprsOfHashConjuncts =
                            Lists.newArrayList(Lists.newArrayList(), Lists.newArrayList());
                    Map<Expression, Alias> exprMap = Maps.newHashMap();
                    join.getHashJoinConjuncts().forEach(conjunct -> {
                        Preconditions.checkArgument(conjunct instanceof EqualTo);
                        // sometimes: t1 join t2 on t2.a + 1 = t1.a + 2, so check the situation, but actually it
                        // doesn't swap the two sides.
                        conjunct = JoinUtils.swapEqualToForChildrenOrder(
                                (EqualTo) conjunct, join.left().getOutputSet());
                        exprsOfHashConjuncts.get(0).add(conjunct.child(0));
                        exprsOfHashConjuncts.get(1).add(conjunct.child(1));
                        conjunct.children().forEach(expr ->
                                exprMap.put(expr, new Alias(expr, "expr_" + expr.toSql())));
                    });
                    Iterator<List<Expression>> iter = exprsOfHashConjuncts.iterator();
                    return join.withhashJoinConjunctsAndChildren(
                            join.getHashJoinConjuncts().stream()
                                    .map(equalTo -> equalTo.withChildren(equalTo.children()
                                            .stream().map(expr -> exprMap.get(expr).toSlot())
                                            .collect(Collectors.toList())))
                                    .collect(Collectors.toList()),
                            join.children().stream().map(
                                    plan -> new LogicalProject<>(new ImmutableList.Builder<NamedExpression>()
                                            .addAll(iter.next().stream().map(expr -> exprMap.get(expr))
                                                    .collect(Collectors.toList()))
                                            .addAll(getOutput(plan, join)).build(), plan))
                                    .collect(Collectors.toList()));
                }).toRule(RuleType.PUSHDOWN_EXPRESSIONS_IN_HASH_CONDITIONS);
    }

    private List<Slot> getOutput(Plan plan, LogicalJoin join) {
        Set<Slot> intersectionSlots = Sets.newHashSet(plan.getOutputSet());
        intersectionSlots.retainAll(join.getOutputSet());
        return Lists.newArrayList(intersectionSlots);
    }
}
