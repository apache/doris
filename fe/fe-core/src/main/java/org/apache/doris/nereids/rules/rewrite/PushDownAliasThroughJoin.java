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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pushdown Alias (inside must be Slot) through Join.
 */
public class PushDownAliasThroughJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
            .when(project -> project.getProjects().stream().allMatch(expr ->
                (expr instanceof Slot && !(expr instanceof MarkJoinSlotReference))
                        || (expr instanceof Alias && ((Alias) expr).child() instanceof Slot
                                && !(((Alias) expr).child() instanceof MarkJoinSlotReference))))
            .when(project -> project.getProjects().stream().anyMatch(expr -> expr instanceof Alias))
            .then(project -> {
                LogicalJoin<? extends Plan, ? extends Plan> join = project.child();
                // aliasMap { Slot -> List<Alias<Slot>> }
                Map<Expression, List<NamedExpression>> aliasMap = Maps.newHashMap();
                project.getProjects().stream()
                        .filter(expr -> expr instanceof Alias && ((Alias) expr).child() instanceof Slot)
                        .forEach(expr -> {
                            List<NamedExpression> aliases = aliasMap.get(((Alias) expr).child());
                            if (aliases == null) {
                                aliases = Lists.newArrayList();
                                aliasMap.put(((Alias) expr).child(), aliases);
                            }
                            aliases.add(expr);
                        });
                Preconditions.checkState(!aliasMap.isEmpty(), "aliasMap should not be empty");
                List<NamedExpression> newProjects = project.getProjects().stream().map(NamedExpression::toSlot)
                        .collect(Collectors.toList());

                List<Slot> leftOutput = join.left().getOutput();
                List<NamedExpression> leftProjects = createNewOutput(leftOutput, aliasMap);
                List<Slot> rightOutput = join.right().getOutput();
                List<NamedExpression> rightProjects = createNewOutput(rightOutput, aliasMap);

                Plan left;
                Plan right;
                if (leftOutput.equals(leftProjects)) {
                    left = join.left();
                } else {
                    left = project.withProjectsAndChild(leftProjects, join.left());
                }
                if (rightOutput.equals(rightProjects)) {
                    right = join.right();
                } else {
                    right = project.withProjectsAndChild(rightProjects, join.right());
                }

                // If condition use alias slot, we should replace condition
                // project a.id as aid -- join a.id = b.id  =>
                // join aid = b.id -- project a.id as aid
                Map<ExprId, Slot> replaceMap = aliasMap.entrySet().stream().collect(
                        Collectors.toMap(entry -> ((Slot) entry.getKey()).getExprId(),
                                entry -> entry.getValue().get(0).toSlot()));

                List<Expression> newHash = replaceJoinConjuncts(join.getHashJoinConjuncts(), replaceMap);
                List<Expression> newOther = replaceJoinConjuncts(join.getOtherJoinConjuncts(), replaceMap);
                List<Expression> newMark = replaceJoinConjuncts(join.getMarkJoinConjuncts(), replaceMap);

                Plan newJoin = join.withConjunctsChildren(newHash, newOther, newMark, left, right,
                            join.getJoinReorderContext());
                return project.withProjectsAndChild(newProjects, newJoin);
            }).toRule(RuleType.PUSH_DOWN_ALIAS_THROUGH_JOIN);
    }

    private List<Expression> replaceJoinConjuncts(List<Expression> joinConjuncts, Map<ExprId, Slot> replaceMaps) {
        return joinConjuncts.stream().map(expr -> expr.rewriteUp(e -> {
            if (e instanceof Slot && replaceMaps.containsKey(((Slot) e).getExprId())) {
                return replaceMaps.get(((Slot) e).getExprId());
            } else {
                return e;
            }
        })).collect(ImmutableList.toImmutableList());
    }

    private List<NamedExpression> createNewOutput(List<Slot> oldOutput,
                                                  Map<Expression, List<NamedExpression>> aliasMap) {
        // we should keep all original outputs and add new alias in the output list
        // because the upper node may require both col#1 and col#1 as colAlias#2
        List<NamedExpression> output = Stream.concat(oldOutput.stream(), oldOutput.stream()
                        .flatMap(slot -> aliasMap.getOrDefault(slot, Collections.emptyList()).stream()))
                .collect(Collectors.toList());
        return output;
    }
}
