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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pushdown Alias (inside must be Slot) into UnionAll outputs.
 * <pre>
 * Project(c1, c2 as c2t)
 * |
 * UnionAll  output(c1, c2, c3)
 * ->
 * Project(c1, c2t)
 * |
 * UnionAll  output(c1, c2 as c2t, c3)
 * </pre>
 */
public class PushDownAliasIntoUnionAll extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalUnion())
                .when(project -> project.child().getQualifier() == Qualifier.ALL)
                .when(project -> project.getProjects().stream().allMatch(expr ->
                        (expr instanceof Slot) || (expr instanceof Alias && ((Alias) expr).child() instanceof Slot)))
                .when(project -> project.getProjects().stream().anyMatch(expr -> expr instanceof Alias))
                .then(project -> {
                    LogicalUnion union = project.child();
                    // aliasMap { Slot -> Alias }
                    Map<Slot, Alias> aliasMap = project.getProjects().stream()
                            .filter(namedExpression -> namedExpression instanceof Alias)
                            .map(namedExpression -> (Alias) namedExpression)
                            .collect(Collectors.toMap(
                                    alias -> (Slot) (alias.child()),
                                    alias -> alias));
                    Preconditions.checkState(!aliasMap.isEmpty(), "aliasMap should not be empty");
                    List<NamedExpression> newOutput = union.getOutputs().stream()
                            .map(ne -> {
                                Slot outSlot = ne.toSlot();
                                Alias alias = aliasMap.get(outSlot);
                                if (alias == null) {
                                    return outSlot;
                                }
                                if (ne instanceof Alias) {
                                    return alias.withChildren(ImmutableList.of(((Alias) ne).child()));
                                } else {
                                    return alias;
                                }
                            })
                            .collect(Collectors.toList());
                    List<NamedExpression> newProjects = project.getProjects().stream().map(NamedExpression::toSlot)
                            .collect(Collectors.toList());
                    return PlanUtils.projectOrSelf(newProjects, union.withNewOutputs(newOutput));
                }).toRule(RuleType.PUSH_DOWN_ALIAS_INTO_UNION_ALL);
    }
}
