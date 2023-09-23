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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * this rule push down the project through union to let MergeUnion could do better
 * TODO: this rule maybe lead to unequal transformation if cast is not monomorphism,
 *   maybe we need to distinguish implicit cast and explicit cast
 */
public class PushProjectThroughUnion extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalSetOperation())
                .when(project -> project.getProjects().size() == project.child().getOutput().size()
                        && project.getProjects().stream().allMatch(e -> {
                            if (e instanceof SlotReference) {
                                return true;
                            } else {
                                Expression expr = ExpressionUtils.getExpressionCoveredByCast(e.child(0));
                                return expr instanceof SlotReference;
                            }
                        }
                ))
                .then(project -> {
                    LogicalSetOperation union = project.child();
                    ImmutableList.Builder<Plan> newChildren = ImmutableList.builder();
                    ImmutableList.Builder<List<SlotReference>> newRegularChildrenOutput = ImmutableList.builder();
                    for (int i = 0; i < union.arity(); i++) {
                        Plan child = union.child(i);
                        Map<Expression, Expression> replaceMap = Maps.newHashMap();
                        for (int j = 0; j < union.getOutput().size(); j++) {
                            replaceMap.put(union.getOutput().get(j), union.getRegularChildOutput(i).get(j));
                        }
                        List<NamedExpression> childProjections = project.getProjects().stream()
                                .map(e -> (NamedExpression) ExpressionUtils.replace(e, replaceMap))
                                .map(e -> {
                                    if (e instanceof Alias) {
                                        return new Alias(((Alias) e).child(), e.getName());
                                    }
                                    return e;
                                })
                                .collect(ImmutableList.toImmutableList());
                        Plan newChild = new LogicalProject<>(childProjections, child);
                        newChildren.add(newChild);
                        newRegularChildrenOutput.add(childProjections.stream()
                                .map(NamedExpression::toSlot)
                                .map(SlotReference.class::cast)
                                .collect(ImmutableList.toImmutableList()));
                    }
                    List<NamedExpression> newOutput = (List) project.getOutput();
                    return union.withNewOutputs(newOutput)
                            .withChildrenAndTheirOutputs(newChildren.build(), newRegularChildrenOutput.build());
                })
                .toRule(RuleType.PUSH_PROJECT_THROUGH_UNION);
    }
}
