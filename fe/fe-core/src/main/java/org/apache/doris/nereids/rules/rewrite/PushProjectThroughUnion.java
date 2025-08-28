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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.ProjectProcessor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
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
                .when(project -> canPushProject(project.getProjects(), project.child()))
                .then(project -> doPushProject(project.getProjects(), project.child()))
                .toRule(RuleType.PUSH_PROJECT_THROUGH_UNION);
    }

    /** canPushProject */
    public static boolean canPushProject(List<NamedExpression> projects, LogicalSetOperation logicalSetOperation) {
        return projects.size() == logicalSetOperation.getOutput().size() && projects.stream().allMatch(e -> {
            if (e instanceof SlotReference) {
                return true;
            } else {
                Expression expr = ExpressionUtils.getExpressionCoveredByCast(e.child(0));
                return expr instanceof SlotReference;
            }
        });
    }

    /** doPushProject */
    public static Plan doPushProject(List<NamedExpression> projects, LogicalSetOperation union) {
        Builder<Plan> newChildren = ImmutableList.builder();
        Builder<List<SlotReference>> newRegularChildrenOutput = ImmutableList.builder();
        ImmutableList.Builder<List<NamedExpression>> newConstantListBuilder = ImmutableList.builder();
        if (union instanceof LogicalUnion) {
            List<List<NamedExpression>> constantExprsList = ((LogicalUnion) union).getConstantExprsList();
            for (List<NamedExpression> oneRowProject : constantExprsList) {
                Map<Expression, Expression> replaceMap = Maps.newHashMap();
                for (int j = 0; j < union.getOutput().size(); j++) {
                    replaceMap.put(union.getOutput().get(j), oneRowProject.get(j));
                }
                ImmutableList.Builder<NamedExpression> newOneRowProject = ImmutableList.builder();
                for (NamedExpression outerProject : projects) {
                    if (outerProject instanceof Slot) {
                        newOneRowProject.add((NamedExpression) replaceMap.getOrDefault(outerProject, outerProject));
                    } else {
                        Expression replacedOutput = outerProject.rewriteUp(e -> {
                            Expression mappingExpr = replaceMap.get(e);
                            return mappingExpr == null ? e : /* remove alias */ mappingExpr.child(0);
                        });
                        newOneRowProject.add((NamedExpression) replacedOutput);
                    }
                }
                newConstantListBuilder.add(newOneRowProject.build());
            }
        }

        List<List<NamedExpression>> newConstantList = newConstantListBuilder.build();

        int projectNum = projects.size();
        for (int i = 0; i < union.arity(); i++) {
            Plan child = union.child(i);
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            for (int j = 0; j < union.getOutput().size(); j++) {
                replaceMap.put(union.getOutput().get(j), union.getRegularChildOutput(i).get(j));
            }
            ImmutableList.Builder<NamedExpression> childProjections
                    = ImmutableList.builderWithExpectedSize(projectNum);
            ImmutableList.Builder<SlotReference> childOutputSlots
                    = ImmutableList.builderWithExpectedSize(projectNum);
            for (NamedExpression selectItem : projects) {
                NamedExpression newSelectItem
                        = ExpressionUtils.replaceNameExpression(selectItem, replaceMap);
                NamedExpression childProject = newSelectItem;
                if (newSelectItem instanceof Alias) {
                    childProject = new Alias(((Alias) newSelectItem).child(), newSelectItem.getName());
                }
                childProjections.add(childProject);
                childOutputSlots.add((SlotReference) childProject.toSlot());
            }
            Plan newChild = ProjectProcessor.tryProcessProject(childProjections.build(), child)
                    .orElseGet(() -> new LogicalProject<>(childProjections.build(), child));
            newChildren.add(newChild);
            newRegularChildrenOutput.add(childOutputSlots.build());
        }

        ImmutableList.Builder<NamedExpression> newOutput = ImmutableList.builderWithExpectedSize(projectNum);
        for (NamedExpression project : projects) {
            newOutput.add(project.toSlot());
        }
        LogicalSetOperation logicalSetOperation = union.withNewOutputs(newOutput.build())
                .withChildrenAndTheirOutputs(newChildren.build(), newRegularChildrenOutput.build());
        if (!newConstantList.isEmpty()) {
            return ((LogicalUnion) logicalSetOperation).withChildrenAndConstExprsList(
                    logicalSetOperation.children(), logicalSetOperation.getRegularChildrenOutputs(), newConstantList
            );
        }

        return logicalSetOperation;
    }
}
