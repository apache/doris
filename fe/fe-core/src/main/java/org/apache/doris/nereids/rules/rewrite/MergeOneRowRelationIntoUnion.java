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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * merge one row relation into union, for easy to compute physical properties
 */
public class MergeOneRowRelationIntoUnion extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalUnion().when(u -> u.children().stream()
                .anyMatch(LogicalOneRowRelation.class::isInstance)).then(u -> {
                    List<List<NamedExpression>> constantExprsList = Lists.newArrayList();
                    List<Plan> newChildren = Lists.newArrayList();
                    for (Plan child : u.children()) {
                        if (!(child instanceof LogicalOneRowRelation)) {
                            newChildren.add(child);
                        } else {
                            ImmutableList.Builder<NamedExpression> constantExprs = new Builder<>();
                            List<NamedExpression> projects = ((LogicalOneRowRelation) child).getProjects();
                            for (int i = 0; i < projects.size(); i++) {
                                NamedExpression project = projects.get(i);
                                DataType targetType = u.getOutput().get(i).getDataType();
                                if (project.getDataType().equals(targetType)) {
                                    constantExprs.add(project);
                                } else {
                                    constantExprs.add((NamedExpression) project.withChildren(
                                            TypeCoercionUtils.castIfNotSameType(project.child(0), targetType)));
                                }
                            }
                            constantExprsList.add(constantExprs.build());
                        }
                    }
                    return u.withChildrenAndConstExprsList(newChildren, constantExprsList);
                }).toRule(RuleType.MERGE_ONE_ROW_RELATION_INTO_UNION);
    }
}
