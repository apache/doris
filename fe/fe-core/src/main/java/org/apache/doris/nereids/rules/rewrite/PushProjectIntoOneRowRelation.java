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
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Project(OneRowRelation) -> OneRowRelation
 */
public class PushProjectIntoOneRowRelation extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalOneRowRelation()).thenApply(ctx -> {
            LogicalProject<LogicalOneRowRelation> p = ctx.root;
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            Map<Expression, NamedExpression> replaceRootMap = Maps.newHashMap();
            p.child().getProjects().forEach(ne -> {
                if (ne instanceof Alias) {
                    replaceMap.put(ne.toSlot(), ((Alias) ne).child());
                } else {
                    replaceMap.put(ne, ne);
                }
                replaceRootMap.put(ne.toSlot(), ne);
            });
            ImmutableList.Builder<NamedExpression> newProjections = ImmutableList.builder();
            for (NamedExpression old : p.getProjects()) {
                if (old instanceof SlotReference) {
                    newProjections.add(replaceRootMap.get(old));
                } else {
                    newProjections.add((NamedExpression) ExpressionUtils.replace(old, replaceMap));
                }
            }
            ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
            return p.child()
                    .withProjects(newProjections.build().stream().map(expr -> expr instanceof Alias
                            ? (Alias) expr.withChildren(
                                    FoldConstantRule.INSTANCE.rewrite(expr.child(0), context))
                            : expr).collect(Collectors.toList()));
        }).toRule(RuleType.PUSH_PROJECT_INTO_ONE_ROW_RELATION);
    }
}
