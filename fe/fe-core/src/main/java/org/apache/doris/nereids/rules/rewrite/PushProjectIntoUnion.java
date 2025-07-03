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
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Project(Union) -> Union, if union with all qualifier and without children.
 */
public class PushProjectIntoUnion extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalUnion()
                .when(u -> u.getQualifier() == Qualifier.ALL)
                .when(u -> u.arity() == 0)
        ).thenApply(ctx -> {
            LogicalProject<LogicalUnion> p = ctx.root;
            ExpressionRewriteContext expressionRewriteContext = new ExpressionRewriteContext(ctx.cascadesContext);
            LogicalUnion union = p.child();
            ImmutableList.Builder<List<NamedExpression>> newConstExprs = ImmutableList.builder();
            for (List<NamedExpression> constExprs : union.getConstantExprsList()) {
                Map<Expression, Expression> replaceMap = Maps.newHashMap();
                Map<Expression, NamedExpression> replaceRootMap = Maps.newHashMap();
                for (int i = 0; i < constExprs.size(); i++) {
                    NamedExpression ne = constExprs.get(i);
                    if (ne instanceof Alias) {
                        replaceMap.put(union.getOutput().get(i), ((Alias) ne).child());
                    } else {
                        replaceMap.put(union.getOutput().get(i), ne);
                    }
                    replaceRootMap.put(union.getOutput().get(i), ne);
                }
                ImmutableList.Builder<NamedExpression> newProjections = ImmutableList.builder();
                for (NamedExpression old : p.getProjects()) {
                    if (old instanceof SlotReference) {
                        newProjections.add((NamedExpression) FoldConstantRule.evaluate(replaceRootMap.get(old),
                                expressionRewriteContext));
                    } else {
                        newProjections.add((NamedExpression) FoldConstantRule.evaluate(
                                ExpressionUtils.replaceNameExpression(old, replaceMap), expressionRewriteContext));
                    }
                }
                newConstExprs.add(newProjections.build());
            }
            return p.child().withNewOutputsChildrenAndConstExprsList(ImmutableList.copyOf(p.getOutput()),
                    ImmutableList.of(), ImmutableList.of(), newConstExprs.build());
        }).toRule(RuleType.PUSH_PROJECT_INTO_UNION);
    }
}
