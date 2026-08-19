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
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
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
        return logicalProject(logicalUnion())
                .when(this::canPushProjectIntoUnion
        ).thenApply(ctx -> {
            LogicalProject<LogicalUnion> p = ctx.root;
            ExpressionRewriteContext expressionRewriteContext = new ExpressionRewriteContext(p, ctx.cascadesContext);
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
                        // replaceRootMap.get(old) is the original constant NamedExpression from
                        // constExprs (each row owns a distinct ExprId, none equal to the new
                        // UNION output ExprId), so it can be reused as-is.
                        newProjections.add((NamedExpression) FoldConstantRule.evaluate(replaceRootMap.get(old),
                                expressionRewriteContext));
                    } else {
                        // `old` must be an Alias (Nereids LogicalProject invariant for non-Slot
                        // projections). Its ExprId equals the new UNION output ExprId, since
                        // p.getOutput() becomes the UNION output below. Feeding the original
                        // Alias into the rewriter would preserve that outer ExprId on every
                        // constant row and collide with the UNION output. Reassign a fresh
                        // ExprId on the Alias first, then run the SlotRef -> constant rewrite
                        // and constant folding on the new Alias; this preserves the Alias'
                        // name/qualifier/nameFromChild while breaking the ExprId collision.
                        Alias oldAlias = (Alias) old;
                        Alias reIdAlias = oldAlias.withExprId(StatementScopeIdGenerator.newExprId());
                        newProjections.add((NamedExpression) FoldConstantRule.evaluate(
                                ExpressionUtils.replace(reIdAlias, replaceMap), expressionRewriteContext));
                    }
                }
                newConstExprs.add(newProjections.build());
            }
            return p.child().withNewOutputsChildrenAndConstExprsList(ImmutableList.copyOf(p.getOutput()),
                    ImmutableList.of(), ImmutableList.of(), newConstExprs.build());
        }).toRule(RuleType.PUSH_PROJECT_INTO_UNION);
    }

    private boolean canPushProjectIntoUnion(LogicalProject<LogicalUnion> project) {
        LogicalUnion union = project.child();
        if (union.getQualifier() != Qualifier.ALL || union.arity() != 0) {
            return false;
        }
        for (List<NamedExpression> constExprs : union.getConstantExprsList()) {
            for (NamedExpression ne : constExprs) {
                // reject sensitive constant rows wholesale: a NoneMovableFunction (e.g.
                // assert_true) or a volatile constant must never be pushed into the union.
                // substitution plus constant folding can eliminate the expression entirely
                // (e.g. IF(FALSE, assert_true(...), TRUE) -> TRUE), suppress a required error,
                // or duplicate/copy its evaluation, even when the parent project references it
                // only once.
                if (ne.containsNoneMovableOrVolatile()) {
                    return false;
                }
            }
        }
        return true;
    }
}
