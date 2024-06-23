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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Select A+B, (A+B+C)*2, (A+B+C)*3, D from T
 *
 * before optimize
 * projection:
 * Proj: A+B, (A+B+C)*2, (A+B+C)*3, D
 *
 * ---
 * after optimize:
 * Projection: List < List < Expression > >
 * A+B, C, D
 * A+B, A+B+C, D
 * A+B, (A+B+C)*2, (A+B+C)*3, D
 */
public class CommonSubExpressionOpt extends PlanPostProcessor {
    @Override
    public PhysicalProject<? extends Plan> visitPhysicalProject(
            PhysicalProject<? extends Plan> project, CascadesContext ctx) {
        project.child().accept(this, ctx);
        List<List<NamedExpression>> multiLayers = computeMultiLayerProjections(
                project.getInputSlots(), project.getProjects());
        project.setMultiLayerProjects(multiLayers);
        return project;
    }

    private List<List<NamedExpression>> computeMultiLayerProjections(
            Set<Slot> inputSlots, List<NamedExpression> projects) {

        List<List<NamedExpression>> multiLayers = Lists.newArrayList();
        CommonSubExpressionCollector collector = new CommonSubExpressionCollector();
        for (Expression expr : projects) {
            expr.accept(collector, null);
        }
        Map<Expression, Alias> aliasMap = new HashMap<>();
        if (!collector.commonExprByDepth.isEmpty()) {
            for (int i = 1; i <= collector.commonExprByDepth.size(); i++) {
                List<NamedExpression> layer = Lists.newArrayList();
                layer.addAll(inputSlots);
                Set<Expression> exprsInDepth = CommonSubExpressionCollector
                        .getExpressionsFromDepthMap(i, collector.commonExprByDepth);
                exprsInDepth.forEach(expr -> {
                    if (!(expr instanceof WhenClause)) {
                        // case whenClause1 whenClause2 END
                        // whenClause should not be regarded as common-sub-expression, because
                        // cse will be replaced by a slot, after rewrite the case clause becomes:
                        // 'case slot whenClause2 END'
                        // This is illegal.
                        Expression rewritten = expr.accept(ExpressionReplacer.INSTANCE, aliasMap);
                        // if rewritten is already alias, use it directly, because in materialized view rewriting
                        // Should keep out slot immutably after rewritten successfully
                        aliasMap.put(expr, rewritten instanceof Alias ? (Alias) rewritten : new Alias(rewritten));
                    }
                });
                layer.addAll(aliasMap.values());
                multiLayers.add(layer);
            }
            // final layer
            List<NamedExpression> finalLayer = Lists.newArrayList();
            projects.forEach(expr -> {
                Expression rewritten = expr.accept(ExpressionReplacer.INSTANCE, aliasMap);
                if (rewritten instanceof Slot) {
                    finalLayer.add((NamedExpression) rewritten);
                } else if (rewritten instanceof Alias) {
                    finalLayer.add(new Alias(expr.getExprId(), ((Alias) rewritten).child(), expr.getName()));
                }
            });
            multiLayers.add(finalLayer);
        }
        return multiLayers;
    }

    /**
     * replace sub expr by aliasMap
     */
    public static class ExpressionReplacer
            extends DefaultExpressionRewriter<Map<? extends Expression, ? extends Alias>> {
        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        private ExpressionReplacer() {
        }

        @Override
        public Expression visit(Expression expr, Map<? extends Expression, ? extends Alias> replaceMap) {
            if (replaceMap.containsKey(expr)) {
                return replaceMap.get(expr).toSlot();
            }
            return super.visit(expr, replaceMap);
        }
    }
}
