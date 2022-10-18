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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * this rule aims to merge consecutive filters.
 * For example:
 * logical plan tree:
 *                project(a)
 *                  |
 *                project(a,b)
 *                  |
 *                project(a, b, c)
 *                  |
 *                scan
 * transformed to:
 *                project(a)
 *                   |
 *                 scan
 */

public class MergeProjects extends OneRewriteRuleFactory {

    private static class ExpressionReplacer extends DefaultExpressionRewriter<Map<Expression, Expression>> {
        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        public Expression replace(Expression expr, Map<Expression, Expression> substitutionMap) {
            if (expr instanceof SlotReference) {
                Slot ref = ((SlotReference) expr).withQualifier(Collections.emptyList());
                return substitutionMap.getOrDefault(ref, expr);
            }
            return visit(expr, substitutionMap);
        }

        /**
         * case 1:
         *          project(alias(c) as d, alias(x) as y)
         *                      |
         *                      |                          ===>       project(alias(a) as d, alias(b) as y)
         *                      |
         *          project(slotRef(a) as c, slotRef(b) as x)
         * case 2:
         *         project(slotRef(x.c), slotRef(x.d))
         *                      |                          ===>       project(slotRef(a) as x.c, slotRef(b) as x.d)
         *         project(slotRef(a) as c, slotRef(b) as d)
         * case 3: others
         */
        @Override
        public Expression visit(Expression expr, Map<Expression, Expression> substitutionMap) {
            if (expr instanceof Alias && expr.child(0) instanceof SlotReference) {
                // case 1:
                Expression c = expr.child(0);
                // Alias doesn't contain qualifier
                Slot ref = ((SlotReference) c).withQualifier(Collections.emptyList());
                if (substitutionMap.containsKey(ref)) {
                    return expr.withChildren(substitutionMap.get(ref).children());
                }
            } else if (expr instanceof SlotReference) {
                // case 2:
                Slot ref = ((SlotReference) expr).withQualifier(Collections.emptyList());
                if (substitutionMap.containsKey(ref)) {
                    Alias res = (Alias) substitutionMap.get(ref);
                    return res.child();
                }
            } else if (substitutionMap.containsKey(expr)) {
                return substitutionMap.get(expr).child(0);
            }
            return super.visit(expr, substitutionMap);
        }
    }

    @Override
    public Rule build() {
        return logicalProject(logicalProject()).then(project -> {
            List<NamedExpression> projectExpressions = project.getProjects();
            LogicalProject<GroupPlan> childProject = project.child();
            List<NamedExpression> childProjectExpressions = childProject.getProjects();
            Map<Expression, Expression> childAliasMap = childProjectExpressions.stream()
                    .filter(e -> e instanceof Alias)
                    .collect(Collectors.toMap(
                            NamedExpression::toSlot, e -> e)
                    );

            projectExpressions = projectExpressions.stream()
                    .map(e -> MergeProjects.ExpressionReplacer.INSTANCE.replace(e, childAliasMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            return new LogicalProject<>(projectExpressions, childProject.children().get(0));
        }).toRule(RuleType.MERGE_PROJECTS);
    }
}
