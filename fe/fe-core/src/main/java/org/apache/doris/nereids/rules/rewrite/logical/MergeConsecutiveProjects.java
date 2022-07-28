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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.IterationVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashMap;
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

public class MergeConsecutiveProjects extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalProject()).then(project -> {
            List<NamedExpression> projectExpressions = project.getProjects();
            LogicalProject childProject = project.child();
            List<NamedExpression> childProjectExpressions = childProject.getProjects();
            HashMap<ExprId, Alias> childAliasMap = new HashMap<ExprId, Alias>();
            AliasExtractor extractor = new AliasExtractor();
            for (NamedExpression expression : childProjectExpressions) {
                extractor.visit(expression, childAliasMap);
            }
            ExpressionReplacer replacer =
                    new ExpressionReplacer<Map<ExprId, Alias>>();
            projectExpressions = projectExpressions.stream()
                    .map(e -> replacer.visit(e, childAliasMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            return new LogicalProject(projectExpressions, (Plan) childProject.children().get(0));
        }).toRule(RuleType.MERGE_CONSECUTIVE_PROJECTS);
    }

    private class ExpressionReplacer<M>
            extends DefaultExpressionRewriter<Map<ExprId, Alias>> {
        @Override
        public Expression visit(Expression expr, Map<ExprId, Alias> aliasMap) {
            if (expr instanceof SlotReference) {
                SlotReference slot = (SlotReference) expr;
                ExprId exprId = slot.getExprId();
                if (aliasMap.containsKey(exprId)) {
                    return aliasMap.get(exprId).child();
                }
            }
            return super.visit(expr, aliasMap);
        }
    }

    private class AliasExtractor extends IterationVisitor<Map<ExprId, Alias>> {
        @Override
        public Void visitAlias(Alias alias, Map<ExprId, Alias> context) {
            context.put(alias.getExprId(), alias);
            return null;
        }
    }
}
