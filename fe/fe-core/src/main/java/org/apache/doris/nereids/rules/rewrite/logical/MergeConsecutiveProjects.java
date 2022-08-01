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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionReplacer;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

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
            LogicalProject<GroupPlan> childProject = project.child();
            List<NamedExpression> childProjectExpressions = childProject.getProjects();
            Map<Expression, Expression> childAliasMap = childProjectExpressions.stream()
                    .filter(e -> e instanceof Alias)
                    .collect(Collectors.toMap(
                            NamedExpression::toSlot, e -> e.child(0))
                    );

            projectExpressions = projectExpressions.stream()
                    .map(e -> ExpressionReplacer.INSTANCE.visit(e, childAliasMap))
                    .map(NamedExpression.class::cast)
                    .collect(Collectors.toList());
            return new LogicalProject(projectExpressions, (Plan) childProject.children().get(0));
        }).toRule(RuleType.MERGE_CONSECUTIVE_PROJECTS);
    }
}
