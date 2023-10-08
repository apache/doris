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
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.List;

/**
 * this rule aims to merge consecutive project. For example:
 * <pre>
 *   project(a)
 *       |
 *   project(a,b)    ->    project(a)
 *       |
 *   project(a, b, c)
 * </pre>
 */
public class MergeProjects extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        // TODO modify ExtractAndNormalizeWindowExpression to handle nested window functions
        // here we just don't merge two projects if there is any window function
        return logicalProject(logicalProject())
                .whenNot(project -> containsWindowExpression(project.getProjects())
                        && containsWindowExpression(project.child().getProjects()))
                .then(MergeProjects::mergeProjects).toRule(RuleType.MERGE_PROJECTS);
    }

    public static Plan mergeProjects(LogicalProject<?> project) {
        LogicalProject<? extends Plan> childProject = (LogicalProject<?>) project.child();
        List<NamedExpression> projectExpressions = project.mergeProjections(childProject);
        return project.withProjectsAndChild(projectExpressions, childProject.child(0));
    }

    private boolean containsWindowExpression(List<NamedExpression> expressions) {
        return expressions.stream().anyMatch(expr -> expr.anyMatch(WindowExpression.class::isInstance));
    }
}
