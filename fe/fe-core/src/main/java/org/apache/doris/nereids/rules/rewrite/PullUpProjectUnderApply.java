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
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Adjust the order of Project and apply in correlated subqueries.
 * <pre>
 * before:
 *              apply
 *             /     \
 * Input(output:b)    Project(output:a)
 *                         |
 *                       child
 *
 * after:
 *          Project(b,(if the Subquery is Scalar add 'a' as the output column))
 *                  |
 *                apply
 *               /     \
 * Input(output:b)      child
 * </pre>
 */
public class PullUpProjectUnderApply extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalProject(any()))
                .when(LogicalApply::isCorrelated)
                .whenNot(apply -> apply.right().child() instanceof LogicalFilter && apply.isIn())
                .whenNot(LogicalApply::alreadyExecutedEliminateFilter)
                .then(apply -> {
                    LogicalProject<Plan> project = apply.right();
                    Plan newCorrelate = apply.withChildren(apply.left(), project.child());
                    List<NamedExpression> newProjects = new ArrayList<>(apply.left().getOutput());
                    if (apply.getSubqueryExpr() instanceof ScalarSubquery) {
                        Preconditions.checkState(project.getProjects().size() == 1,
                                "ScalarSubquery should only have one output column");
                        newProjects.add(project.getProjects().get(0));
                    }
                    return project.withProjectsAndChild(newProjects, newCorrelate);
                }).toRule(RuleType.PULL_UP_PROJECT_UNDER_APPLY);
    }
}
