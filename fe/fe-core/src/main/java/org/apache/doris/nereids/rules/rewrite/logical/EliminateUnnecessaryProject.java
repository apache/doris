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
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * remove the project that output same with its child to avoid we get two consecutive projects in best plan.
 * for more information, please see <a href="https://github.com/apache/doris/pull/13886">this PR</a>
 */
public class EliminateUnnecessaryProject implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.MARK_NECESSARY_PROJECT.build(
                logicalSetOperation(logicalProject(), group())
                    .thenApply(ctx -> {
                        LogicalProject project = (LogicalProject) ctx.root.child(0);
                        return ctx.root.withChildren(project.withEliminate(false), ctx.root.child(1));
                    })
            ),
            RuleType.MARK_NECESSARY_PROJECT.build(
                logicalSetOperation(group(), logicalProject())
                    .thenApply(ctx -> {
                        LogicalProject project = (LogicalProject) ctx.root.child(1);
                        return ctx.root.withChildren(ctx.root.child(0), project.withEliminate(false));
                    })
            ),
            RuleType.ELIMINATE_UNNECESSARY_PROJECT.build(
                logicalProject(any())
                    .when(LogicalProject::canEliminate)
                    .when(project -> project.getOutputSet().equals(project.child().getOutputSet()))
                    .thenApply(ctx -> {
                        int rootGroupId = ctx.cascadesContext.getMemo().getRoot().getGroupId().asInt();
                        LogicalProject<Plan> project = ctx.root;
                        // if project is root, we need to ensure the output order is same.
                        if (project.getGroupExpression().get().getOwnerGroup().getGroupId().asInt()
                                == rootGroupId) {
                            if (project.getOutput().equals(project.child().getOutput())) {
                                return project.child();
                            } else {
                                return null;
                            }
                        } else {
                            return project.child();
                        }
                    })));
    }
}
