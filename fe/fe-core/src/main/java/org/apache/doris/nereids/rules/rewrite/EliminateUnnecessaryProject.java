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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * remove the project that output same with its child to avoid we get two consecutive projects in best plan.
 * for more information, please see <a href="https://github.com/apache/doris/pull/13886">this PR</a>
 */
@DependsRules(ColumnPruning.class)
public class EliminateUnnecessaryProject implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return rewrite(plan);
    }

    private Plan rewrite(Plan plan) {
        if (plan instanceof LogicalProject) {
            return rewriteProject((LogicalProject<?>) plan);
        } else {
            return rewriteChildren(plan);
        }
    }

    private Plan rewriteProject(LogicalProject<?> project) {
        if (project.child() instanceof LogicalEmptyRelation) {
            // eliminate unnecessary project
            return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), project.getProjects());
        } else if (project.getOutputSet().equals(project.child().getOutputSet())) {
            // eliminate unnecessary project
            return rewrite(project.child());
        } else {
            return rewriteChildren(project);
        }
    }

    private Plan rewriteChildren(Plan plan) {
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = rewrite(child);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren) : plan;
    }
}
