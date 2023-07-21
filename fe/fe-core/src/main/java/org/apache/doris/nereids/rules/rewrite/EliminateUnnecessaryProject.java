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
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.OutputSavePoint;
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
        return rewrite(plan, false);
    }

    private Plan rewrite(Plan plan, boolean outputSavePoint) {
        if (plan instanceof LogicalSetOperation) {
            return rewriteLogicalSetOperation((LogicalSetOperation) plan, outputSavePoint);
        } else if (plan instanceof LogicalProject) {
            return rewriteProject((LogicalProject) plan, outputSavePoint);
        } else if (plan instanceof OutputSavePoint) {
            return rewriteChildren(plan, true);
        } else {
            return rewriteChildren(plan, outputSavePoint);
        }
    }

    private Plan rewriteProject(LogicalProject<Plan> project, boolean outputSavePoint) {
        if (project.child() instanceof LogicalEmptyRelation) {
            // eliminate unnecessary project
            return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), project.getProjects());
        } else if (project.canEliminate() && outputSavePoint
                && project.getOutputSet().equals(project.child().getOutputSet())) {
            // eliminate unnecessary project
            return rewrite(project.child(), outputSavePoint);
        } else if (project.canEliminate() && project.getOutput().equals(project.child().getOutput())) {
            // eliminate unnecessary project
            return rewrite(project.child(), outputSavePoint);
        } else {
            return rewriteChildren(project, true);
        }
    }

    private Plan rewriteLogicalSetOperation(LogicalSetOperation set, boolean outputSavePoint) {
        if (set.arity() == 2) {
            Plan left = set.child(0);
            Plan right = set.child(1);
            boolean changed = false;
            if (isCanEliminateProject(left)) {
                changed = true;
                left = ((LogicalProject) left).withEliminate(false);
            }
            if (isCanEliminateProject(right)) {
                changed = true;
                right = ((LogicalProject) right).withEliminate(false);
            }
            if (changed) {
                set = (LogicalSetOperation) set.withChildren(left, right);
            }
        }
        return rewriteChildren(set, outputSavePoint);
    }

    private Plan rewriteChildren(Plan plan, boolean outputSavePoint) {
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = rewrite(child, outputSavePoint);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren) : plan;
    }

    private static boolean isCanEliminateProject(Plan plan) {
        return plan instanceof LogicalProject && ((LogicalProject<?>) plan).canEliminate();
    }
}
