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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;

import java.util.List;
import java.util.Objects;

/**
 * PlanTreeRewriteTopDownJob
 * It's easier than the 'BottomUp' job, it handles the plan tree top-down. If some new plans generated after rewrite,
 * it only processes the current node again. Otherwise, it just recursively handles its children.
 */
public class PlanTreeRewriteTopDownJob extends PlanTreeRewriteJob {

    private final RewriteJobContext rewriteJobContext;
    private final List<Rule> rules;

    public PlanTreeRewriteTopDownJob(RewriteJobContext rewriteJobContext, JobContext context, List<Rule> rules) {
        super(JobType.TOP_DOWN_REWRITE, context);
        this.rewriteJobContext = Objects.requireNonNull(rewriteJobContext, "rewriteContext cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
    }

    @Override
    public void execute() {
        if (!rewriteJobContext.childrenVisited) {
            RewriteResult rewriteResult = rewrite(rewriteJobContext.plan, rules, rewriteJobContext);
            if (rewriteResult.hasNewPlan) {
                RewriteJobContext newContext = rewriteJobContext
                        .withPlanAndChildrenVisited(rewriteResult.plan, false);
                pushJob(new PlanTreeRewriteTopDownJob(newContext, context, rules));
                return;
            }

            RewriteJobContext newRewriteJobContext = rewriteJobContext.withChildrenVisited(true);
            pushJob(new PlanTreeRewriteTopDownJob(newRewriteJobContext, context, rules));

            List<Plan> children = newRewriteJobContext.plan.children();
            for (int i = children.size() - 1; i >= 0; i--) {
                RewriteJobContext childRewriteJobContext = new RewriteJobContext(
                        children.get(i), newRewriteJobContext, i, false);
                // NOTICE: this relay on pull up cte anchor
                if (!(rewriteJobContext.plan instanceof LogicalCTEAnchor)) {
                    pushJob(new PlanTreeRewriteTopDownJob(childRewriteJobContext, context, rules));
                }
            }
        } else {
            // All the children part are already visited. Just link the children plan to the current node.
            Plan result = linkChildrenAndParent(rewriteJobContext.plan, rewriteJobContext);
            if (rewriteJobContext.parentContext == null) {
                context.getCascadesContext().setRewritePlan(result);
            }
        }
    }
}
