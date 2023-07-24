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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Objects;

/** RootPlanTreeRewriteJob */
public class RootPlanTreeRewriteJob implements RewriteJob {

    private final List<Rule> rules;
    private final RewriteJobBuilder rewriteJobBuilder;
    private final boolean once;

    public RootPlanTreeRewriteJob(List<Rule> rules, RewriteJobBuilder rewriteJobBuilder, boolean once) {
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.rewriteJobBuilder = Objects.requireNonNull(rewriteJobBuilder, "rewriteJobBuilder cannot be null");
        this.once = once;
    }

    @Override
    public void execute(JobContext context) {
        CascadesContext cascadesContext = context.getCascadesContext();
        // get plan from the cascades context
        Plan root = cascadesContext.getRewritePlan();
        // write rewritten root plan to cascades context by the RootRewriteJobContext
        RootRewriteJobContext rewriteJobContext = new RootRewriteJobContext(root, false, context);
        Job rewriteJob = rewriteJobBuilder.build(rewriteJobContext, context, rules);

        context.getScheduleContext().pushJob(rewriteJob);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    @Override
    public boolean isOnce() {
        return once;
    }

    /** RewriteJobBuilder */
    public interface RewriteJobBuilder {
        Job build(RewriteJobContext rewriteJobContext, JobContext jobContext, List<Rule> rules);
    }

    /** RootRewriteJobContext */
    public static class RootRewriteJobContext extends RewriteJobContext {

        private final JobContext jobContext;

        RootRewriteJobContext(Plan plan, boolean childrenVisited, JobContext jobContext) {
            super(plan, null, -1, childrenVisited);
            this.jobContext = Objects.requireNonNull(jobContext, "jobContext cannot be null");
        }

        @Override
        public boolean isRewriteRoot() {
            return true;
        }

        @Override
        public void setResult(Plan result) {
            jobContext.getCascadesContext().setRewritePlan(result);
        }

        @Override
        public RewriteJobContext withChildrenVisited(boolean childrenVisited) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext);
        }

        @Override
        public RewriteJobContext withPlan(Plan plan) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext);
        }

        @Override
        public RewriteJobContext withPlanAndChildrenVisited(Plan plan, boolean childrenVisited) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext);
        }
    }
}
