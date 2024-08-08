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
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/** RootPlanTreeRewriteJob */
public class RootPlanTreeRewriteJob implements RewriteJob {
    private static final AtomicInteger BATCH_ID = new AtomicInteger();

    private final List<Rule> rules;
    private final RewriteJobBuilder rewriteJobBuilder;
    private final boolean once;
    private final Predicate<Plan> isTraverseChildren;

    public RootPlanTreeRewriteJob(List<Rule> rules, RewriteJobBuilder rewriteJobBuilder, boolean once) {
        this(rules, rewriteJobBuilder, plan -> true, once);
    }

    public RootPlanTreeRewriteJob(
            List<Rule> rules, RewriteJobBuilder rewriteJobBuilder, Predicate<Plan> isTraverseChildren, boolean once) {
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.rewriteJobBuilder = Objects.requireNonNull(rewriteJobBuilder, "rewriteJobBuilder cannot be null");
        this.once = once;
        this.isTraverseChildren = isTraverseChildren;
    }

    @Override
    public void execute(JobContext context) {
        CascadesContext cascadesContext = context.getCascadesContext();
        // get plan from the cascades context
        Plan root = cascadesContext.getRewritePlan();
        // write rewritten root plan to cascades context by the RootRewriteJobContext
        int batchId = BATCH_ID.incrementAndGet();
        RootRewriteJobContext rewriteJobContext = new RootRewriteJobContext(
                root, false, context, batchId);
        Job rewriteJob = rewriteJobBuilder.build(rewriteJobContext, context, isTraverseChildren, rules);

        context.getScheduleContext().pushJob(rewriteJob);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);

        cascadesContext.setCurrentRootRewriteJobContext(null);
    }

    @Override
    public boolean isOnce() {
        return once;
    }

    /** RewriteJobBuilder */
    public interface RewriteJobBuilder {
        Job build(RewriteJobContext rewriteJobContext, JobContext jobContext,
                Predicate<Plan> isTraverseChildren, List<Rule> rules);
    }

    /** RootRewriteJobContext */
    public static class RootRewriteJobContext extends RewriteJobContext {

        private final JobContext jobContext;

        RootRewriteJobContext(Plan plan, boolean childrenVisited, JobContext jobContext, int batchId) {
            super(plan, null, -1, childrenVisited, batchId);
            this.jobContext = Objects.requireNonNull(jobContext, "jobContext cannot be null");
            jobContext.getCascadesContext().setCurrentRootRewriteJobContext(this);
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
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        @Override
        public RewriteJobContext withPlan(Plan plan) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        @Override
        public RewriteJobContext withPlanAndChildrenVisited(Plan plan, boolean childrenVisited) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        /** linkChildren */
        public Plan getNewestPlan() {
            JobStack jobStack = new JobStack();
            LinkPlanJob linkPlanJob = new LinkPlanJob(
                    jobContext, this, null, false, jobStack);
            jobStack.push(linkPlanJob);
            while (!jobStack.isEmpty()) {
                Job job = jobStack.pop();
                job.execute();
            }
            return linkPlanJob.result;
        }
    }

    public List<Rule> getRules() {
        return rules;
    }

    /** use to assemble the rewriting plan */
    private static class LinkPlanJob extends Job {
        LinkPlanJob parentJob;
        RewriteJobContext rewriteJobContext;
        Plan[] childrenResult;
        Plan result;
        boolean linked;
        JobStack jobStack;

        private LinkPlanJob(JobContext context, RewriteJobContext rewriteJobContext,
                LinkPlanJob parentJob, boolean linked, JobStack jobStack) {
            super(JobType.LINK_PLAN, context);
            this.rewriteJobContext = rewriteJobContext;
            this.parentJob = parentJob;
            this.linked = linked;
            this.childrenResult = new Plan[rewriteJobContext.plan.arity()];
            this.jobStack = jobStack;
        }

        @Override
        public void execute() {
            if (!linked) {
                linked = true;
                jobStack.push(this);
                for (int i = rewriteJobContext.childrenContext.length - 1; i >= 0; i--) {
                    RewriteJobContext childContext = rewriteJobContext.childrenContext[i];
                    if (childContext != null) {
                        jobStack.push(new LinkPlanJob(context, childContext, this, false, jobStack));
                    }
                }
            } else if (rewriteJobContext.result != null) {
                linkResult(rewriteJobContext.result);
            } else {
                Plan[] newChildren = new Plan[childrenResult.length];
                for (int i = 0; i < newChildren.length; i++) {
                    Plan childResult = childrenResult[i];
                    if (childResult == null) {
                        childResult = rewriteJobContext.plan.child(i);
                    }
                    newChildren[i] = childResult;
                }
                linkResult(rewriteJobContext.plan.withChildren(newChildren));
            }
        }

        private void linkResult(Plan result) {
            if (parentJob != null) {
                parentJob.childrenResult[rewriteJobContext.childIndexInParentContext] = result;
            } else {
                this.result = result;
            }
        }
    }
}
