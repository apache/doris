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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.CostBasedRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.CustomRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.TopicRewriteJob;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public abstract class AbstractBatchJobExecutor {
    private static final ThreadLocal<Set<Class<Plan>>> NOT_TRAVERSE_CHILDREN = new ThreadLocal();
    private static final Predicate<Plan> TRAVERSE_ALL_PLANS = plan -> true;

    protected CascadesContext cascadesContext;

    public AbstractBatchJobExecutor(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not null");
    }

    /**
     * flat map jobs in TopicRewriteJob to could really run jobs, and filter null.
     */
    public static List<RewriteJob> jobs(RewriteJob... jobs) {
        return Arrays.stream(jobs)
                .filter(Objects::nonNull)
                .flatMap(job -> job instanceof TopicRewriteJob
                    ? ((TopicRewriteJob) job).jobs.stream().filter(Objects::nonNull)
                    : Stream.of(job)
                ).collect(ImmutableList.toImmutableList());
    }

    /** notTraverseChildrenOf */
    public static <T> T notTraverseChildrenOf(
            Set<Class<? extends Plan>> notTraverseClasses, Supplier<T> action) {
        try {
            NOT_TRAVERSE_CHILDREN.set((Set) notTraverseClasses);
            return action.get();
        } finally {
            NOT_TRAVERSE_CHILDREN.remove();
        }
    }

    public static TopicRewriteJob topic(String topicName, RewriteJob... jobs) {
        return new TopicRewriteJob(topicName, Arrays.asList(jobs));
    }

    public static RewriteJob costBased(RewriteJob... jobs) {
        return new CostBasedRewriteJob(Arrays.asList(jobs));
    }

    public static RewriteJob bottomUp(RuleFactory... ruleFactories) {
        return bottomUp(Arrays.asList(ruleFactories));
    }

    public static RewriteJob bottomUp(List<RuleFactory> ruleFactories) {
        List<Rule> rules = ruleFactories.stream()
                .map(RuleFactory::buildRules)
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList());
        return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteBottomUpJob::new, getTraversePredicate(), true);
    }

    public static RewriteJob topDown(RuleFactory... ruleFactories) {
        return topDown(Arrays.asList(ruleFactories));
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories) {
        return topDown(ruleFactories, true);
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories, boolean once) {
        List<Rule> rules = ruleFactories.stream()
                .map(RuleFactory::buildRules)
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList());
        return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteTopDownJob::new, getTraversePredicate(), once);
    }

    public static RewriteJob custom(RuleType ruleType, Supplier<CustomRewriter> planRewriter) {
        return new CustomRewriteJob(planRewriter, ruleType);
    }

    /**
     * execute.
     */
    public void execute() {
        for (int i = 0; i < getJobs().size(); i++) {
            JobContext jobContext = cascadesContext.getCurrentJobContext();
            RewriteJob currentJob = getJobs().get(i);
            if (currentJob instanceof CostBasedRewriteJob) {
                List<RewriteJob> remainJobs = getJobs().subList(i + 1, getJobs().size()).stream()
                        .filter(j -> !(j instanceof CostBasedRewriteJob))
                        .collect(Collectors.toList());
                jobContext.setRemainJobs(remainJobs);
            }
            do {
                jobContext.setRewritten(false);
                currentJob.execute(jobContext);
            } while (!currentJob.isOnce() && jobContext.isRewritten());
        }
    }

    public abstract List<RewriteJob> getJobs();

    private static Predicate<Plan> getTraversePredicate() {
        Set<Class<Plan>> notTraverseChildren = NOT_TRAVERSE_CHILDREN.get();
        return notTraverseChildren == null
                ? TRAVERSE_ALL_PLANS
                : new NotTraverseChildren(notTraverseChildren);
    }

    private static class NotTraverseChildren implements Predicate<Plan> {
        private final Set<Class<Plan>> notTraverseChildren;

        public NotTraverseChildren(Set<Class<Plan>> notTraverseChildren) {
            this.notTraverseChildren = Objects.requireNonNull(notTraverseChildren, "notTraversePlans can not be null");
        }

        @Override
        public boolean test(Plan plan) {
            return !notTraverseChildren.contains(plan.getClass());
        }
    }
}
