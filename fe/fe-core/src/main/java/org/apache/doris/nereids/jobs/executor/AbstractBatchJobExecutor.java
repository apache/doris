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
import org.apache.doris.nereids.jobs.rewrite.BottomUpVisitorRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.CostBasedRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.CustomRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.jobs.rewrite.TopDownVisitorRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.TopicRewriteJob;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public abstract class AbstractBatchJobExecutor {
    private static final ThreadLocal<Set<Class<Plan>>> NOT_TRAVERSE_CHILDREN = new ThreadLocal<>();
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
                .flatMap(job -> {
                    if (job instanceof TopicRewriteJob && !((TopicRewriteJob) job).condition.isPresent()) {
                        return ((TopicRewriteJob) job).jobs.stream();
                    } else {
                        return Stream.of(job);
                    }
                }).collect(ImmutableList.toImmutableList());
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
        return new TopicRewriteJob(topicName, Arrays.asList(jobs), null);
    }

    public static TopicRewriteJob topic(String topicName, Predicate<CascadesContext> condition, RewriteJob... jobs) {
        return new TopicRewriteJob(topicName, Arrays.asList(jobs), condition);
    }

    public static RewriteJob costBased(RewriteJob... jobs) {
        return new CostBasedRewriteJob(Arrays.asList(jobs));
    }

    public static RewriteJob bottomUp(RuleFactory... ruleFactories) {
        return bottomUp(Arrays.asList(ruleFactories));
    }

    /** bottomUp */
    public static RewriteJob bottomUp(List<RuleFactory> ruleFactories) {
        Rules rules = new FilteredRules(ruleFactories.stream()
                .map(RuleFactory::buildRules)
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList()));
        return new BottomUpVisitorRewriteJob(rules);
        // return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteBottomUpJob::new, getTraversePredicate(), true);
    }

    public static RewriteJob topDown(RuleFactory... ruleFactories) {
        return topDown(Arrays.asList(ruleFactories));
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories) {
        return topDown(ruleFactories, true);
    }

    /** topDown */
    public static RewriteJob topDown(List<RuleFactory> ruleFactories, boolean once) {
        Rules rules = new FilteredRules(ruleFactories.stream()
                .map(RuleFactory::buildRules)
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList()));
        return new TopDownVisitorRewriteJob(rules);
        // return new TopDownVisitorRewriteJob2(rules);
        // return new RootPlanTreeRewriteJob(rules, PlanTreeRewriteTopDownJob::new, getTraversePredicate(), once);
    }

    public static RewriteJob custom(RuleType ruleType, Supplier<CustomRewriter> planRewriter) {
        return new CustomRewriteJob(planRewriter, ruleType);
    }

    /**
     * execute.
     */
    public void execute() {
        List<RewriteJob> jobs = Lists.newArrayList(getJobs());
        for (int i = 0; i < jobs.size(); i++) {
            JobContext jobContext = cascadesContext.getCurrentJobContext();
            RewriteJob currentJob = jobs.get(i);

            if (currentJob instanceof TopicRewriteJob) {
                TopicRewriteJob topicRewriteJob = (TopicRewriteJob) currentJob;
                Optional<Predicate<CascadesContext>> condition = topicRewriteJob.condition;
                if (!condition.isPresent() || condition.get().test(jobContext.getCascadesContext())) {
                    jobs.addAll(i + 1, topicRewriteJob.jobs);
                }
                continue;
            }

            if (currentJob instanceof CostBasedRewriteJob) {
                ImmutableList.Builder<RewriteJob> remainJobs
                        = ImmutableList.builderWithExpectedSize(jobs.size() - i - 1);
                for (int j = i + 1; j < jobs.size(); j++) {
                    RewriteJob job = jobs.get(j);
                    if (!(job instanceof CostBasedRewriteJob)) {
                        remainJobs.add(job);
                    }
                }
                jobContext.setRemainJobs(remainJobs.build());
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
