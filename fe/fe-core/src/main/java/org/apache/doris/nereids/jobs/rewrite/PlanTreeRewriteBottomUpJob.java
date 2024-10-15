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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * PlanTreeRewriteBottomUpJob
 * The job is used for bottom-up rewrite. If some rewrite rules can take effect,
 * we will process all the rules from the leaf node again. So there are some rules that can take effect interactively,
 * we should use the 'Bottom-Up' job to handle it.
 */
public class PlanTreeRewriteBottomUpJob extends PlanTreeRewriteJob {

    // REWRITE_STATE_KEY represents the key to store the 'RewriteState'. Each plan node has their own 'RewriteState'.
    // Different 'RewriteState' has different actions,
    // so we will do specified action for each node based on their 'RewriteState'.
    private static final String REWRITE_STATE_KEY = "rewrite_state";
    private final RewriteJobContext rewriteJobContext;
    private final List<Rule> rules;
    private final int batchId;

    enum RewriteState {
        // 'REWRITE_THIS' means the current plan node can be handled immediately. If the plan state is 'REWRITE_THIS',
        // it means all of its children's state are 'REWRITTEN'. Because we handle the plan tree bottom up.
        REWRITE_THIS,
        // 'REWRITTEN' means the current plan have been handled already, we don't need to do anything else.
        REWRITTEN,
        // 'ENSURE_CHILDREN_REWRITTEN' means we need to check the children for the current plan node first.
        // It means some plans have changed after rewrite, so we need traverse the plan tree and reset their state.
        // All the plan nodes need to be handled again.
        ENSURE_CHILDREN_REWRITTEN
    }

    public PlanTreeRewriteBottomUpJob(
            RewriteJobContext rewriteJobContext, JobContext context,
            Predicate<Plan> isTraverseChildren, List<Rule> rules) {
        super(JobType.BOTTOM_UP_REWRITE, context, isTraverseChildren);
        this.rewriteJobContext = Objects.requireNonNull(rewriteJobContext, "rewriteContext cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.batchId = rewriteJobContext.batchId;
    }

    @Override
    public void execute() {
        // We'll do different actions based on their different states.
        // You can check the comment in 'RewriteState' structure for more details.
        Plan plan = rewriteJobContext.plan;
        RewriteState state = getState(plan, batchId);
        switch (state) {
            case REWRITE_THIS:
                rewriteThis();
                return;
            case ENSURE_CHILDREN_REWRITTEN:
                ensureChildrenRewritten();
                return;
            case REWRITTEN:
                rewriteJobContext.result = plan;
                return;
            default:
                throw new IllegalStateException("Unknown rewrite state: " + state);
        }
    }

    private void rewriteThis() {
        // Link the current node with the sub-plan to get the current plan which is used in the rewrite phase later.
        Plan plan = linkChildren(rewriteJobContext.plan, rewriteJobContext.childrenContext);
        RewriteResult rewriteResult = rewrite(plan, rules, rewriteJobContext);
        if (rewriteResult.hasNewPlan) {
            RewriteJobContext newJobContext = rewriteJobContext.withPlan(rewriteResult.plan);
            RewriteState state = getState(rewriteResult.plan, batchId);
            // Some eliminate rule will return a rewritten plan, for example the current node is eliminated
            // and return the child plan. So we don't need to handle it again.
            if (state == RewriteState.REWRITTEN) {
                newJobContext.setResult(rewriteResult.plan);
                return;
            }
            // After the rewrite take effect, we should handle the children part again.
            pushJob(new PlanTreeRewriteBottomUpJob(newJobContext, context, isTraverseChildren, rules));
            setState(rewriteResult.plan, RewriteState.ENSURE_CHILDREN_REWRITTEN, batchId);
        } else {
            // No new plan is generated, so just set the state of the current plan to 'REWRITTEN'.
            setState(rewriteResult.plan, RewriteState.REWRITTEN, batchId);
            rewriteJobContext.setResult(rewriteResult.plan);
        }
    }

    private void ensureChildrenRewritten() {
        Plan plan = rewriteJobContext.plan;
        int batchId = rewriteJobContext.batchId;
        setState(plan, RewriteState.REWRITE_THIS, batchId);
        pushJob(new PlanTreeRewriteBottomUpJob(rewriteJobContext, context, isTraverseChildren, rules));

        // some rule return new plan tree, which the number of new plan node > 1,
        // we should transform this new plan nodes too.
        // NOTICE: this relay on pull up cte anchor
        if (isTraverseChildren.test(plan)) {
            pushChildrenJobs(plan);
        }
    }

    private void pushChildrenJobs(Plan plan) {
        List<Plan> children = plan.children();
        switch (children.size()) {
            case 0: return;
            case 1:
                Plan child = children.get(0);
                RewriteJobContext childRewriteJobContext = new RewriteJobContext(
                        child, rewriteJobContext, 0, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, isTraverseChildren, rules));
                return;
            case 2:
                Plan right = children.get(1);
                RewriteJobContext rightRewriteJobContext = new RewriteJobContext(
                        right, rewriteJobContext, 1, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(rightRewriteJobContext, context, isTraverseChildren, rules));

                Plan left = children.get(0);
                RewriteJobContext leftRewriteJobContext = new RewriteJobContext(
                        left, rewriteJobContext, 0, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(leftRewriteJobContext, context, isTraverseChildren, rules));
                return;
            default:
                for (int i = children.size() - 1; i >= 0; i--) {
                    child = children.get(i);
                    childRewriteJobContext = new RewriteJobContext(
                            child, rewriteJobContext, i, false, batchId);
                    pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, isTraverseChildren, rules));
                }
        }
    }

    private static RewriteState getState(Plan plan, int currentBatchId) {
        Optional<RewriteStateContext> state = plan.getMutableState(REWRITE_STATE_KEY);
        if (!state.isPresent()) {
            return RewriteState.ENSURE_CHILDREN_REWRITTEN;
        }
        RewriteStateContext context = state.get();
        if (context.batchId != currentBatchId) {
            return RewriteState.ENSURE_CHILDREN_REWRITTEN;
        }
        return context.rewriteState;
    }

    private static void setState(Plan plan, RewriteState state, int batchId) {
        plan.setMutableState(REWRITE_STATE_KEY, new RewriteStateContext(state, batchId));
    }

    private static class RewriteStateContext {
        private final RewriteState rewriteState;
        private final int batchId;

        public RewriteStateContext(RewriteState rewriteState, int batchId) {
            this.rewriteState = rewriteState;
            this.batchId = batchId;
        }
    }
}
