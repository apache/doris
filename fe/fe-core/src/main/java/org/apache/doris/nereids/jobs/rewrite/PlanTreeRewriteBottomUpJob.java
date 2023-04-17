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

/** PlanTreeRewriteBottomUpJob */
public class PlanTreeRewriteBottomUpJob extends PlanTreeRewriteJob {
    private static final String REWRITE_STATE_KEY = "rewrite_state";
    private RewriteJobContext rewriteJobContext;
    private List<Rule> rules;

    enum RewriteState {
        ENSURE_CHILDREN_REWRITTEN, REWRITE_THIS, REWRITTEN
    }

    public PlanTreeRewriteBottomUpJob(RewriteJobContext rewriteJobContext, JobContext context, List<Rule> rules) {
        super(JobType.TOP_DOWN_REWRITE, context);
        this.rewriteJobContext = Objects.requireNonNull(rewriteJobContext, "rewriteContext cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
    }

    @Override
    public void execute() {
        // use childrenVisited to judge whether clear the state in the previous batch
        boolean clearStatePhase = !rewriteJobContext.childrenVisited;
        if (clearStatePhase) {
            traverseClearState();
            return;
        }

        Plan plan = rewriteJobContext.plan;
        RewriteState state = getState(plan);
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

    private void traverseClearState() {
        RewriteJobContext clearedStateContext = rewriteJobContext.withChildrenVisited(true);
        setState(clearedStateContext.plan, RewriteState.REWRITE_THIS);
        pushJob(new PlanTreeRewriteBottomUpJob(clearedStateContext, context, rules));

        List<Plan> children = clearedStateContext.plan.children();
        for (int i = children.size() - 1; i >= 0; i--) {
            Plan child = children.get(i);
            RewriteJobContext childRewriteJobContext = new RewriteJobContext(
                    child, clearedStateContext, i, false);
            pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, rules));
        }
    }

    private void rewriteThis() {
        Plan plan = linkChildren(rewriteJobContext.plan, rewriteJobContext.childrenContext);
        RewriteResult rewriteResult = rewrite(plan, rules, rewriteJobContext);
        if (rewriteResult.hasNewPlan) {
            RewriteJobContext newJobContext = rewriteJobContext.withPlan(rewriteResult.plan);
            RewriteState state = getState(rewriteResult.plan);
            // some eliminate rule will return a rewritten plan
            if (state == RewriteState.REWRITTEN) {
                newJobContext.setResult(rewriteResult.plan);
                return;
            }
            pushJob(new PlanTreeRewriteBottomUpJob(newJobContext, context, rules));
            setState(rewriteResult.plan, RewriteState.ENSURE_CHILDREN_REWRITTEN);
        } else {
            setState(rewriteResult.plan, RewriteState.REWRITTEN);
            rewriteJobContext.setResult(rewriteResult.plan);
        }
    }

    private void ensureChildrenRewritten() {
        Plan plan = rewriteJobContext.plan;
        setState(plan, RewriteState.REWRITE_THIS);
        pushJob(new PlanTreeRewriteBottomUpJob(rewriteJobContext, context, rules));

        List<Plan> children = plan.children();
        for (int i = children.size() - 1; i >= 0; i--) {
            Plan child = children.get(i);
            // some rule return new plan tree, which the number of new plan node > 1,
            // we should transform this new plan nodes too.
            RewriteJobContext childRewriteJobContext = new RewriteJobContext(
                    child, rewriteJobContext, i, false);
            pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, rules));
        }
    }

    private static final RewriteState getState(Plan plan) {
        Optional<RewriteState> state = plan.getMutableState(REWRITE_STATE_KEY);
        return state.orElse(RewriteState.ENSURE_CHILDREN_REWRITTEN);
    }

    private static final void setState(Plan plan, RewriteState state) {
        plan.setMutableState(REWRITE_STATE_KEY, state);
    }
}
