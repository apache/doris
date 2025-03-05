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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;

/** TopDownVisitorRewriteJob */
public class TopDownVisitorRewriteJob implements RewriteJob {
    private final Rules rules;

    public TopDownVisitorRewriteJob(Rules rules) {
        this.rules = rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan root = rewrite(jobContext.getCascadesContext().getRewritePlan(), jobContext);
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private Plan rewrite(Plan plan, JobContext jobContext) {
        if (rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            return plan;
        }

        plan = doRewrite(plan, jobContext);

        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Plan newChild = rewrite(child, jobContext);
            newChildren.add(newChild);
            changed |= newChild != child;
        }

        if (changed) {
            plan = plan.withChildren(newChildren.build());
        }

        return plan;
    }

    private Plan doRewrite(Plan plan, JobContext jobContext) {
        List<Rule> currentRules = rules.getCurrentRules(plan);
        BitSet forbidRules = jobContext.getCascadesContext().getAndCacheDisableRules();
        for (Rule currentRule : currentRules) {
            if (!currentRule.getPattern().matchPlanTree(plan) || forbidRules.get(currentRule.getRuleType().ordinal())) {
                continue;
            }
            List<Plan> transform = currentRule.transform(plan, jobContext.getCascadesContext());
            if (!transform.isEmpty() && transform.get(0) != plan) {
                return transform.get(0);
            }
        }
        return plan;
    }
}
