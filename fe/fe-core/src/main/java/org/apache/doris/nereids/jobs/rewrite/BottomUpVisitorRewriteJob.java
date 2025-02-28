package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.BitSet;
import java.util.List;

public class BottomUpVisitorRewriteJob implements RewriteJob {
    private final Rules rules;

    public BottomUpVisitorRewriteJob(Rules rules) {
        this.rules = rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan root = rewite(jobContext.getCascadesContext().getRewritePlan(), jobContext);
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private Plan rewite(Plan plan, JobContext jobContext) {
        Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        for (Plan child : plan.children()) {
            newChildren.add(rewite(child, jobContext));
        }

        plan = plan.withChildren(newChildren.build());
        return doRewrite(plan, jobContext);
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
