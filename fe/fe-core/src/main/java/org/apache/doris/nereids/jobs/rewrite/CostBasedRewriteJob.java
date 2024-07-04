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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseCboRuleHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Cost based rewrite job.
 * This job do
 */
public class CostBasedRewriteJob implements RewriteJob {

    private static final Logger LOG = LogManager.getLogger(CostBasedRewriteJob.class);

    private final List<RewriteJob> rewriteJobs;

    public CostBasedRewriteJob(List<RewriteJob> rewriteJobs) {
        this.rewriteJobs = rewriteJobs;
        // need to generate real rewrite job list
    }

    @Override
    public void execute(JobContext jobContext) {
        // checkHint.first means whether it use hint and checkHint.second means what kind of hint it used
        Pair<Boolean, Hint> checkHint = checkRuleHint();
        // this means it no_use_cbo_rule(xxx) hint
        if (checkHint.first && checkHint.second == null) {
            return;
        }
        CascadesContext currentCtx = jobContext.getCascadesContext();
        CascadesContext skipCboRuleCtx = CascadesContext.newCurrentTreeContext(currentCtx);
        CascadesContext applyCboRuleCtx = CascadesContext.newCurrentTreeContext(currentCtx);
        // execute cbo rule on one candidate
        Rewriter.getCteChildrenRewriter(applyCboRuleCtx, rewriteJobs).execute();
        if (skipCboRuleCtx.getRewritePlan().deepEquals(applyCboRuleCtx.getRewritePlan())) {
            // this means rewrite do not do anything
            return;
        }

        // compare two candidates
        Optional<Pair<Cost, GroupExpression>> skipCboRuleCost = getCost(currentCtx, skipCboRuleCtx, jobContext);
        Optional<Pair<Cost, GroupExpression>> appliedCboRuleCost = getCost(currentCtx, applyCboRuleCtx, jobContext);
        // If one of them optimize failed, just return
        if (!skipCboRuleCost.isPresent() || !appliedCboRuleCost.isPresent()) {
            LOG.warn("Cbo rewrite execute failed on sql: {}, jobs are {}, plan is {}.",
                    currentCtx.getStatementContext().getOriginStatement().originStmt,
                    rewriteJobs, currentCtx.getRewritePlan());
            return;
        }
        if (checkHint.first) {
            checkHint.second.setStatus(Hint.HintStatus.SUCCESS);
            if (((UseCboRuleHint) checkHint.second).isNotUseCboRule()) {
                currentCtx.setRewritePlan(applyCboRuleCtx.getRewritePlan());
            }
            return;
        }
        // If the candidate applied cbo rule is better, replace the original plan with it.
        if (appliedCboRuleCost.get().first.getValue() < skipCboRuleCost.get().first.getValue()) {
            currentCtx.setRewritePlan(applyCboRuleCtx.getRewritePlan());
        }
    }

    /**
     * check if we have use rule hint or no use rule hint
     *     return an optional object which checkHint.first means whether it use hint
     *     and checkHint.second means what kind of hint it used
     *     example, when we use *+ no_use_cbo_rule(xxx) * the optional would be (true, false)
     *     which means it use hint and the hint forbid this kind of rule
     */
    private Pair<Boolean, Hint> checkRuleHint() {
        Pair<Boolean, Hint> checkResult = Pair.of(false, null);
        if (rewriteJobs.get(0) instanceof RootPlanTreeRewriteJob) {
            for (Rule rule : ((RootPlanTreeRewriteJob) rewriteJobs.get(0)).getRules()) {
                checkResult = checkRuleHintWithHintName(rule.getRuleType());
                if (checkResult.first) {
                    return checkResult;
                }
            }
        }
        if (rewriteJobs.get(0) instanceof CustomRewriteJob) {
            checkResult = checkRuleHintWithHintName(((CustomRewriteJob) rewriteJobs.get(0)).getRuleType());
        }
        return checkResult;
    }

    /**
     * for these rules we need use_cbo_rule hint to enable it, otherwise it would be close by default
     */
    private static boolean checkBlackList(RuleType ruleType) {
        List<RuleType> ruleWhiteList = new ArrayList<>(Arrays.asList(
                RuleType.PUSH_DOWN_AGG_THROUGH_JOIN,
                RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE,
                RuleType.PUSH_DOWN_DISTINCT_THROUGH_JOIN));
        if (!ruleWhiteList.isEmpty() && ruleWhiteList.contains(ruleType)) {
            return true;
        }
        return false;
    }

    /**
     * main mechanism of checkRuleHint
     * return an optional object which checkHint.first means whether it use hint
     * and checkHint.second means what kind of hint it used
     */
    private Pair<Boolean, Hint> checkRuleHintWithHintName(RuleType ruleType) {
        for (Hint hint : ConnectContext.get().getStatementContext().getHints()) {
            if (hint.getHintName().equalsIgnoreCase(ruleType.name())) {
                return Pair.of(true, hint);
            }
        }
        if (checkBlackList(ruleType)) {
            return Pair.of(true, null);
        }
        return Pair.of(false, null);
    }

    @Override
    public boolean isOnce() {
        // TODO: currently, we do not support execute it more than once.
        return true;
    }

    private Optional<Pair<Cost, GroupExpression>> getCost(CascadesContext currentCtx,
            CascadesContext cboCtx, JobContext jobContext) {
        // Do subtree rewrite
        Rewriter.getCteChildrenRewriter(cboCtx, jobContext.getRemainJobs()).execute();
        CascadesContext rootCtx = currentCtx.getRoot();
        if (rootCtx.getRewritePlan() instanceof LogicalCTEAnchor) {
            // set subtree rewrite cache
            currentCtx.getStatementContext().getRewrittenCteProducer()
                    .put(currentCtx.getCurrentTree().orElse(null), (LogicalPlan) cboCtx.getRewritePlan());
        }
        // Do post tree rewrite
        CascadesContext rootCtxCopy = CascadesContext.newCurrentTreeContext(rootCtx);
        Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(rootCtxCopy).execute();
        // Do optimize
        new Optimizer(rootCtxCopy).execute();
        return rootCtxCopy.getMemo().getRoot().getLowestCostPlan(
                rootCtxCopy.getCurrentJobContext().getRequiredProperties());
    }
}
