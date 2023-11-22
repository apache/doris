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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        // If the candidate applied cbo rule is better, replace the original plan with it.
        if (appliedCboRuleCost.get().first.getValue() < skipCboRuleCost.get().first.getValue()) {
            currentCtx.setRewritePlan(applyCboRuleCtx.getRewritePlan());
        }
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
            // Do Whole tree rewrite
            CascadesContext rootCtxCopy = CascadesContext.newCurrentTreeContext(rootCtx);
            Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(rootCtxCopy).execute();
            // Do optimize
            new Optimizer(rootCtxCopy).execute();
            return rootCtxCopy.getMemo().getRoot().getLowestCostPlan(
                    rootCtxCopy.getCurrentJobContext().getRequiredProperties());
        } else {
            new Optimizer(cboCtx).execute();
            return cboCtx.getMemo().getRoot().getLowestCostPlan(
                    cboCtx.getCurrentJobContext().getRequiredProperties());
        }
    }
}
