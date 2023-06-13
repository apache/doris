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
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        CascadesContext skipCboRuleCtx = CascadesContext.newRewriteContext(
                cascadesContext, cascadesContext.getRewritePlan(),
                cascadesContext.getCurrentJobContext().getRequiredProperties());
        CascadesContext applyCboRuleCtx = CascadesContext.newRewriteContext(
                cascadesContext, cascadesContext.getRewritePlan(),
                cascadesContext.getCurrentJobContext().getRequiredProperties());
        // execute cbo rule on one candidate
        new Rewriter(applyCboRuleCtx, rewriteJobs).execute();
        if (skipCboRuleCtx.getRewritePlan().deepEquals(applyCboRuleCtx.getRewritePlan())) {
            // this means rewrite do not do anything
            return;
        }
        // Do rewrite on 2 candidates
        new Rewriter(skipCboRuleCtx, jobContext.getRemainJobs()).execute();
        new Rewriter(applyCboRuleCtx, jobContext.getRemainJobs()).execute();
        // Do optimize on 2 candidates
        new Optimizer(skipCboRuleCtx).execute();
        new Optimizer(applyCboRuleCtx).execute();
        Optional<Pair<Cost, GroupExpression>> skipCboRuleCost = skipCboRuleCtx.getMemo().getRoot()
                .getLowestCostPlan(skipCboRuleCtx.getCurrentJobContext().getRequiredProperties());
        Optional<Pair<Cost, GroupExpression>> appliedCboRuleCost = applyCboRuleCtx.getMemo().getRoot()
                .getLowestCostPlan(applyCboRuleCtx.getCurrentJobContext().getRequiredProperties());
        // If one of them optimize failed, just return
        if (!skipCboRuleCost.isPresent() || !appliedCboRuleCost.isPresent()) {
            LOG.warn("Cbo rewrite execute failed");
            return;
        }
        // If the candidate applied cbo rule is better, replace the original plan with it.
        if (appliedCboRuleCost.get().first.getValue() < skipCboRuleCost.get().first.getValue()) {
            cascadesContext.setRewritePlan(applyCboRuleCtx.getRewritePlan());
        }
    }

    @Override
    public boolean isOnce() {
        // TODO: currently, we do not support execute it more than once.
        return true;
    }
}
