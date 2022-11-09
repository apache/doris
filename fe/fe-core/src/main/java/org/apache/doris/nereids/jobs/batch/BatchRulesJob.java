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

package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.VisitorRewriteJob;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public abstract class BatchRulesJob {
    protected CascadesContext cascadesContext;
    protected List<Job> rulesJob = new ArrayList<>();

    BatchRulesJob(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not null");
    }

    protected Job bottomUpBatch(List<RuleFactory> ruleFactories) {
        List<Rule> rules = new ArrayList<>();
        for (RuleFactory ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        return new RewriteBottomUpJob(
                cascadesContext.getMemo().getRoot(),
                rules,
                cascadesContext.getCurrentJobContext());
    }

    protected Job topDownBatch(List<RuleFactory> ruleFactories) {
        List<Rule> rules = new ArrayList<>();
        for (RuleFactory ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        return new RewriteTopDownJob(cascadesContext.getMemo().getRoot(), rules,
                cascadesContext.getCurrentJobContext());
    }

    protected Job topDownBatch(List<RuleFactory> ruleFactories, boolean once) {
        List<Rule> rules = new ArrayList<>();
        for (RuleFactory ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        return new RewriteTopDownJob(cascadesContext.getMemo().getRoot(), rules,
                cascadesContext.getCurrentJobContext(), once);
    }

    protected Job visitorJob(DefaultPlanRewriter<JobContext> planRewriter) {
        return new VisitorRewriteJob(cascadesContext, planRewriter, true);
    }

    protected Job optimize() {
        return new OptimizeGroupJob(
                cascadesContext.getMemo().getRoot(),
                cascadesContext.getCurrentJobContext());
    }

    /**
     * execute.
     */
    public void execute() {
        for (Job job : rulesJob) {
            do {
                cascadesContext.getCurrentJobContext().setRewritten(false);
                cascadesContext.pushJob(job);
                cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            } while (!job.isOnce() && cascadesContext.getCurrentJobContext().isRewritten());
        }
    }
}
