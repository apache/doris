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

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public class BatchRulesJob {
    protected PlannerContext plannerContext;
    protected List<Job<Plan>> rulesJob = new ArrayList<>();

    BatchRulesJob(PlannerContext plannerContext) {
        this.plannerContext = Objects.requireNonNull(plannerContext, "plannerContext can not null");
    }

    protected Job<Plan> bottomUpBatch(List<RuleFactory<Plan>> ruleFactories) {
        List<Rule<Plan>> rules = new ArrayList<>();
        for (RuleFactory<Plan> ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        Collections.reverse(rules);
        return new RewriteBottomUpJob(
                plannerContext.getMemo().getRoot(),
                rules,
                plannerContext.getCurrentJobContext());
    }

    protected Job<Plan> topDownBatch(List<RuleFactory<Plan>> ruleFactories) {
        List<Rule<Plan>> rules = new ArrayList<>();
        for (RuleFactory<Plan> ruleFactory : ruleFactories) {
            rules.addAll(ruleFactory.buildRules());
        }
        Collections.reverse(rules);
        return new RewriteTopDownJob(
                plannerContext.getMemo().getRoot(),
                rules,
                plannerContext.getCurrentJobContext());
    }

    protected Job<Plan> optimize() {
        return new OptimizeGroupJob(
                plannerContext.getMemo().getRoot(),
                plannerContext.getCurrentJobContext());
    }

    public void execute() {
        for (Job job : rulesJob) {
            plannerContext.pushJob(job);
            plannerContext.getJobScheduler().executeJobPool(plannerContext);
        }
    }
}
