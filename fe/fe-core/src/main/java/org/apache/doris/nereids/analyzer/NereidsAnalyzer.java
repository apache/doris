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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.batch.AnalyzeRulesJob;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

/**
 * Bind symbols according to metadata in the catalog, perform semantic analysis, etc.
 * TODO: revisit the interface after subquery analysis is supported.
 */
public class NereidsAnalyzer {
    private final ConnectContext connectContext;

    public NereidsAnalyzer(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    /**
     * Analyze plan.
     */
    public LogicalPlan analyze(Plan plan) {
        return (LogicalPlan) analyzeWithPlannerContext(plan).getMemo().copyOut();
    }

    /**
     * Convert SQL String to analyzed plan.
     */
    public LogicalPlan analyze(String sql) {
        return analyze(parse(sql));
    }

    /**
     * Analyze plan and return {@link PlannerContext}.
     * Thus returned {@link PlannerContext} could be reused to do
     * further plan optimization without creating new {@link Memo} and {@link PlannerContext}.
     */
    public PlannerContext analyzeWithPlannerContext(Plan plan) {
        PlannerContext plannerContext = new Memo(plan)
                .newPlannerContext(connectContext)
                .setDefaultJobContext();

        new AnalyzeRulesJob(plannerContext).execute();
        return plannerContext;
    }

    /**
     * Convert SQL String to analyzed plan without copying out of {@link Memo}.
     * Thus returned {@link PlannerContext} could be reused to do
     * further plan optimization without creating new {@link Memo} and {@link PlannerContext}.
     */
    public PlannerContext analyzeWithPlannerContext(String sql) {
        return analyzeWithPlannerContext(parse(sql));
    }

    private Plan parse(String sql) {
        return new NereidsParser().parseSingle(sql);
    }
}
