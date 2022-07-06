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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteBottomUpJob;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

/**
 * sql parse util.
 */
public class AnalyzeUtils {

    private static final NereidsParser parser = new NereidsParser();

    /**
     * analyze sql.
     */
    public static LogicalPlan analyze(String sql, ConnectContext connectContext) {
        try {
            LogicalPlan parsed = parser.parseSingle(sql);
            return analyze(parsed, connectContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LogicalPlan analyze(LogicalPlan inputPlan, ConnectContext connectContext) {
        Memo memo = new Memo();
        memo.initialize(inputPlan);
        PlannerContext plannerContext = new PlannerContext(memo, connectContext);
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        plannerContext.pushJob(
                new RewriteBottomUpJob(memo.getRoot(), new BindSlotReference().buildRules(), jobContext));
        plannerContext.pushJob(
                new RewriteBottomUpJob(memo.getRoot(), new BindRelation().buildRules(), jobContext));
        plannerContext.getJobScheduler().executeJobPool(plannerContext);
        return (LogicalPlan) memo.copyOut();
    }
}
