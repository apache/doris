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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.rules.rewrite.LeadingJoin.LeadingContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

/**
 *  Leading join is used to generate leading join and replace original logical join
*/
public class LeadingJoin extends DefaultPlanRewriter<LeadingContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (jobContext.getCascadesContext().getStatementContext().isLeadingJoin()) {
            Hint leadingHint = jobContext.getCascadesContext().getStatementContext().getHintMap().get("Leading");
            Plan leadingPlan = plan.accept(this, new LeadingContext(
                    (LeadingHint) leadingHint, ((LeadingHint) leadingHint)
                        .getLeadingTableBitmap(jobContext.getCascadesContext().getTables())));
            if (leadingHint.isSuccess()) {
                try {
                    jobContext.getCascadesContext().getConnectContext().getSessionVariable()
                            .disableNereidsJoinReorderOnce();
                } catch (DdlException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return plan;
            }
            return leadingPlan;
        }
        return plan;
    }

    @Override
    public Plan visit(Plan plan, LeadingContext context) {
        Long currentBitMap = LongBitmap.computeTableBitmap(plan.getInputRelations());
        if (LongBitmap.isSubset(currentBitMap, context.totalBitmap)
                && plan instanceof LogicalJoin && !context.leading.isSyntaxError()) {
            Plan leadingJoin = context.leading.generateLeadingJoinPlan();
            if (context.leading.isSuccess() && leadingJoin != null) {
                return leadingJoin;
            }
        } else {
            return (LogicalPlan) super.visit(plan, context);
        }
        return plan;
    }

    /** LeadingContext */
    public static class LeadingContext {
        public LeadingHint leading;
        public Long totalBitmap;

        public LeadingContext(LeadingHint leading, Long totalBitmap) {
            this.leading = leading;
            this.totalBitmap = totalBitmap;
        }
    }
}
