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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter.PreRewriteStrategy;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Record plan for later mv rewrite
 * */
public class RecordPlanForMvPreRewrite extends DefaultPlanRewriter<Void> implements CustomRewriter {

    public static final Logger LOG = LogManager.getLogger(RecordPlanForMvPreRewrite.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (PreRewriteStrategy.NOT_IN_RBO.equals(PreRewriteStrategy.getEnum(
                cascadesContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy()))) {
            return plan;
        }
        // when sync statementContext.getCandidateMTMVs() is always empty, so sync mv rewrite is in final CBO
        if (!statementContext.isForceRecordTmpPlan() && (statementContext.getCandidateMTMVs().isEmpty()
                || !MaterializedViewUtils.containMaterializedViewHook(cascadesContext.getStatementContext()))) {
            return plan;
        }
        // plan pre normalize
        Plan finalPlan;
        try {
            finalPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getCteChildrenRewriter(childContext,
                                Rewriter.CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED).execute();
                        return childContext.getRewritePlan();
                    }, plan, plan, false, false);
            statementContext.addTmpPlanForMvRewrite(finalPlan);
        } catch (Exception e) {
            LOG.error("mv rewrite in rbo rewrite pre normalize fail, sql hash is {}",
                    cascadesContext.getConnectContext().getSqlHash(), e);
        }
        return plan;
    }
}
