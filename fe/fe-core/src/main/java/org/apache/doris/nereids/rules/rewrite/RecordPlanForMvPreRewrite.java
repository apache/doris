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
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Record plan for later mv rewrite
 * */
public class RecordPlanForMvPreRewrite extends DefaultPlanRewriter<Void> implements CustomRewriter {

    public static final Logger LOG = LogManager.getLogger(RecordPlanForMvPreRewrite.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (!PreMaterializedViewRewriter.needRecordTmpPlanForRewrite(cascadesContext)) {
            return plan;
        }
        // plan pre normalize
        try {
            ConnectContext connectContext = cascadesContext.getConnectContext();
            StatementContext originalStatementContext = connectContext.getStatementContext();
            try (StatementContext temporaryStatementContext = statementContext.forkForTemporaryRewrite()) {
                try {
                    // Some rewrite utilities access the thread-local ConnectContext directly. Switch it together
                    // with CascadesContext so every temporary CTE cache stays isolated from the main rewrite.
                    connectContext.setStatementContext(temporaryStatementContext);
                    CascadesContext temporaryCascadesContext = CascadesContext.initContext(
                            temporaryStatementContext, plan,
                            cascadesContext.getCurrentJobContext().getRequiredProperties());
                    AtomicReference<Plan> finalPlan = new AtomicReference<>();
                    temporaryCascadesContext.withPlanProcess(cascadesContext.showPlanProcess(),
                            () -> finalPlan.set(MaterializedViewUtils.rewriteByRules(
                                    temporaryCascadesContext, this::rewriteCteChildren, plan, plan, false)));
                    cascadesContext.addPlanProcesses(temporaryCascadesContext.getPlanProcesses());
                    statementContext.addTmpPlanForMvRewrite(finalPlan.get());
                } finally {
                    connectContext.setStatementContext(originalStatementContext);
                }
            }
        } catch (Exception e) {
            LOG.error("mv rewrite in rbo rewrite pre normalize fail, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier(), e);
        }
        return plan;
    }

    private Plan rewriteCteChildren(CascadesContext childContext) {
        Rewriter.getCteChildrenRewriter(childContext,
                        ImmutableList.of(Rewriter.custom(
                                RuleType.REWRITE_CTE_CHILDREN,
                                () -> new RewriteCteChildren(
                                        Rewriter.CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED,
                                        false))))
                .execute();
        return childContext.getRewritePlan();
    }
}
