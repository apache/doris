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
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Record plan for later mv rewrite
 * */
public class RecordPlanAndTryToMvRewrite extends DefaultPlanRewriter<Void> implements CustomRewriter {

    public static final Logger LOG = LogManager.getLogger(RecordPlanAndTryToMvRewrite.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        // todo optimize performance
        List<RewriteJob> recordMvBeforeJobs =
                new ArrayList<>(Rewriter.CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED);
        recordMvBeforeJobs.addAll(
                Rewriter.notTraverseChildrenOf(
                        ImmutableSet.of(LogicalCTEAnchor.class),
                        () -> Rewriter.jobs(
                                Rewriter.topic("Push project and filter on cte consumer to cte producer",
                                        Rewriter.topDown(
                                                new CollectFilterAboveConsumer(),
                                                new CollectCteConsumerOutput())
                                ),
                                Rewriter.topic("Table/Physical optimization",
                                        Rewriter.topDown(
                                                new PruneOlapScanPartition(),
                                                new PruneEmptyPartition(),
                                                new PruneFileScanPartition(),
                                                new PushDownFilterIntoSchemaScan()
                                        )
                                ),
                                Rewriter.topic("necessary rules before record mv",
                                        Rewriter.topDown(new SplitLimit()),
                                        Rewriter.topDown(new AdjustPreAggStatus()),
                                        Rewriter.custom(RuleType.OPERATIVE_COLUMN_DERIVE, OperativeColumnDerive::new),
                                        Rewriter.custom(RuleType.ADJUST_NULLABLE, AdjustNullable::new)
                                )
                        )));
        // plan pre normalize
        Plan finalPlan;
        try {
            finalPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getCteChildrenRewriter(childContext, recordMvBeforeJobs).execute();
                        return childContext.getRewritePlan();
                    }, plan, plan, false, false);
            statementContext.addTmpPlanForMvRewrite(finalPlan);
        } catch (Exception e) {
            LOG.error("mv rewrite in rbo rewrite pre normalize fail, sql hash is {}",
                    cascadesContext.getConnectContext().getSqlHash(), e);
            return plan;
        }
        boolean containMaterializedViewHook = MaterializedViewUtils.containMaterializedViewHook(
                cascadesContext.getStatementContext());
        if (!containMaterializedViewHook) {
            return plan;
        }
        List<Plan> plansWhichContainMv = new ArrayList<>();
        // mv rewrite
        try {
            MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getCteChildrenRewriter(childContext, ImmutableList.of(Rewriter.mvRewriteJob()))
                                .execute();
                        plansWhichContainMv.addAll(childContext.getStatementContext().getRewrittenPlansByMv());
                        // copy the child's materialization context to root cascades for showing explain message
                        childContext.getMaterializationContexts().forEach(cascadesContext::addMaterializationContext);
                        return childContext.getRewritePlan();
                    }, finalPlan, finalPlan, false, true);
        } catch (Exception e) {
            LOG.error("mv rewrite in rbo rewrite fail, sql hash is {}",
                    cascadesContext.getConnectContext().getSqlHash(), e);
            return plan;
        }
        if (plansWhichContainMv.isEmpty()) {
            return plan;
        }
        List<Plan> plansWhichContainMvOptimized = new ArrayList<>();
        // plan which contain mv optimize by mv
        plansWhichContainMv.forEach(planToOptimize -> plansWhichContainMvOptimized.add(
                MaterializedViewUtils.rewriteByRules(cascadesContext,
                        childContext -> {
                            Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(childContext).execute();
                            return childContext.getRewritePlan();
                        }, planToOptimize, planToOptimize, false, false)
        ));
        // clear the rewritten plans by plan pre normalize
        statementContext.getRewrittenPlansByMv().clear();
        plansWhichContainMvOptimized.forEach(statementContext::addRewrittenPlanByMv);
        return plan;
    }
}
