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

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Record plan for later mv rewrite
 * */
public class RecordPlanForMvLaterRewrite extends DefaultPlanRewriter<Void> implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        List<RewriteJob> recordMvBeforeJobs =
                new ArrayList<>(Rewriter.CTE_CHILDREN_REWRITE_JOBS_BEFORE_SUB_PATH_PUSH_DOWN_STAGE_1);
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
        Plan finalPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                childContext -> {
                    Rewriter.getCteChildrenRewriter(childContext, recordMvBeforeJobs).execute();
                    return childContext.getRewritePlan();
                }, plan, plan);
        statementContext.addTmpPlanForLaterMvRewrite(finalPlan);
        return plan;
    }
}
