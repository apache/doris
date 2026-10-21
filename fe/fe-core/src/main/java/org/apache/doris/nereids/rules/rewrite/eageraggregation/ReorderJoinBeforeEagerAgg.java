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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.joinorder.JoinReorderRule;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Reorder joins before eager aggregation. */
public class ReorderJoinBeforeEagerAgg implements CustomRewriter {
    private static final Logger LOG = LoggerFactory.getLogger(ReorderJoinBeforeEagerAgg.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        List<CatalogRelation> scans = plan.collectToList(CatalogRelation.class::isInstance);
        StatsCalculator.disableJoinReorderIfStatsInvalid(scans, jobContext.getCascadesContext());
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext.getSessionVariable().isDisableJoinReorder()
                || jobContext.getCascadesContext().isLeadingDisableJoinReorder()
                || !connectContext.getSessionVariable().enableJoinReorderBeforeEagerAgg) {
            return plan;
        }
        long startNanos = System.nanoTime();
        Plan reorderedPlan = JoinReorderRule.INSTANCE.rewrite(plan, null);
        if (LOG.isDebugEnabled()) {
            double elapsedMs = (System.nanoTime() - startNanos) / 1_000_000.0;
            LOG.debug("{} join reorder before eager aggregation [changed={}, elapsedMs={}]",
                    connectContext.getQueryIdentifier(), reorderedPlan != plan, elapsedMs);
        }
        if (reorderedPlan == plan) {
            return plan;
        }
        return new ColumnPruning().rewriteRoot(reorderedPlan, jobContext);
    }
}
