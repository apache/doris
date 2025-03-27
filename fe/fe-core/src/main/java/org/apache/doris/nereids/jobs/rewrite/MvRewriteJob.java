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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * RBO MV Rewrite job
 */
public class MvRewriteJob implements RewriteJob {

    private static final Logger LOG = LogManager.getLogger(MvRewriteJob.class);

    @Override
    public void execute(JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        for (PlannerHook hook : cascadesContext.getStatementContext().getPlannerHooks()) {
            // call hook manually
            hook.afterAnalyze(cascadesContext);
        }
        if (cascadesContext.getMaterializationContexts().isEmpty()) {
            return;
        }
        // Do optimize
        new Optimizer(cascadesContext).execute();
        // Chose the best physical plan
        Group root = cascadesContext.getMemo().getRoot();
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(root,
                cascadesContext.getCurrentJobContext().getRequiredProperties(), cascadesContext);
        Pair<CascadesContext, BitSet> collectTableContext = Pair.of(cascadesContext, new BitSet());
        final Set<Boolean> usedMv = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Pair<CascadesContext, BitSet>>() {
            @Override
            public Void visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation,
                    Pair<CascadesContext, BitSet> ctx) {
                ctx.value().set(ctx.key().getStatementContext().getTableId(catalogRelation.getTable()).asInt());
                if (catalogRelation.getTable() instanceof MTMV) {
                    usedMv.add(true);
                }
                return null;
            }
        }, collectTableContext);
        // Calc the table id set which is used by physical plan
        boolean tmpEnableNestMaterializedViewRewrite =
                cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite;
        try {
            cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite = true;
            cascadesContext.getMemo().incrementAndGetRefreshVersion();
            root.getstructInfoMap().refresh(root, cascadesContext, new HashSet<>());
        } finally {
            cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite =
                    tmpEnableNestMaterializedViewRewrite;
        }
        // Extract logical plan by table id set by the corresponding best physical plan
        Collection<StructInfo> structInfo = root.getstructInfoMap().getStructInfo(cascadesContext,
                collectTableContext.second, root, null);
        if (!structInfo.isEmpty() && !usedMv.isEmpty()) {
            structInfo.forEach(info ->
                    cascadesContext.getStatementContext().addRewrittenPlanByMv(info.getOriginalPlan()));
        }
    }

    @Override
    public boolean isOnce() {
        return true;
    }
}
