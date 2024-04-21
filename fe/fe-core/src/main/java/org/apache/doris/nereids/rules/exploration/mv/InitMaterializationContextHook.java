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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * If enable query rewrite with mv, should init materialization context after analyze
 */
public class InitMaterializationContextHook implements PlannerHook {

    public static final Logger LOG = LogManager.getLogger(InitMaterializationContextHook.class);
    public static final InitMaterializationContextHook INSTANCE = new InitMaterializationContextHook();

    @Override
    public void afterAnalyze(NereidsPlanner planner) {
        initMaterializationContext(planner.getCascadesContext());
    }

    /**
     * init materialization context
     */
    @VisibleForTesting
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewRewrite()) {
            return;
        }
        Plan rewritePlan = cascadesContext.getRewritePlan();
        TableCollectorContext collectorContext = new TableCollectorContext(Sets.newHashSet(), true);
        rewritePlan.accept(TableCollector.INSTANCE, collectorContext);
        Set<TableIf> collectedTables = collectorContext.getCollectedTables();
        if (collectedTables.isEmpty()) {
            return;
        }
        List<BaseTableInfo> usedBaseTables =
                collectedTables.stream().map(BaseTableInfo::new).collect(Collectors.toList());
        Set<MTMV> availableMTMVs = Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(usedBaseTables, cascadesContext.getConnectContext());
        if (availableMTMVs.isEmpty()) {
            LOG.warn(String.format("enable materialized view rewrite but availableMTMVs is empty, current queryId "
                            + "is %s", cascadesContext.getConnectContext().getQueryIdentifier()));
            return;
        }
        for (MTMV materializedView : availableMTMVs) {
            cascadesContext.addMaterializationContext(
                    MaterializationContext.fromMaterializedView(materializedView,
                            MaterializedViewUtils.generateMvScanPlan(materializedView, cascadesContext),
                            cascadesContext));
        }
    }
}
