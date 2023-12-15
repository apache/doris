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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** If enable query rewrite with mv, should init materialization context after analyze*/
public class InitMaterializationContextHook implements PlannerHook {

    public static final Logger LOG = LogManager.getLogger(InitMaterializationContextHook.class);
    public static final InitMaterializationContextHook INSTANCE = new InitMaterializationContextHook();

    @Override
    public void afterAnalyze(NereidsPlanner planner) {
        initMaterializationContext(planner.getCascadesContext());
    }

    private void initMaterializationContext(CascadesContext cascadesContext) {

        Plan rewritePlan = cascadesContext.getRewritePlan();
        TableCollectorContext collectorContext = new TableCollectorContext(Sets.newHashSet());
        rewritePlan.accept(TableCollector.INSTANCE, collectorContext);
        List<TableIf> collectedTables = collectorContext.getCollectedTables();
        if (collectedTables.isEmpty()) {
            return;
        }
        List<BaseTableInfo> baseTableUsed =
                collectedTables.stream().map(BaseTableInfo::new).collect(Collectors.toList());
        // TODO the logic should be move to MTMVCacheManager later when getAvailableMaterializedView is ready in
        //  MV Cache manager
        Env env = cascadesContext.getConnectContext().getEnv();
        MTMVCacheManager cacheManager = env.getMtmvService().getCacheManager();
        Set<BaseTableInfo> materializedViews = new HashSet<>();
        for (BaseTableInfo baseTableInfo : baseTableUsed) {
            Set<BaseTableInfo> mtmvsByBaseTable = cacheManager.getMtmvsByBaseTable(baseTableInfo);
            if (mtmvsByBaseTable == null || mtmvsByBaseTable.isEmpty()) {
                continue;
            }
            materializedViews.addAll(mtmvsByBaseTable);
        }
        if (materializedViews.isEmpty()) {
            return;
        }
        materializedViews.forEach(mvBaseTableInfo -> {
            try {
                MTMV materializedView = (MTMV) Env.getCurrentInternalCatalog()
                        .getDbOrMetaException(mvBaseTableInfo.getDbId())
                        .getTableOrMetaException(mvBaseTableInfo.getTableId(), TableType.MATERIALIZED_VIEW);

                // generate outside, maybe add partition filter in the future
                LogicalOlapScan mvScan = new LogicalOlapScan(
                        cascadesContext.getStatementContext().getNextRelationId(),
                        (OlapTable) materializedView,
                        ImmutableList.of(materializedView.getQualifiedDbName()),
                        // this must be empty, or it will be used to sample
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Optional.empty());
                mvScan = mvScan.withMaterializedIndexSelected(PreAggStatus.on(), materializedView.getBaseIndexId());
                List<NamedExpression> mvProjects = mvScan.getOutput().stream().map(NamedExpression.class::cast)
                        .collect(Collectors.toList());
                // todo should force keep consistency to mv sql plan output
                Plan projectScan = new LogicalProject<Plan>(mvProjects, mvScan);
                cascadesContext.addMaterializationContext(
                        MaterializationContext.fromMaterializedView(materializedView, projectScan, cascadesContext));
            } catch (MetaNotFoundException metaNotFoundException) {
                LOG.error(mvBaseTableInfo.toString() + " can not find corresponding materialized view.");
            }
        });
    }
}
