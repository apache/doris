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
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
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

    @VisibleForTesting
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewRewrite()) {
            return;
        }
        doInitMaterializationContext(cascadesContext);
    }

    /**
     * Init materialization context
     * @param cascadesContext current cascadesContext in the planner
     */
    protected void doInitMaterializationContext(CascadesContext cascadesContext) {
        // Only collect the table or mv which query use directly, to avoid useless mv partition in rewrite
        TableCollectorContext collectorContext = new TableCollectorContext(Sets.newHashSet(), false);
        try {
            Plan rewritePlan = cascadesContext.getRewritePlan();
            // Keep use one connection context when in query, if new connect context,
            // the ConnectionContext.get() will change
            collectorContext.setConnectContext(cascadesContext.getConnectContext());
            rewritePlan.accept(TableCollector.INSTANCE, collectorContext);
        } catch (Exception e) {
            LOG.warn(String.format("MaterializationContext init table collect fail, current queryId is %s",
                    cascadesContext.getConnectContext().getQueryIdentifier()), e);
            return;
        }
        Set<TableIf> collectedTables = collectorContext.getCollectedTables();
        if (collectedTables.isEmpty()) {
            return;
        }
        // Create async materialization context
        for (MaterializationContext context : createAsyncMaterializationContext(cascadesContext,
                collectorContext.getCollectedTables())) {
            cascadesContext.addMaterializationContext(context);
        }
    }

    protected Set<MTMV> getAvailableMTMVs(Set<TableIf> usedTables, CascadesContext cascadesContext) {
        List<BaseTableInfo> usedBaseTables =
                usedTables.stream().map(BaseTableInfo::new).collect(Collectors.toList());
        return Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(usedBaseTables, cascadesContext.getConnectContext(),
                        false, ((connectContext, mtmv) -> {
                            return MTMVUtil.mtmvContainsExternalTable(mtmv) && (!connectContext.getSessionVariable()
                                    .isEnableMaterializedViewRewriteWhenBaseTableUnawareness());
                        }));
    }

    private List<MaterializationContext> createAsyncMaterializationContext(CascadesContext cascadesContext,
            Set<TableIf> usedTables) {
        Set<MTMV> availableMTMVs = getAvailableMTMVs(usedTables, cascadesContext);
        if (availableMTMVs.isEmpty()) {
            LOG.debug(String.format("Enable materialized view rewrite but availableMTMVs is empty, current queryId "
                    + "is %s", cascadesContext.getConnectContext().getQueryIdentifier()));
            return ImmutableList.of();
        }
        List<MaterializationContext> asyncMaterializationContext = new ArrayList<>();
        for (MTMV materializedView : availableMTMVs) {
            MTMVCache mtmvCache = null;
            try {
                mtmvCache = materializedView.getOrGenerateCache(cascadesContext.getConnectContext());
                if (mtmvCache == null) {
                    continue;
                }
                // For async materialization context, the cascades context when construct the struct info maybe
                // different from the current cascadesContext
                // so regenerate the struct info table bitset
                StructInfo mvStructInfo = mtmvCache.getStructInfo();
                BitSet tableBitSetInCurrentCascadesContext = new BitSet();
                mvStructInfo.getRelations().forEach(relation -> tableBitSetInCurrentCascadesContext.set(
                        cascadesContext.getStatementContext().getTableId(relation.getTable()).asInt()));
                asyncMaterializationContext.add(new AsyncMaterializationContext(materializedView,
                        mtmvCache.getLogicalPlan(), mtmvCache.getOriginalPlan(), ImmutableList.of(),
                        ImmutableList.of(), cascadesContext,
                        mtmvCache.getStructInfo().withTableBitSet(tableBitSetInCurrentCascadesContext)));
            } catch (Exception e) {
                LOG.warn(String.format("MaterializationContext init mv cache generate fail, current queryId is %s",
                        cascadesContext.getConnectContext().getQueryIdentifier()), e);
            }
        }
        return asyncMaterializationContext;
    }
}
