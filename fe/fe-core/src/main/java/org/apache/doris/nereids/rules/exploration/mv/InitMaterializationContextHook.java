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
import org.apache.doris.nereids.StatementContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
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
    public void afterRewrite(NereidsPlanner planner) {
        CascadesContext cascadesContext = planner.getCascadesContext();
        // collect partitions table used, this is for query rewrite by materialized view
        // this is needed before init hook, because compare partition version in init hook would use this
        MaterializedViewUtils.collectTableUsedPartitions(cascadesContext.getRewritePlan(), cascadesContext);
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsCollectTablePartitionFinishTime();
        }
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
        if (cascadesContext.getConnectContext().getSessionVariable().isInDebugMode()) {
            LOG.info("MaterializationContext init return because is in debug mode, current queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return;
        }
        Set<TableIf> collectedTables = Sets.newHashSet(cascadesContext.getStatementContext().getTables().values());
        if (collectedTables.isEmpty()) {
            return;
        }
        // Create async materialization context
        for (MaterializationContext context : createAsyncMaterializationContext(cascadesContext,
                collectedTables)) {
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
        Set<MTMV> availableMTMVs;
        try {
            availableMTMVs = getAvailableMTMVs(usedTables, cascadesContext);
        } catch (Exception e) {
            LOG.warn(String.format("MaterializationContext getAvailableMTMVs generate fail, current sqlHash is %s",
                    cascadesContext.getConnectContext().getSqlHash()), e);
            return ImmutableList.of();
        }
        if (CollectionUtils.isEmpty(availableMTMVs)) {
            LOG.debug("Enable materialized view rewrite but availableMTMVs is empty, current sqlHash "
                    + "is {}", cascadesContext.getConnectContext().getSqlHash());
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
                BitSet relationIdBitSetInCurrentCascadesContext = new BitSet();
                mvStructInfo.getRelations().forEach(relation -> {
                    tableBitSetInCurrentCascadesContext.set(
                            cascadesContext.getStatementContext().getTableId(relation.getTable()).asInt());
                    relationIdBitSetInCurrentCascadesContext.set(relation.getRelationId().asInt());
                });
                asyncMaterializationContext.add(new AsyncMaterializationContext(materializedView,
                        mtmvCache.getLogicalPlan(), mtmvCache.getOriginalPlan(), ImmutableList.of(),
                        ImmutableList.of(), cascadesContext,
                        mtmvCache.getStructInfo().withTableBitSet(tableBitSetInCurrentCascadesContext,
                                relationIdBitSetInCurrentCascadesContext)));
            } catch (Exception e) {
                LOG.warn(String.format("MaterializationContext init mv cache generate fail, current queryId is %s",
                        cascadesContext.getConnectContext().getQueryIdentifier()), e);
            }
        }
        return asyncMaterializationContext;
    }
}
