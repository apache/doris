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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Minimal orchestration entry point for incremental refresh.
 */
public class IvmRefreshManager {
    private static final Logger LOG = LogManager.getLogger(IvmRefreshManager.class);
    private final IvmDeltaExecutor deltaExecutor;

    public IvmRefreshManager() {
        this(new IvmDeltaExecutor());
    }

    @VisibleForTesting
    IvmRefreshManager(IvmDeltaExecutor deltaExecutor) {
        this.deltaExecutor = Objects.requireNonNull(deltaExecutor, "deltaExecutor can not be null");
    }

    public IvmRefreshResult doRefresh(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        IvmRefreshResult precheckResult = precheck(mtmv);
        if (!precheckResult.isSuccess()) {
            LOG.warn("IVM precheck failed for mv={}, result={}", mtmv.getName(), precheckResult);
            return precheckResult;
        }
        final IvmRefreshContext context;
        try {
            context = buildRefreshContext(mtmv);
        } catch (Exception e) {
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, e.getMessage());
            LOG.warn("IVM context build failed for mv={}, result={}", mtmv.getName(), result);
            return result;
        }
        return doRefreshInternal(context);
    }

    @VisibleForTesting
    IvmRefreshResult precheck(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        if (mtmv.getIvmInfo().isBinlogBroken()) {
            return IvmRefreshResult.fallback(FallbackReason.BINLOG_BROKEN,
                    "Stream binlog is marked as broken");
        }
        // return checkStreamSupport(mtmv);
        return IvmRefreshResult.success();
    }

    @VisibleForTesting
    IvmRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
        ConnectContext connectContext = MTMVPlanUtil.createMTMVContext(mtmv,
                MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        MTMVRefreshContext mtmvRefreshContext = MTMVRefreshContext.buildContext(mtmv);
        return new IvmRefreshContext(mtmv, connectContext, mtmvRefreshContext);
    }

    @VisibleForTesting
    List<DeltaCommandBundle> analyzeDeltaCommandBundles(IvmRefreshContext context) throws Exception {
        MTMVAnalyzeQueryInfo queryInfo = MTMVPlanUtil.analyzeQueryWithSql(
                context.getMtmv(), context.getConnectContext(), true);
        IvmNormalizeResult normalizeResult = queryInfo.getIvmNormalizeResult();
        Plan normalizedPlan = queryInfo.getIvmNormalizedPlan();
        if (normalizedPlan == null) {
            return Collections.emptyList();
        }
        IvmDeltaRewriteContext rewriteCtx = new IvmDeltaRewriteContext(
                context.getMtmv(), context.getConnectContext(), normalizeResult);
        return new IvmDeltaRewriter().rewrite(normalizedPlan, rewriteCtx);
    }

    private IvmRefreshResult doRefreshInternal(IvmRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");

        // Run Nereids with IVM rewrite enabled — per-pattern delta rules write bundles to CascadesContext
        List<DeltaCommandBundle> bundles;
        try {
            bundles = analyzeDeltaCommandBundles(context);
        } catch (Exception e) {
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    FallbackReason.PLAN_PATTERN_UNSUPPORTED, e.getMessage());
            LOG.warn("IVM plan analysis failed for mv={}, result={}", context.getMtmv().getName(), result);
            return result;
        }

        if (bundles == null || bundles.isEmpty()) {
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    FallbackReason.PLAN_PATTERN_UNSUPPORTED, "No IVM delta rule matched the MV define plan");
            LOG.warn("IVM no delta command bundles for mv={}, result={}", context.getMtmv().getName(), result);
            return result;
        }

        try {
            deltaExecutor.execute(context, bundles);
            return IvmRefreshResult.success();
        } catch (Exception e) {
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    FallbackReason.INCREMENTAL_EXECUTION_FAILED, e.getMessage());
            LOG.warn("IVM execution failed for mv={}, result={}", context.getMtmv().getName(), result, e);
            return result;
        }
    }

    private IvmRefreshResult checkStreamSupport(MTMV mtmv) {
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null) {
            return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                    "No base table relation found for incremental refresh");
        }
        Set<BaseTableInfo> baseTables = relation.getBaseTablesOneLevelAndFromView();
        if (baseTables == null || baseTables.isEmpty()) {
            return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                    "No base tables found for incremental refresh");
        }
        Map<BaseTableInfo, IvmStreamRef> baseTableStreams = mtmv.getIvmInfo().getBaseTableStreams();
        if (baseTableStreams == null || baseTableStreams.isEmpty()) {
            return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                    "No stream bindings are registered for this materialized view");
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            IvmStreamRef streamRef = baseTableStreams.get(baseTableInfo);
            if (streamRef == null) {
                return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                        "No stream binding found for base table: " + baseTableInfo);
            }
            if (streamRef.getStreamType() != StreamType.OLAP) {
                return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                        "Only OLAP base table streams are supported for incremental refresh: " + baseTableInfo);
            }
            final TableIf table;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (Exception e) {
                return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                        "Failed to resolve base table metadata for incremental refresh: "
                                + baseTableInfo + ", reason=" + e.getMessage());
            }
            if (!(table instanceof OlapTable)) {
                return IvmRefreshResult.fallback(FallbackReason.STREAM_UNSUPPORTED,
                        "Only OLAP base tables are supported for incremental refresh: " + baseTableInfo);
            }
        }
        return IvmRefreshResult.success();
    }
}
