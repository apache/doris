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
                    IvmFallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, e.getMessage());
            LOG.warn("IVM context build failed for mv={}, result={}", mtmv.getName(), result);
            return result;
        }
        return doRefreshInternal(context);
    }

    @VisibleForTesting
    IvmRefreshResult precheck(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        if (mtmv.getIvmInfo().isBinlogBroken()) {
            return IvmRefreshResult.fallback(IvmFallbackReason.BINLOG_BROKEN,
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
    List<IvmDeltaCommandBundle> analyzeDeltaCommandBundles(IvmRefreshContext context) throws Exception {
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
        List<IvmDeltaCommandBundle> bundles;
        try {
            bundles = analyzeDeltaCommandBundles(context);
        } catch (Exception e) {
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    IvmFallbackReason.PLAN_PATTERN_UNSUPPORTED, detail);
            LOG.warn("IVM plan analysis failed for mv={}, result={}", context.getMtmv().getName(), result, e);
            return result;
        }

        if (bundles == null || bundles.isEmpty()) {
            // All base tables are up to date — no delta to apply. This is a success (no-op).
            LOG.info("IVM no delta bundles for mv={} (all base tables up to date)", context.getMtmv().getName());
            return IvmRefreshResult.success();
        }

        // Consume one ExprId from the analysis StatementContext to obtain the next safe start
        // value for execution. This prevents ExprId collisions between plan-embedded ExprIds
        // (allocated during analyzeDeltaCommandBundles) and new ExprIds allocated during
        // bundle execution (in a fresh StatementContext). See: apache/doris#58494.
        // StatementContext may be null in unit-test paths where analyzeDeltaCommandBundles is mocked out;
        // in that case start from 0 (safe because no real plan ExprIds exist).
        org.apache.doris.nereids.StatementContext analysisStmtCtx =
                context.getConnectContext().getStatementContext();
        int exprIdStart = analysisStmtCtx != null
                ? analysisStmtCtx.getNextExprId().asInt() : 0;
        try {
            deltaExecutor.execute(context, bundles, exprIdStart);
            return IvmRefreshResult.success();
        } catch (Exception e) {
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            if (detail.contains("IVM: deleted row may be current")) {
                IvmRefreshResult result = IvmRefreshResult.fallback(
                        IvmFallbackReason.MIN_MAX_BOUNDARY_HIT, detail);
                LOG.info("IVM MIN/MAX boundary hit for mv={}, falling back to COMPLETE refresh, result={}",
                        context.getMtmv().getName(), result);
                return result;
            }
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    IvmFallbackReason.INCREMENTAL_EXECUTION_FAILED, detail);
            LOG.warn("IVM execution failed for mv={}, result={}", context.getMtmv().getName(), result, e);
            return result;
        }
    }

    private IvmRefreshResult checkStreamSupport(MTMV mtmv) {
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null) {
            return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                    "No base table relation found for incremental refresh");
        }
        Set<BaseTableInfo> baseTables = relation.getBaseTablesOneLevelAndFromView();
        if (baseTables == null || baseTables.isEmpty()) {
            return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                    "No base tables found for incremental refresh");
        }
        Map<BaseTableInfo, IvmStreamRef> baseTableStreams = mtmv.getIvmInfo().getBaseTableStreams();
        if (baseTableStreams == null || baseTableStreams.isEmpty()) {
            return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                    "No stream bindings are registered for this materialized view");
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            IvmStreamRef streamRef = baseTableStreams.get(baseTableInfo);
            if (streamRef == null) {
                return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                        "No stream binding found for base table: " + baseTableInfo);
            }
            final TableIf table;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (Exception e) {
                return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                        "Failed to resolve base table metadata for incremental refresh: "
                                + baseTableInfo + ", reason=" + e.getMessage());
            }
            if (!(table instanceof OlapTable)) {
                return IvmRefreshResult.fallback(IvmFallbackReason.STREAM_UNSUPPORTED,
                        "Only OLAP base tables are supported for incremental refresh: " + baseTableInfo);
            }
        }
        return IvmRefreshResult.success();
    }
}
