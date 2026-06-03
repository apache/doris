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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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
    private IvmPlanSignature currentPlanSignatureForFallback;

    public IvmRefreshManager() {
        this(new IvmDeltaExecutor());
    }

    @VisibleForTesting
    IvmRefreshManager(IvmDeltaExecutor deltaExecutor) {
        this.deltaExecutor = Objects.requireNonNull(deltaExecutor, "deltaExecutor can not be null");
    }

    public IvmRefreshResult doRefresh(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        currentPlanSignatureForFallback = null;
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
                    IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, e.getMessage());
            LOG.warn("IVM context build failed for mv={}, result={}", mtmv.getName(), result);
            return result;
        }
        return doRefreshInternal(context);
    }

    @VisibleForTesting
    IvmRefreshResult precheck(MTMV mtmv) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        if (mtmv.getIvmInfo().isRunningIvmRefresh()) {
            return IvmRefreshResult.fallback(IvmFailureReason.PREVIOUS_RUN_INCOMPLETE,
                    "A previous incremental refresh did not complete; full refresh is required");
        }
        if (mtmv.getIvmInfo().isBinlogBroken()) {
            return IvmRefreshResult.fallback(IvmFailureReason.BINLOG_BROKEN,
                    "Stream binlog is marked as broken");
        }
        // return checkStreamSupport(mtmv);
        return IvmRefreshResult.success();
    }

    @VisibleForTesting
    IvmRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
        ConnectContext connectContext = MTMVPlanUtil.createMTMVContext(mtmv,
                MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        return new IvmRefreshContext(mtmv, connectContext);
    }

    @VisibleForTesting
    List<Command> analyzeDeltaCommands(IvmRefreshContext context) throws Exception {
        MTMV mtmv = context.getMtmv();
        MTMVAnalyzeQueryInfo queryInfo = MTMVPlanUtil.analyzeQueryWithSql(
                mtmv, context.getConnectContext(), true);
        validatePlanSignature(mtmv, queryInfo);
        IvmNormalizeResult normalizeResult = queryInfo.getIvmNormalizeResult();
        Plan normalizedPlan = queryInfo.getIvmNormalizedPlan();
        if (normalizedPlan == null) {
            return Collections.emptyList();
        }

        // Ensure base table streams are initialized (first refresh may have empty map)
        ensureBaseTableStreamsInitialized(mtmv);

        // Read latestTso for each base table and pass baseTableStreams to rewrite context
        Map<BaseTableInfo, IvmStreamRef> baseTableStreams = mtmv.getIvmInfo().getBaseTableStreams();
        populateLatestTso(baseTableStreams);

        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), normalizeResult,
                IvmRefreshContext.buildBaseTableStreams(baseTableStreams));
        return new IvmDeltaRewriter().rewrite(normalizedPlan, rewriteCtx);
    }

    /**
     * Builds the IVM normalized plan and all dry-run delta plans for EXPLAIN REFRESH.
     * This method does not mutate persisted IVM state and intentionally includes
     * no-op streams so users can inspect every delta plan shape.
     */
    public IvmRefreshExplainResult explainRefresh(MTMV mtmv) throws Exception {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        IvmRefreshContext context = buildRefreshContext(mtmv);
        MTMVAnalyzeQueryInfo queryInfo = MTMVPlanUtil.analyzeQueryWithSql(
                mtmv, context.getConnectContext(), true);
        validatePlanSignature(mtmv, queryInfo);
        IvmNormalizeResult normalizeResult = queryInfo.getIvmNormalizeResult();
        Plan normalizedPlan = queryInfo.getIvmNormalizedPlan();
        if (normalizedPlan == null) {
            throw new AnalysisException("IVM normalized plan is empty");
        }

        Map<BaseTableInfo, IvmStreamRef> baseTableStreams = buildBaseTableStreamsForExplain(mtmv);
        populateLatestTso(baseTableStreams);
        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), normalizeResult,
                IvmRefreshContext.buildBaseTableStreams(baseTableStreams));
        IvmDeltaRewriter rewriter = new IvmDeltaRewriter();
        List<IvmDeltaExplainBundle> bundles = rewriter
                .generateDeltaExplainBundles(normalizedPlan, rewriteCtx,
                        scan -> rewriter.isExcludedTriggerTable(scan, mtmv.getExcludedTriggerTables()));
        return new IvmRefreshExplainResult(normalizedPlan, bundles);
    }

    private Map<BaseTableInfo, IvmStreamRef> buildBaseTableStreamsForExplain(MTMV mtmv) {
        Map<BaseTableInfo, IvmStreamRef> streams = mtmv.getIvmInfo().getBaseTableStreams();
        Map<BaseTableInfo, IvmStreamRef> explainStreams = new HashMap<>();
        if (streams != null && !streams.isEmpty()) {
            for (Map.Entry<BaseTableInfo, IvmStreamRef> entry : streams.entrySet()) {
                IvmStreamRef source = entry.getValue();
                IvmStreamRef copy = new IvmStreamRef(source.getConsumedTso());
                copy.setLatestTso(source.getLatestTso());
                explainStreams.put(entry.getKey(), copy);
            }
            return explainStreams;
        }

        Set<BaseTableInfo> baseTables = getBaseTablesForIvmState(mtmv);
        if (baseTables == null || baseTables.isEmpty()) {
            return explainStreams;
        }
        for (BaseTableInfo tableInfo : baseTables) {
            explainStreams.put(tableInfo, new IvmStreamRef());
        }
        return explainStreams;
    }

    @VisibleForTesting
    void validatePlanSignature(MTMV mtmv, MTMVAnalyzeQueryInfo queryInfo) {
        IvmNormalizeResult normalizeResult = queryInfo.getIvmNormalizeResult();
        IvmPlanSignature currentSignature = normalizeResult == null ? null : normalizeResult.getPlanSignature();
        currentPlanSignatureForFallback = currentSignature;
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        String storedSignature = ivmInfo.getPlanSignature();
        boolean signatureMatched = currentSignature != null
                && Objects.equals(storedSignature, currentSignature.getSha256());
        if (signatureMatched) {
            return;
        }
        LOG.info("IVM layout signature mismatch for mv={}, storedSignature={}, currentSignature={}, "
                        + "currentCanonicalLayout={}",
                mtmv.getName(), storedSignature,
                currentSignature == null ? "null" : currentSignature.getSha256(),
                currentSignature == null ? "null" : currentSignature.getCanonicalString());
        String detail = "IVM layout signature mismatch for mv=" + mtmv.getName()
                + ", storedSignature=" + storedSignature
                + ", currentSignature=" + (currentSignature == null ? "null" : currentSignature.getSha256())
                + ". Run a full refresh to rebuild IVM layout baseline.";
        throw new IvmException(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, detail);
    }

    /**
     * Reads the current visible TSO from each base table and stores it in the
     * corresponding {@link IvmStreamRef#setLatestTso}. Throws on failure to ensure
     * the caller falls back to full refresh rather than proceeding with stale TSO
     * values (which could cause missed data or false no-op results).
     */
    @VisibleForTesting
    void populateLatestTso(Map<BaseTableInfo, IvmStreamRef> baseTableStreams) {
        if (baseTableStreams == null) {
            return;
        }
        for (Map.Entry<BaseTableInfo, IvmStreamRef> entry : baseTableStreams.entrySet()) {
            BaseTableInfo tableInfo = entry.getKey();
            IvmStreamRef ref = entry.getValue();
            TableIf table;
            try {
                table = MTMVUtil.getTable(tableInfo);
            } catch (Exception e) {
                throw new AnalysisException("IVM: failed to resolve base table: " + tableInfo, e);
            }
            if (!(table instanceof OlapTable)) {
                throw new AnalysisException(
                        "IVM: base table is not OlapTable: " + tableInfo);
            }
            try {
                ref.setLatestTso(((OlapTable) table).getVisibleTso());
            } catch (Exception e) {
                throw new AnalysisException(
                        "IVM: failed to get visible TSO for table: " + tableInfo, e);
            }
        }
    }

    /**
     * Ensures that baseTableStreams in IvmInfo is populated. On the first
     * incremental refresh the map is empty; this method initializes it from
     * the MTMV's relation metadata, creating an IvmStreamRef with
     * consumedTso=0 for each base table.
     *
     * <p>Note: uses getBaseTablesOneLevel() which returns all base tables referenced
     * in the MV query, including multi-table joins. When view support is added,
     * this should align with getBaseTablesOneLevelAndFromView() and also backfill
     * missing entries in partially populated maps.
     */
    @VisibleForTesting
    void ensureBaseTableStreamsInitialized(MTMV mtmv) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        Map<BaseTableInfo, IvmStreamRef> streams = ivmInfo.getBaseTableStreams();
        if (streams != null && !streams.isEmpty()) {
            return;
        }
        Set<BaseTableInfo> baseTables = getBaseTablesForIvmState(mtmv);
        if (baseTables == null || baseTables.isEmpty()) {
            return;
        }
        Map<BaseTableInfo, IvmStreamRef> newStreams = new HashMap<>();
        for (BaseTableInfo tableInfo : baseTables) {
            newStreams.put(tableInfo, new IvmStreamRef());
        }
        ivmInfo.setBaseTableStreams(newStreams);
        LOG.info("IVM initialized baseTableStreams for mv={} with {} base tables",
                mtmv.getName(), newStreams.size());
    }

    private IvmRefreshResult doRefreshInternal(IvmRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");
        MTMV mtmv = context.getMtmv();

        // Run Nereids with IVM rewrite enabled — per-pattern delta rules write bundles to CascadesContext
        List<Command> commands;
        try {
            commands = analyzeDeltaCommands(context);
        } catch (IvmException e) {
            // Preserve the typed failure reason across the refresh boundary so MTMVTask can decide
            // whether ordinary partition fallback is enough or a full layout-baseline rebuild is required.
            IvmPlanSignature currentSignature = e.getFailureReason() == IvmFailureReason.PLAN_SIGNATURE_MISMATCH
                    ? currentPlanSignatureForFallback : null;
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    e.getFailureReason(), e.getMessage(), currentSignature);
            LOG.warn("IVM plan analysis failed for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        } catch (Exception e) {
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, detail);
            LOG.warn("IVM plan analysis failed for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        }

        if (commands == null || commands.isEmpty()) {
            // All base tables are up to date — no delta to apply. This is a success (no-op).
            LOG.info("IVM no delta commands for mv={} (all base tables up to date)", mtmv.getName());
            return IvmRefreshResult.success();
        }

        // Mark incremental refresh in progress and persist BEFORE execution.
        // If FE crashes during execution, on restart the flag triggers full refresh.
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        ivmInfo.setRunningIvmRefresh(true);
        persistIvmInfo(mtmv, ivmInfo);

        // Consume one ExprId from the analysis StatementContext to obtain the next safe start
        // value for execution. This prevents ExprId collisions between plan-embedded ExprIds
        // (allocated during analyzeDeltaCommandBundles) and new ExprIds allocated during
        // bundle execution (in a fresh StatementContext). See: apache/doris#58494.
        // StatementContext may be null in unit-test paths where analyzeDeltaCommandBundles is mocked out;
        // in that case start from 0 (safe because no real plan ExprIds exist).
        StatementContext analysisStmtCtx =
                context.getConnectContext().getStatementContext();
        int exprIdStart = analysisStmtCtx != null
                ? analysisStmtCtx.getNextExprId().asInt() : 0;
        try {
            deltaExecutor.execute(context, commands, exprIdStart);
        } catch (Exception e) {
            // Leave runningIvmRefresh=true — the next task will detect this and
            // fall back to full refresh, which resets the flag on success.
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            if (detail.contains("IVM: deleted row may be current")) {
                IvmRefreshResult result = IvmRefreshResult.fallback(
                        IvmFailureReason.MIN_MAX_BOUNDARY_HIT, detail);
                LOG.info("IVM MIN/MAX boundary hit for mv={}, falling back to COMPLETE refresh, result={}",
                        mtmv.getName(), result);
                return result;
            }
            if (detail.contains("IVM fallback: delete on non-deterministic row_id")) {
                IvmRefreshResult result = IvmRefreshResult.fallback(
                        IvmFailureReason.NON_DETERMINISTIC_ROW_ID, detail);
                LOG.info("IVM non-deterministic row_id for mv={}, falling back to COMPLETE refresh, result={}",
                        mtmv.getName(), result);
                return result;
            }
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, detail);
            LOG.warn("IVM execution failed for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        }

        // Advance consumedTso to latestTso for all base tables and clear the flag,
        // persisting everything in one editlog entry.
        advanceConsumedTsoAndClearFlag(mtmv);

        return IvmRefreshResult.success();
    }

    /**
     * After successful bundle execution, advances each stream's consumedTso to
     * latestTso and clears the runningIvmRefresh flag, then persists via editlog.
     */
    private void advanceConsumedTsoAndClearFlag(MTMV mtmv) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        for (IvmStreamRef ref : ivmInfo.getBaseTableStreams().values()) {
            if (ref.getLatestTso() >= ref.getConsumedTso()) {
                ref.setConsumedTso(ref.getLatestTso());
            }
        }
        ivmInfo.setRunningIvmRefresh(false);
        persistIvmInfo(mtmv, ivmInfo);
    }

    /**
     * Persists the IvmInfo via the AlterMTMV editlog mechanism.
     * Package-private so tests can override to avoid Env dependency.
     */
    @VisibleForTesting
    void persistIvmInfo(MTMV mtmv, IvmInfo ivmInfo) {
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, ivmInfo);
    }

    /**
     * Resets IVM state after a successful full (COMPLETE) refresh. Called from MTMVTask
     * when the partition-based refresh succeeds and a previous IVM run left
     * {@code runningIvmRefresh=true}. Sets each base table's consumedTso to the
     * pre-captured snapshot TSO so the next incremental refresh starts from the correct
     * position, and clears the flag.
     *
     * @param mtmv the materialized view
     * @param capturedTsos TSO values captured before the full refresh executed,
     *                     keyed by BaseTableInfo
     */
    public static void resetIvmStateAfterFullRefresh(MTMV mtmv,
            Map<BaseTableInfo, Long> capturedTsos) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        resetIvmStateAfterFullRefresh(ivmInfo, capturedTsos);
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, ivmInfo);
        LOG.info("IVM state reset after full refresh for mv={}", mtmv.getName());
    }

    public static void clearRunningIvmRefreshAfterFullRefresh(MTMV mtmv) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        clearRunningIvmRefreshAfterFullRefresh(ivmInfo);
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, ivmInfo);
        LOG.info("IVM running refresh flag cleared after full refresh for mv={}", mtmv.getName());
    }

    public static void updatePlanSignatureAfterFullRefresh(MTMV mtmv, String planSignature,
            String canonicalString) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        ivmInfo.setPlanSignature(planSignature);
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, ivmInfo);
        LOG.info("IVM layout signature baseline updated after full refresh for mv={}, signature={}, "
                        + "canonicalLayout={}",
                mtmv.getName(), planSignature, canonicalString == null ? "null" : canonicalString);
    }

    @VisibleForTesting
    static void clearRunningIvmRefreshAfterFullRefresh(IvmInfo ivmInfo) {
        ivmInfo.setRunningIvmRefresh(false);
    }

    @VisibleForTesting
    static void resetIvmStateAfterFullRefresh(IvmInfo ivmInfo,
            Map<BaseTableInfo, Long> capturedTsos) {
        Map<BaseTableInfo, IvmStreamRef> streams = ivmInfo.getBaseTableStreams();
        if (streams == null) {
            streams = new HashMap<>();
            ivmInfo.setBaseTableStreams(streams);
        }
        if (capturedTsos != null) {
            for (Map.Entry<BaseTableInfo, Long> entry : capturedTsos.entrySet()) {
                IvmStreamRef ref = streams.computeIfAbsent(entry.getKey(), key -> new IvmStreamRef());
                ref.setConsumedTso(entry.getValue());
                ref.setLatestTso(entry.getValue());
            }
        }
        ivmInfo.setRunningIvmRefresh(false);
    }

    /**
     * Captures the current visible TSO for each base table stream. Should be called
     * BEFORE a full refresh executes, so the captured values represent the snapshot
     * that the refresh will read. On failure, logs a warning and returns an empty map;
     * the caller should check the result size and skip consumedTso reset if incomplete.
     */
    public static Map<BaseTableInfo, Long> captureBaseTableTsos(MTMV mtmv) {
        Map<BaseTableInfo, Long> result = new HashMap<>();
        Map<BaseTableInfo, IvmStreamRef> streams = mtmv.getIvmInfo().getBaseTableStreams();
        Set<BaseTableInfo> baseTables = streams == null || streams.isEmpty()
                ? getBaseTablesForIvmState(mtmv) : streams.keySet();
        if (baseTables == null || baseTables.isEmpty()) {
            return result;
        }
        for (BaseTableInfo tableInfo : baseTables) {
            try {
                TableIf table = MTMVUtil.getTable(tableInfo);
                if (table instanceof OlapTable) {
                    result.put(tableInfo, ((OlapTable) table).getVisibleTso());
                }
            } catch (Exception e) {
                LOG.warn("IVM: failed to capture TSO for table {} before full refresh: {}. "
                        + "IVM state reset will be skipped.", tableInfo, e.getMessage());
                return Collections.emptyMap();
            }
        }
        return result;
    }

    private static Set<BaseTableInfo> getBaseTablesForIvmState(MTMV mtmv) {
        MTMVRelation relation = mtmv.getRelation();
        return relation == null ? null : relation.getBaseTablesOneLevel();
    }

    private IvmRefreshResult checkStreamSupport(MTMV mtmv) {
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null) {
            return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                    "No base table relation found for incremental refresh");
        }
        Set<BaseTableInfo> baseTables = relation.getBaseTablesOneLevelAndFromView();
        if (baseTables == null || baseTables.isEmpty()) {
            return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                    "No base tables found for incremental refresh");
        }
        Map<BaseTableInfo, IvmStreamRef> baseTableStreams = mtmv.getIvmInfo().getBaseTableStreams();
        if (baseTableStreams == null || baseTableStreams.isEmpty()) {
            return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                    "No stream bindings are registered for this materialized view");
        }
        for (BaseTableInfo baseTableInfo : baseTables) {
            IvmStreamRef streamRef = baseTableStreams.get(baseTableInfo);
            if (streamRef == null) {
                return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                        "No stream binding found for base table: " + baseTableInfo);
            }
            final TableIf table;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (Exception e) {
                return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                        "Failed to resolve base table metadata for incremental refresh: "
                                + baseTableInfo + ", reason=" + e.getMessage());
            }
            if (!(table instanceof OlapTable)) {
                return IvmRefreshResult.fallback(IvmFailureReason.STREAM_UNSUPPORTED,
                        "Only OLAP base tables are supported for incremental refresh: " + baseTableInfo);
            }
        }
        return IvmRefreshResult.success();
    }
}
