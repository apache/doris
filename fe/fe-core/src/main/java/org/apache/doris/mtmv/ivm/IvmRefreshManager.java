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

        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), normalizeResult);
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

        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), normalizeResult);
        IvmDeltaRewriter rewriter = new IvmDeltaRewriter();
        Plan mergedDeltaPlan = rewriter.generateMergedDeltaPlan(normalizedPlan, rewriteCtx,
                scan -> rewriter.isExcludedTriggerTable(scan, mtmv.getExcludedTriggerTables()), true);
        return new IvmRefreshExplainResult(normalizedPlan, mergedDeltaPlan);
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


    private IvmRefreshResult doRefreshInternal(IvmRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");
        MTMV mtmv = context.getMtmv();

        // Run Nereids with IVM rewrite enabled — per-pattern delta rules write bundles to CascadesContext
        List<Command> commands;
        try {
            commands = analyzeDeltaCommands(context);
        } catch (IvmException e) {
            // Analysis has not written MV data yet, so unsupported IVM patterns
            // can be represented as a fallback result for the task planner. Preserve
            // the typed failure reason so MTMVTask can decide whether ordinary partition
            // fallback is enough or a full layout-baseline rebuild is required.
            IvmPlanSignature currentSignature = e.getFailureReason() == IvmFailureReason.PLAN_SIGNATURE_MISMATCH
                    ? currentPlanSignatureForFallback : null;
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    e.getFailureReason(), e.getMessage(), currentSignature);
            LOG.warn("IVM plan analysis failed for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        } catch (Exception e) {
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            // Unknown analysis errors are still pre-execution failures. Return a
            // fallback result instead of throwing so AUTO/INCREMENTAL FALLBACK
            // can try PARTITIONS/COMPLETE.
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
            // require full refresh recovery, which resets the flag on success.
            // Do not return a fallback result here: delta commands are executed
            // one by one and may already have partially modified the MV.
            String detail = e.getMessage() != null ? e.getMessage()
                    : e.getClass().getName() + " (no message)";
            LOG.warn("IVM execution failed for mv={}, detail={}", mtmv.getName(), detail, e);
            throw new IvmException(IvmFailureClassifier.classifyExecutionFailure(detail)
                    .orElse(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED), detail);
        }

        // Advance consumedTso to latestTso for all base tables and clear the flag,
        // persisting everything in one editlog entry.
        advanceStreamOffsetAndClearFlag(mtmv);

        return IvmRefreshResult.success();
    }

    /**
     * After successful bundle execution, advances each base table's stream offset
     * and clears the runningIvmRefresh flag, then persists via editlog.
     *
     * TODO: Implement stream offset advancement via OlapTableStream.unprotectedUpdateStreamUpdate()
     * once streams are auto-created in Phase 1.
     */
    private void advanceStreamOffsetAndClearFlag(MTMV mtmv) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        // TODO: advance stream offsets for each base table
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
     * {@code runningIvmRefresh=true}. Resets each base table's stream offset to the
     * pre-captured snapshot TSO and clears the flag.
     *
     * TODO: Implement stream offset reset via OlapTableStream.unprotectedUpdateStreamUpdate()
     * once streams are auto-created in Phase 1. For now, just clear the flag.
     */
    public static void resetIvmStateAfterFullRefresh(MTMV mtmv,
            Map<BaseTableInfo, Long> capturedTsos) {
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        clearRunningIvmRefreshAfterFullRefresh(ivmInfo);
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

    // resetIvmStateAfterFullRefresh(IvmInfo, Map) removed — stream offsets are now
    // reset directly via OlapTableStream.unprotectedUpdateStreamUpdate() in the public
    // resetIvmStateAfterFullRefresh(MTMV, Map) method.

    /**
     * Captures the current visible TSO for each base table. Should be called
     * BEFORE a full refresh executes, so the captured values represent the snapshot
     * that the refresh will read. On failure, logs a warning and returns an empty map.
     *
     * TODO: Update to get table list from MTMV relation (not IvmStreamRef) once
     * streams are auto-created in Phase 1.
     */
    public static Map<BaseTableInfo, Long> captureBaseTableTsos(MTMV mtmv) {
        Map<BaseTableInfo, Long> result = new HashMap<>();
        Set<BaseTableInfo> baseTables = getBaseTablesForIvmState(mtmv);
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

}
