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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
        IvmRewriteResult rewriteResult = queryInfo.getIvmRewriteResult();
        Plan normalizedPlan = queryInfo.getIvmNormalizedPlan();
        if (normalizedPlan == null) {
            return Collections.emptyList();
        }

        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), rewriteResult);
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
        IvmRewriteResult rewriteResult = queryInfo.getIvmRewriteResult();
        Plan normalizedPlan = queryInfo.getIvmNormalizedPlan();
        if (normalizedPlan == null) {
            throw new AnalysisException("IVM normalized plan is empty");
        }

        IvmRefreshContext rewriteCtx = new IvmRefreshContext(
                mtmv, context.getConnectContext(), rewriteResult);
        IvmDeltaRewriter rewriter = new IvmDeltaRewriter();
        Plan mergedDeltaPlan = rewriter.generateMergedDeltaPlan(normalizedPlan, rewriteCtx,
                scan -> rewriter.isExcludedTriggerTable(scan, mtmv.getExcludedTriggerTables()), true);
        return new IvmRefreshExplainResult(normalizedPlan, mergedDeltaPlan);
    }

    @VisibleForTesting
    void validatePlanSignature(MTMV mtmv, MTMVAnalyzeQueryInfo queryInfo) {
        IvmRewriteResult rewriteResult = queryInfo.getIvmRewriteResult();
        IvmPlanSignature currentSignature = rewriteResult == null ? null : rewriteResult.getPlanSignature();
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
        try {
            executeInternalRefresh(context);
        } catch (IvmException e) {
            // Analysis has not written MV data yet, so unsupported IVM patterns
            // can be represented as a fallback result for the task planner. Preserve
            // the typed failure reason so MTMVTask can decide whether ordinary partition
            // fallback is enough or a full layout-baseline rebuild is required.
            IvmPlanSignature currentSignature = e.getFailureReason() == IvmFailureReason.PLAN_SIGNATURE_MISMATCH
                    ? currentPlanSignatureForFallback
                    : null;
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
        return IvmRefreshResult.success();
    }

    @VisibleForTesting
    void executeInternalRefresh(IvmRefreshContext context) throws Exception {
        MTMV mtmv = context.getMtmv();
        StatementContext statementContext = new StatementContext(
                context.getConnectContext(), new OriginStatement(mtmv.getQuerySql(), 0));
        statementContext.setIvmRewriteContext(Optional.of(
                IvmRewriteContext.incremental(mtmv, false, false)));
        InsertIntoTableCommand command = buildInternalInsertCommand(mtmv);
        MTMVPlanUtil.executeCommand(context.getConnectContext(), command,
                statementContext, mtmv.getQuerySql(), false);
    }

    private InsertIntoTableCommand buildInternalInsertCommand(MTMV mtmv) {
        List<StatementBase> statements = new NereidsParser().parseSQL(mtmv.getQuerySql());
        LogicalPlan queryPlan = ((LogicalPlanAdapter) statements.get(0)).getLogicalPlan();
        List<String> sinkColumns = new ArrayList<>(mtmv.getInsertedColumnNames());
        sinkColumns.add(Column.DELETE_SIGN);
        List<String> mvNameParts = ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
        UnboundTableSink<LogicalPlan> sink = new UnboundTableSink<>(
                mvNameParts, sinkColumns, ImmutableList.of(),
                false, ImmutableList.of(), false,
                TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.INSERT,
                Optional.empty(), Optional.empty(), queryPlan);
        return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty());
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

}
