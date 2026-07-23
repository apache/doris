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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal orchestration entry point for incremental refresh.
 */
public class IvmRefreshManager {
    private static final Logger LOG = LogManager.getLogger(IvmRefreshManager.class);
    public static final String DEBUG_POINT_FORCE_FALLBACK_REASON =
            "IvmRefreshManager.doRefresh.force_fallback_reason";

    public IvmRefreshManager() {
    }

    public IvmRefreshResult doRefresh(IvmRefreshContext context) {
        Objects.requireNonNull(context, "context can not be null");
        MTMV mtmv = context.getMtmv();
        Objects.requireNonNull(context.getAuditStmt(), "auditStmt can not be null");
        Objects.requireNonNull(context.getQueryIdConsumer(), "queryIdConsumer can not be null");
        String forceFallbackReason = DebugPointUtil.getDebugParamOrDefault(
                DEBUG_POINT_FORCE_FALLBACK_REASON, "reason", "");
        if (!forceFallbackReason.isEmpty()) {
            return IvmRefreshResult.fallback(
                    IvmFailureReason.valueOf(forceFallbackReason), "forced by debug point");
        }
        try {
            return doRefreshInternal(context);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("IVM refresh execution failed for mv=" + mtmv.getName()
                    + ", detail=" + Util.getRootCauseMessage(e), e);
        }
    }

    private IvmRefreshResult doRefreshInternal(IvmRefreshContext context) throws Exception {
        Objects.requireNonNull(context, "context can not be null");
        MTMV mtmv = context.getMtmv();
        try {
            executeInternalRefresh(context);
            return IvmRefreshResult.success();
        } catch (IvmException e) {
            IvmRefreshResult result = fallbackResult(e.getFailureReason(), e.getMessage());
            LOG.warn("IVM refresh fell back for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        } catch (Exception e) {
            String detail = Util.getRootCauseMessage(e);
            Optional<IvmFailureReason> failureReason = IvmFailureClassifier.classifyExecutionFailure(detail);
            if (!failureReason.isPresent()) {
                throw e;
            }
            IvmRefreshResult result = fallbackResult(failureReason.get(), detail);
            LOG.warn("IVM execution guard fell back for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        } finally {
            context.getQueryIdConsumer().accept(DebugUtil.printId(context.getConnectContext().queryId()));
        }
    }

    private IvmRefreshResult fallbackResult(IvmFailureReason failureReason, String detail) {
        return IvmRefreshResult.fallback(failureReason, detail);
    }

    @VisibleForTesting
    void executeInternalRefresh(IvmRefreshContext context) throws Exception {
        MTMV mtmv = context.getMtmv();
        StatementContext statementContext = new StatementContext(
                context.getConnectContext(), new OriginStatement(mtmv.getQuerySql(), 0));
        statementContext.setIvmRewriteContext(Optional.of(IvmRewriteContext.incremental(mtmv, false)));
        InsertIntoTableCommand command = buildInsertCommand(mtmv);
        MTMVPlanUtil.executeCommand(context.getConnectContext(), command,
                statementContext, context.getAuditStmt());
    }

    @VisibleForTesting
    public InsertIntoTableCommand buildInsertCommand(MTMV mtmv) {
        return buildInsertCommand(parseInsertQueryPlan(mtmv), mtmv);
    }

    @VisibleForTesting
    InsertIntoTableCommand buildInsertCommand(LogicalPlan queryPlan, MTMV mtmv) {
        Objects.requireNonNull(queryPlan, "queryPlan can not be null");
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        List<String> sinkColumns = mtmv.getInsertedColumnNames();
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

    private LogicalPlan parseInsertQueryPlan(MTMV mtmv) {
        Plan plan = new NereidsParser().parseSingle(mtmv.getQuerySql());
        if (plan instanceof Sink) {
            plan = plan.child(0);
        }
        return (LogicalPlan) plan;
    }

    public static long markIvmBaselineBroken(MTMV mtmv) {
        mtmv.writeMvLock();
        try {
            // Stage a detached metadata snapshot before catalog update and journaling.
            IvmInfo updatedIvmInfo = new IvmInfo(mtmv.getIvmInfo());
            if (!updatedIvmInfo.isBinlogBroken()) {
                updatedIvmInfo.setBinlogBroken(true);
                persistFullRefreshIvmInfo(mtmv, updatedIvmInfo);
            }
            return mtmv.getIvmBinlogBrokenGeneration();
        } finally {
            mtmv.writeMvUnlock();
        }
    }

    public static void finishIvmFullRefresh(MTMV mtmv, long expectedGeneration,
            IvmPlanSignature planSignature) throws JobException {
        Objects.requireNonNull(planSignature, "planSignature can not be null");
        mtmv.writeMvLock();
        try {
            if (expectedGeneration != mtmv.getIvmBinlogBrokenGeneration()) {
                throw new JobException("Base table metadata changed during COMPLETE refresh, mv="
                        + mtmv.getName());
            }
            IvmInfo updatedIvmInfo = new IvmInfo(mtmv.getIvmInfo());
            updatedIvmInfo.setPlanSignature(planSignature.getSha256());
            updatedIvmInfo.setBinlogBroken(false);
            persistFullRefreshIvmInfo(mtmv, updatedIvmInfo);
            LOG.info("IVM baseline published after full refresh for mv={}, signature={}, canonicalLayout={}",
                    mtmv.getName(), updatedIvmInfo.getPlanSignature(),
                    planSignature.getCanonicalString());
        } finally {
            mtmv.writeMvUnlock();
        }
    }

    private static void persistFullRefreshIvmInfo(MTMV mtmv, IvmInfo ivmInfo) {
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, ivmInfo);
    }

}
