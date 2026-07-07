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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal orchestration entry point for incremental refresh.
 */
public class IvmRefreshManager {
    private static final Logger LOG = LogManager.getLogger(IvmRefreshManager.class);

    public IvmRefreshManager() {
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
                    IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, e.getMessage());
            LOG.warn("IVM context build failed for mv={}, result={}", mtmv.getName(), result);
            return result;
        }
        try {
            return doRefreshInternal(context);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("IVM refresh execution failed for mv=" + mtmv.getName(), e);
        }
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

    private IvmRefreshResult doRefreshInternal(IvmRefreshContext context) throws Exception {
        Objects.requireNonNull(context, "context can not be null");
        MTMV mtmv = context.getMtmv();
        try {
            executeInternalRefresh(context);
        } catch (IvmException e) {
            // Analysis has not written MV data yet, so unsupported IVM patterns
            // can be represented as a fallback result for the task planner. Preserve
            // the typed failure reason so MTMVTask can decide whether ordinary partition
            // fallback is enough or a full layout-baseline rebuild is required.
            IvmRefreshResult result = IvmRefreshResult.fallback(
                    e.getFailureReason(), e.getMessage());
            LOG.warn("IVM plan analysis failed for mv={}, result={}", mtmv.getName(), result, e);
            return result;
        }
        // TODO: Split analysis/rewrite failures from execution failures so non-IVM exceptions
        // can be classified precisely instead of relying on a single catch boundary here.
        return IvmRefreshResult.success();
    }

    @VisibleForTesting
    void executeInternalRefresh(IvmRefreshContext context) throws Exception {
        MTMV mtmv = context.getMtmv();
        StatementContext statementContext = new StatementContext(
                context.getConnectContext(), new OriginStatement(mtmv.getQuerySql(), 0));
        statementContext.setIvmRewriteContext(Optional.of(
                IvmRewriteContext.incremental(mtmv, false, false)));
        InsertIntoTableCommand command = buildInsertCommand(mtmv);
        MTMVPlanUtil.executeCommand(context.getConnectContext(), command,
                statementContext, mtmv.getQuerySql(), false);
    }

    @VisibleForTesting
    public InsertIntoTableCommand buildInsertCommand(MTMV mtmv) {
        return buildInsertCommand(parseInsertQueryPlan(mtmv), mtmv);
    }

    @VisibleForTesting
    InsertIntoTableCommand buildInsertCommand(LogicalPlan queryPlan, MTMV mtmv) {
        Objects.requireNonNull(queryPlan, "queryPlan can not be null");
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        List<String> sinkColumns = new ArrayList<>(mtmv.getInsertedColumnNames());
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

}
