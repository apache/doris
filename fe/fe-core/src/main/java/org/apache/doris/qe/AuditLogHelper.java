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

package org.apache.doris.qe;

import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.FrontendOptions;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AuditLogHelper {

    private static final Logger LOG = LogManager.getLogger(AuditLogHelper.class);
    private static final Set<String> LOG_PLAN_INFO_TYPES = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    static {
        LOG_PLAN_INFO_TYPES.add("SELECT");
        LOG_PLAN_INFO_TYPES.add("INSERT");
        LOG_PLAN_INFO_TYPES.add("UPDATE");
        LOG_PLAN_INFO_TYPES.add("DELETE");
    }

    /**
     * Add a new method to wrap original logAuditLog to catch all exceptions. Because write audit
     * log may write to a doris internal table, we may meet errors. We do not want this affect the
     * query process. Ignore this error and just write warning log.
     */
    public static void logAuditLog(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        try {
            logAuditLogImpl(ctx, origStmt, parsedStmt, statistics, printFuzzyVariables);
        } catch (Throwable t) {
            LOG.warn("Failed to write audit log.", t);
        }
    }

    /**
     * Truncate sql and if SQL is in the following situations, count the number of rows:
     * <ul>
     * <li>{@code insert into tbl values (1), (2), (3)}</li>
     * </ul>
     * The final SQL will be:
     * {@code insert into tbl values (1), (2 ...}
     */
    public static String handleStmt(String origStmt, StatementBase parsedStmt) {
        if (origStmt == null) {
            return null;
        }
        // 1. handle insert statement first
        Optional<String> res = handleInsertStmt(origStmt, parsedStmt);
        if (res.isPresent()) {
            return res.get();
        }

        // 2. handle other statement
        int maxLen = GlobalVariable.auditPluginMaxSqlLength;
        origStmt = truncateByBytes(origStmt, maxLen, " ... /* truncated. audit_plugin_max_sql_length=" + maxLen
                + " */");
        return origStmt;
    }

    private static Optional<String> handleInsertStmt(String origStmt, StatementBase parsedStmt) {
        int rowCnt = 0;
        // nereids planner
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan plan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            if (plan instanceof InsertIntoTableCommand) {
                LogicalPlan query = ((InsertIntoTableCommand) plan).getLogicalQuery();
                if (query instanceof UnboundTableSink) {
                    rowCnt = countValues(query.children());
                }
            }
        }
        if (rowCnt > 0) {
            // This is an insert statement.
            int maxLen = Math.max(0,
                    Math.min(GlobalVariable.auditPluginMaxInsertStmtLength, GlobalVariable.auditPluginMaxSqlLength));
            origStmt = truncateByBytes(origStmt, maxLen, " ... /* total " + rowCnt
                    + " rows, truncated. audit_plugin_max_insert_stmt_length=" + maxLen + " */");
            return Optional.of(origStmt);
        } else {
            return Optional.empty();
        }
    }

    private static String truncateByBytes(String str, int maxLen, String suffix) {
        // use `getBytes().length` to get real byte length
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        Charset utf8Charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position()) + suffix;
    }

    /**
     * When SQL is in the following situations, count the number of rows:
     * <ul>
     * <li>{@code insert into tbl values (1), (2), (3)}</li>
     * </ul>
     */
    private static int countValues(List<Plan> children) {
        if (children == null) {
            return 0;
        }
        int cnt = 0;
        for (Plan child : children) {
            if (child instanceof OneRowRelation) {
                cnt++;
            } else if (child instanceof InlineTable) {
                cnt += ((InlineTable) child).getConstantExprsList().size();
            } else if (child instanceof LogicalUnion) {
                cnt += ((LogicalUnion) child).getConstantExprsList().size();
                cnt += countValues(child.children());
            }
        }
        return cnt;
    }

    private static void logAuditLogImpl(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();
        CatalogIf catalog = ctx.getCurrentCatalog();
        String cloudCluster = "";
        try {
            if (Config.isCloudMode()) {
                cloudCluster = ctx.getCloudCluster(false);
            }
        } catch (ComputeGroupException e) {
            LOG.warn("Failed to get cloud cluster", e);
        }
        String cluster = Config.isCloudMode() ? cloudCluster : "";
        String stmtType = getStmtType(parsedStmt);

        AuditEventBuilder auditEventBuilder = ctx.getAuditEventBuilder();
        // ATTN: MUST reset, otherwise, the same AuditEventBuilder instance will be used in the next query.
        auditEventBuilder.reset();
        auditEventBuilder
                .setEventType(EventType.AFTER_QUERY)
                .setQueryId(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()))
                .setTimestamp(ctx.getStartTime())
                .setClientIp(ctx.getClientIP())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser()))
                .setFeIp(FrontendOptions.getLocalHostAddress())
                .setCtl(catalog == null ? InternalCatalog.INTERNAL_CATALOG_NAME : catalog.getName())
                .setDb(ClusterNamespace.getNameFromFullName(ctx.getDatabase()))
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorCode() == null ? 0 : ctx.getState().getErrorCode().getCode())
                .setErrorMessage((ctx.getState().getErrorMessage() == null ? "" :
                        ctx.getState().getErrorMessage().replace("\n", " ").replace("\t", " ")))
                .setQueryTime(elapseMs)
                .setCpuTimeMs(statistics == null ? 0 : statistics.getCpuMs())
                .setPeakMemoryBytes(statistics == null ? 0 : statistics.getMaxPeakMemoryBytes())
                .setScanBytes(statistics == null ? 0 : statistics.getScanBytes())
                .setScanRows(statistics == null ? 0 : statistics.getScanRows())
                .setReturnRows(ctx.getReturnRows())
                .setSpillWriteBytesToLocalStorage(statistics == null ? 0 :
                        statistics.getSpillWriteBytesToLocalStorage())
                .setSpillReadBytesFromLocalStorage(statistics == null ? 0 :
                        statistics.getSpillReadBytesFromLocalStorage())
                .setScanBytesFromLocalStorage(statistics == null ? 0 :
                        statistics.getScanBytesFromLocalStorage())
                .setScanBytesFromRemoteStorage(statistics == null ? 0 :
                        statistics.getScanBytesFromRemoteStorage())
                .setFuzzyVariables(!printFuzzyVariables ? "" : ctx.getSessionVariable().printFuzzyVariables())
                .setCommandType(ctx.getCommand().toString())
                .setStmtType(stmtType)
                .setStmtId(ctx.getStmtId())
                .setSqlHash(ctx.getSqlHash())
                .setIsQuery(ctx.getState().isQuery())
                .setIsNereids(ctx.getState().isNereids())
                .setisInternal(ctx.getState().isInternal())
                .setCloudCluster(Strings.isNullOrEmpty(cluster) ? "UNKNOWN" : cluster)
                .setWorkloadGroup(ctx.getWorkloadGroupName());

        // sql mode
        if (ctx.sessionVariable != null) {
            try {
                auditEventBuilder.setSqlMode(SqlModeHelper.decode(ctx.sessionVariable.getSqlMode()));
            } catch (Exception e) {
                LOG.warn("decode sql mode {} failed.", ctx.sessionVariable.getSqlMode(), e);
            }
        }

        // TODO only for slow query?
        if (ctx.getExecutor() != null && LOG_PLAN_INFO_TYPES.contains(stmtType)) {
            auditEventBuilder.setHitSqlCache(ctx.getExecutor().isCached());
            auditEventBuilder.setHandledInFe(ctx.getExecutor().isHandleQueryInFe());

            SummaryProfile summaryProfile = ctx.getExecutor().getSummaryProfile();
            // parse time
            auditEventBuilder.setParseTimeMs(summaryProfile.getParseSqlTimeMs());
            // plan time
            auditEventBuilder.setPlanTimesMs(summaryProfile.getPlanTime());
            // meta time
            auditEventBuilder.setGetMetaTimeMs(summaryProfile.getMetaTime());
            // schedule time
            auditEventBuilder.setScheduleTimeMs(summaryProfile.getScheduleTime());
            // changed variables
            if (ctx.sessionVariable != null) {
                List<List<String>> changedVars = VariableMgr.dumpChangedVars(ctx.sessionVariable);
                StringBuilder changedVarsStr = new StringBuilder();
                changedVarsStr.append("{");
                for (int i = 0; i < changedVars.size(); i++) {
                    if (i > 0) {
                        changedVarsStr.append(",");
                    }
                    changedVarsStr.append("\"").append(changedVars.get(i).get(0)).append("\"")
                            .append(":").append("\"").append(changedVars.get(i).get(1)).append("\"");
                }
                changedVarsStr.append("}");
                auditEventBuilder.setChangedVariables(changedVarsStr.toString());
            }

            if (ctx.getExecutor() != null && ctx.getExecutor().planner() != null
                    && ctx.getExecutor().planner() instanceof NereidsPlanner) {
                // queried tables and views list, in audit log schema, its data type is array<string>
                NereidsPlanner nereidsPlanner = (NereidsPlanner) ctx.getExecutor().planner();
                String tables = "[" + nereidsPlanner.getStatementContext()
                        .getTables().keySet().stream()
                        .map(list -> "\"" + String.join(".", list) + "\"")
                        .collect(Collectors.joining(",")) + "]";
                auditEventBuilder.setQueriedTablesAndViews(tables);

                if (nereidsPlanner.getCascadesContext() != null
                        && nereidsPlanner.getCascadesContext().getMaterializationContexts() != null
                        && nereidsPlanner.getPhysicalPlan() != null) {
                    Set<List<String>> chosenMvCtx = MaterializationContext
                            .getChosenMvsQualifiers(nereidsPlanner.getCascadesContext()
                                    .getMaterializationContexts(), nereidsPlanner.getPhysicalPlan());
                    String chosenMvsStr = "["
                            + chosenMvCtx.stream()
                            .map(list -> "\"" + String.join(".", list) + "\"")
                            .collect(Collectors.joining(","))
                            + "]";
                    auditEventBuilder.setChosenMViews(chosenMvsStr);
                }
            }
        }

        if (ctx.getState().isQuery()) {
            if (MetricRepo.isInit) {
                if (!ctx.getState().isInternal()) {
                    MetricRepo.COUNTER_QUERY_ALL.increase(1L);
                    MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(ctx.getQualifiedUser()).increase(1L);
                }
                String physicalClusterName = "";
                try {
                    if (Config.isCloudMode()) {
                        cloudCluster = ctx.getCloudCluster(false);
                        physicalClusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getPhysicalCluster(cloudCluster);
                        if (!cloudCluster.equals(physicalClusterName)) {
                            // vcg
                            MetricRepo.increaseClusterQueryAll(physicalClusterName);
                        }
                    }
                } catch (ComputeGroupException e) {
                    LOG.warn("Failed to get cloud cluster, cloudCluster={}, physicalClusterName={} ",
                            cloudCluster, physicalClusterName, e);
                    return;
                }

                MetricRepo.increaseClusterQueryAll(cloudCluster);
                if (!ctx.getState().isInternal()) {
                    if (ctx.getState().getStateType() == MysqlStateType.ERR
                            && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                        // err query
                        MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                        MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd(ctx.getQualifiedUser()).increase(1L);
                        if (cloudCluster.equals(physicalClusterName)) {
                            // not vcg
                            MetricRepo.increaseClusterQueryErr(cloudCluster);
                        } else {
                            // vcg
                            MetricRepo.increaseClusterQueryErr(cloudCluster);
                            MetricRepo.increaseClusterQueryErr(physicalClusterName);
                        }
                    } else if (ctx.getState().getStateType() == MysqlStateType.OK
                            || ctx.getState().getStateType() == MysqlStateType.EOF) {
                        // ok query
                        MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                        MetricRepo.USER_HISTO_QUERY_LATENCY.getOrAdd(ctx.getQualifiedUser()).update(elapseMs);
                        if (cloudCluster.equals(physicalClusterName)) {
                            // not vcg
                            MetricRepo.updateClusterQueryLatency(cloudCluster, elapseMs);
                        } else {
                            // vcg
                            MetricRepo.updateClusterQueryLatency(cloudCluster, elapseMs);
                            MetricRepo.updateClusterQueryLatency(physicalClusterName, elapseMs);
                        }
                        if (elapseMs > Config.qe_slow_log_ms) {
                            String sqlDigest = DigestUtils.md5Hex(((Queriable) parsedStmt).toDigest());
                            auditEventBuilder.setSqlDigest(sqlDigest);
                            MetricRepo.COUNTER_QUERY_SLOW.increase(1L);
                        }
                    }
                }
            }
            auditEventBuilder.setScanBytesFromLocalStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromLocalStorage())
                    .setScanBytesFromRemoteStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromRemoteStorage());
        }

        boolean isAnalysisErr = ctx.getState().getStateType() == MysqlStateType.ERR
                && ctx.getState().getErrType() == QueryState.ErrType.ANALYSIS_ERR;
        String encryptSql = isAnalysisErr ? ctx.getState().getErrorMessage() : origStmt;
        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            if ((logicalPlan instanceof NeedAuditEncryption)) {
                encryptSql = ((NeedAuditEncryption) logicalPlan).geneEncryptionSQL(origStmt);
            }
        } else {
            if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
                encryptSql = parsedStmt.toSql();
            }
        }
        auditEventBuilder.setStmt(handleStmt(encryptSql, parsedStmt));

        if (!Env.getCurrentEnv().isMaster()) {
            if (ctx.executor != null && ctx.executor.isForwardToMaster()) {
                auditEventBuilder.setState(ctx.executor.getProxyStatus());
                int proxyStatusCode = ctx.executor.getProxyStatusCode();
                if (proxyStatusCode != 0) {
                    auditEventBuilder.setErrorCode(proxyStatusCode);
                    auditEventBuilder.setErrorMessage(ctx.executor.getProxyErrMsg());
                }
            }
        }
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE && ctx.getState().getErrorCode() == null) {
            auditEventBuilder.setState(String.valueOf(MysqlStateType.OK));
        }
        AuditEvent event = auditEventBuilder.build();
        Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().submitFinishQueryToAudit(event);
        if (LOG.isDebugEnabled()) {
            LOG.debug("submit audit event: {}", event.queryId);
        }
    }

    private static String getStmtType(StatementBase stmt) {
        if (stmt == null) {
            return StmtType.OTHER.name();
        }
        if (stmt.isExplain()) {
            return StmtType.EXPLAIN.name();
        }
        if (stmt instanceof LogicalPlanAdapter) {
            return ((LogicalPlanAdapter) stmt).getLogicalPlan().stmtType().name();
        } else {
            return stmt.stmtType().name();
        }
    }
}

