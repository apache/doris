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

import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.ValueList;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.FrontendOptions;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;

public class AuditLogHelper {

    private static final Logger LOG = LogManager.getLogger(AuditLogHelper.class);

    /**
     * Add a new method to wrap original logAuditLog to catch all exceptions. Because write audit
     * log may write to a doris internal table, we may meet errors. We do not want this affect the
     * query process. Ignore this error and just write warning log.
     */
    public static void logAuditLog(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        try {
            origStmt = handleStmt(origStmt, parsedStmt);
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
        int maxLen = GlobalVariable.auditPluginMaxSqlLength;
        if (origStmt.length() <= maxLen) {
            return origStmt.replace("\n", "\\n")
                .replace("\t", "\\t")
                .replace("\r", "\\r");
        }
        origStmt = truncateByBytes(origStmt)
            .replace("\n", "\\n")
            .replace("\t", "\\t")
            .replace("\r", "\\r");
        int rowCnt = 0;
        // old planner
        if (parsedStmt instanceof NativeInsertStmt) {
            QueryStmt queryStmt = ((NativeInsertStmt) parsedStmt).getQueryStmt();
            if (queryStmt instanceof SelectStmt) {
                ValueList list = ((SelectStmt) queryStmt).getValueList();
                if (list != null && list.getRows() != null) {
                    rowCnt = list.getRows().size();
                }
            }
        }
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
            return origStmt + " ... /* total " + rowCnt + " rows, truncated audit_plugin_max_sql_length="
                + GlobalVariable.auditPluginMaxSqlLength + " */";
        } else {
            return origStmt
                + " ... /* truncated audit_plugin_max_sql_length="
                + GlobalVariable.auditPluginMaxSqlLength + " */";
        }
    }

    private static String truncateByBytes(String str) {
        int maxLen = Math.min(GlobalVariable.auditPluginMaxSqlLength, str.getBytes().length);
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
        return new String(charBuffer.array(), 0, charBuffer.position());
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
            if (child instanceof UnboundOneRowRelation) {
                cnt++;
            } else if (child instanceof LogicalInlineTable) {
                cnt += ((LogicalInlineTable) child).getConstantExprsList().size();
            } else if (child instanceof LogicalUnion) {
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

        AuditEventBuilder auditEventBuilder = ctx.getAuditEventBuilder();
        // ATTN: MUST reset, otherwise, the same AuditEventBuilder instance will be used in the next query.
        auditEventBuilder.reset();
        auditEventBuilder
                .setTimestamp(ctx.getStartTime())
                .setClientIp(ctx.getClientIP())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser()))
                .setSqlHash(ctx.getSqlHash())
                .setEventType(EventType.AFTER_QUERY)
                .setCtl(catalog == null ? InternalCatalog.INTERNAL_CATALOG_NAME : catalog.getName())
                .setDb(ClusterNamespace.getNameFromFullName(ctx.getDatabase()))
                .setState(ctx.getState().toString())
                .setErrorCode(ctx.getState().getErrorCode() == null ? 0 : ctx.getState().getErrorCode().getCode())
                .setErrorMessage((ctx.getState().getErrorMessage() == null ? "" :
                        ctx.getState().getErrorMessage().replace("\n", " ").replace("\t", " ")))
                .setQueryTime(elapseMs)
                .setScanBytes(statistics == null ? 0 : statistics.getScanBytes())
                .setScanRows(statistics == null ? 0 : statistics.getScanRows())
                .setCpuTimeMs(statistics == null ? 0 : statistics.getCpuMs())
                .setPeakMemoryBytes(statistics == null ? 0 : statistics.getMaxPeakMemoryBytes())
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()))
                .setWorkloadGroup(ctx.getWorkloadGroupName())
                .setFuzzyVariables(!printFuzzyVariables ? "" : ctx.getSessionVariable().printFuzzyVariables())
                .setCommandType(ctx.getCommand().toString());

        if (ctx.getState().isQuery()) {
            if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                MetricRepo.COUNTER_QUERY_ALL.increase(1L);
                MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(ctx.getQualifiedUser()).increase(1L);
            }
            if (ctx.getState().getStateType() == MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                    MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                    MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd(ctx.getQualifiedUser()).increase(1L);
                }
            } else if (ctx.getState().getStateType() == MysqlStateType.OK
                    || ctx.getState().getStateType() == MysqlStateType.EOF) {
                // ok query
                if (!ctx.getSessionVariable().internalSession && MetricRepo.isInit) {
                    MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                    MetricRepo.USER_HISTO_QUERY_LATENCY.getOrAdd(ctx.getQualifiedUser()).update(elapseMs);
                }

                if (elapseMs > Config.qe_slow_log_ms) {
                    String sqlDigest = DigestUtils.md5Hex(((Queriable) parsedStmt).toDigest());
                    auditEventBuilder.setSqlDigest(sqlDigest);
                }
            }
            auditEventBuilder.setIsQuery(true)
                    .setScanBytesFromLocalStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromLocalStorage())
                    .setScanBytesFromRemoteStorage(
                            statistics == null ? 0 : statistics.getScanBytesFromRemoteStorage());
        } else {
            auditEventBuilder.setIsQuery(false);
        }
        auditEventBuilder.setIsNereids(ctx.getState().isNereids);

        auditEventBuilder.setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            auditEventBuilder.setStmt(parsedStmt.toSql());
        } else {
            auditEventBuilder.setStmt(origStmt);
        }
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
        Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().submitFinishQueryToAudit(auditEventBuilder.build());
    }
}
