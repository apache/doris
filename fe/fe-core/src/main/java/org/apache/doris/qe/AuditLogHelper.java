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

import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.FrontendOptions;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import org.apache.commons.codec.digest.DigestUtils;

public class AuditLogHelper {

    public static void logAuditLog(ConnectContext ctx, String origStmt, StatementBase parsedStmt,
            org.apache.doris.proto.Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        origStmt = origStmt.replace("\n", " ");
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();
        SpanContext spanContext = Span.fromContext(Context.current()).getSpanContext();

        ctx.getAuditEventBuilder().setEventType(EventType.AFTER_QUERY)
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
                .setTraceId(spanContext.isValid() ? spanContext.getTraceId() : "")
                .setWorkloadGroup(ctx.getWorkloadGroupName())
                .setFuzzyVariables(!printFuzzyVariables ? "" : ctx.getSessionVariable().printFuzzyVariables());

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd(ctx.getQualifiedUser()).increase(1L);
            if (ctx.getState().getStateType() == MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
                MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd(ctx.getQualifiedUser()).increase(1L);
            } else if (ctx.getState().getStateType() == MysqlStateType.OK
                    || ctx.getState().getStateType() == MysqlStateType.EOF) {
                // ok query
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                MetricRepo.USER_HISTO_QUERY_LATENCY.getOrAdd(ctx.getQualifiedUser()).update(elapseMs);

                if (elapseMs > Config.qe_slow_log_ms) {
                    String sqlDigest = DigestUtils.md5Hex(((Queriable) parsedStmt).toDigest());
                    ctx.getAuditEventBuilder().setSqlDigest(sqlDigest);
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
            if (ctx.getQueryDetail() != null) {
                ctx.getQueryDetail().setEventTime(endTime);
                ctx.getQueryDetail().setEndTime(endTime);
                ctx.getQueryDetail().setLatency(elapseMs);
                ctx.getQueryDetail().setState(QueryDetail.QueryMemState.FINISHED);
                QueryDetailQueue.addOrUpdateQueryDetail(ctx.getQueryDetail());
                ctx.setQueryDetail(null);
            }
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }
        ctx.getAuditEventBuilder().setIsNereids(ctx.getState().isNereids);

        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            ctx.getAuditEventBuilder().setStmt(parsedStmt.toSql());
        } else {
            if (parsedStmt instanceof InsertStmt && !((InsertStmt) parsedStmt).needLoadManager()
                    && ((InsertStmt) parsedStmt).isValuesOrConstantSelect()) {
                // INSERT INTO VALUES may be very long, so we only log at most 1K bytes.
                int length = Math.min(1024, origStmt.length());
                ctx.getAuditEventBuilder().setStmt(origStmt.substring(0, length));
            } else {
                ctx.getAuditEventBuilder().setStmt(origStmt);
            }
        }
        if (!Env.getCurrentEnv().isMaster()) {
            if (ctx.executor.isForwardToMaster()) {
                ctx.getAuditEventBuilder().setState(ctx.executor.getProxyStatus());
            }
        }
        Env.getCurrentAuditEventProcessor().handleAuditEvent(ctx.getAuditEventBuilder().build());
    }
}
