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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * FEOpExecutor is used to send request to specific FE
 */
public class FEOpExecutor {
    private static final Logger LOG = LogManager.getLogger(FEOpExecutor.class);

    protected static final float RPC_TIMEOUT_COEFFICIENT = 1.2f;

    protected final OriginStatement originStmt;
    protected final ConnectContext ctx;
    protected TMasterOpResult result;
    protected TNetworkAddress feAddr;

    // the total time of thrift connectTime, readTime and writeTime
    protected int thriftTimeoutMs;

    protected boolean shouldNotRetry;

    public FEOpExecutor(TNetworkAddress feAddress, OriginStatement originStmt, ConnectContext ctx, boolean isQuery) {
        this.feAddr = feAddress;
        this.originStmt = originStmt;
        this.ctx = ctx;
        this.thriftTimeoutMs = (int) (ctx.getExecTimeoutS() * 1000 * RPC_TIMEOUT_COEFFICIENT);
        // if isQuery=false, we shouldn't retry twice when catch exception because of Idempotency
        this.shouldNotRetry = !isQuery;
    }

    public void execute() throws Exception {
        result = forward(buildStmtForwardParams());
        if (ctx.isTxnModel()) {
            if (result.isSetTxnLoadInfo()) {
                ctx.getTxnEntry().setTxnLoadInfoInObserver(result.getTxnLoadInfo());
            } else {
                ctx.setTxnEntry(null);
                LOG.info("set txn entry to null");
            }
        }
        if (result.isSetAffectedRows()) {
            ctx.updateReturnRows((int) result.getAffectedRows());
        }
    }

    public void cancel() throws Exception {
        TUniqueId queryId = ctx.queryId();
        if (queryId == null) {
            return;
        }
        Preconditions.checkNotNull(feAddr, "query with id %s is not forwarded to fe", queryId);
        TMasterOpRequest request = new TMasterOpRequest();
        request.setCancelQeury(true);
        request.setQueryId(queryId);
        request.setDb(ctx.getDatabase());
        request.setUser(ctx.getQualifiedUser());
        request.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        request.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        // just make the protocol happy
        request.setSql("");
        result = forward(request);
    }

    // Send request to specific fe
    protected TMasterOpResult forward(TMasterOpRequest params) throws Exception {
        ctx.getEnv().checkReadyOrThrow();

        FrontendService.Client client;
        try {
            client = ClientPool.frontendPool.borrowObject(feAddr, thriftTimeoutMs);
        } catch (Exception e) {
            // may throw NullPointerException. add err msg
            throw new Exception("Failed to get master client.", e);
        }
        final StringBuilder forwardMsg = new StringBuilder("forward to master FE " + feAddr.toString());
        if (!params.isSyncJournalOnly()) {
            forwardMsg.append(", statement id: ").append(ctx.getStmtId());
        }
        LOG.info(forwardMsg.toString());

        boolean isReturnToPool = false;
        try {
            final TMasterOpResult result = client.forward(params);
            isReturnToPool = true;
            return result;
        } catch (TTransportException e) {
            // wrap the raw exception.
            forwardMsg.append(" : failed");
            Exception exception = new ForwardToMasterException(forwardMsg.toString(), e);

            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw exception;
            }
            if (shouldNotRetry || e.getType() == TTransportException.TIMED_OUT) {
                throw exception;
            } else {
                LOG.warn(forwardMsg.append(" twice").toString(), e);
                try {
                    TMasterOpResult result = client.forward(params);
                    isReturnToPool = true;
                    return result;
                } catch (TException ex) {
                    throw exception;
                }
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(feAddr, client);
            } else {
                ClientPool.frontendPool.invalidateObject(feAddr, client);
            }
        }
    }

    protected TMasterOpRequest buildStmtForwardParams() throws AnalysisException {
        TMasterOpRequest params = new TMasterOpRequest();
        // node ident
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setSql(originStmt.originStmt);
        params.setStmtIdx(originStmt.idx);
        params.setUser(ctx.getQualifiedUser());
        params.setDefaultCatalog(ctx.getDefaultCatalog());
        params.setDefaultDatabase(ctx.getDatabase());
        params.setDb(ctx.getDatabase());
        params.setUserIp(ctx.getRemoteIP());
        params.setStmtId(ctx.getStmtId());
        params.setCurrentUserIdent(ctx.getCurrentUserIdentity().toThrift());
        params.setSessionId(ctx.getSessionId());

        if (Config.isCloudMode()) {
            String cluster = "";
            try {
                cluster = ctx.getCloudCluster(false);
            } catch (Exception e) {
                LOG.warn("failed to get cloud compute group", e);
            }
            if (!Strings.isNullOrEmpty(cluster)) {
                params.setCloudCluster(cluster);
            }
        }

        // session variables
        params.setSessionVariables(ctx.getSessionVariable().getForwardVariables());
        params.setUserVariables(getForwardUserVariables(ctx.getUserVars()));
        if (null != ctx.queryId()) {
            params.setQueryId(ctx.queryId());
        }

        // set transaction load info
        if (ctx.isTxnModel()) {
            params.setTxnLoadInfo(ctx.getTxnEntry().getTxnLoadInfoInObserver());
        }

        if (ctx.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
            if (null != ctx.getPrepareExecuteBuffer()) {
                params.setPrepareExecuteBuffer(ctx.getPrepareExecuteBuffer());
            }
        }

        return params;
    }

    public int getStatusCode() {
        if (result == null || !result.isSetStatusCode()) {
            return ErrorCode.ERR_UNKNOWN_ERROR.getCode();
        }
        return result.getStatusCode();
    }

    public String getErrMsg() {
        if (result == null) {
            return ErrorCode.ERR_UNKNOWN_ERROR.getErrorMsg();
        }
        if (!result.isSetErrMessage()) {
            return "";
        }
        return result.getErrMessage();
    }


    public ByteBuffer getOutputPacket() {
        if (result == null) {
            return null;
        }
        return result.packet;
    }

    public TUniqueId getQueryId() {
        if (result != null && result.isSetQueryId()) {
            return result.getQueryId();
        } else {
            return null;
        }
    }

    public String getProxyStatus() {
        if (result == null) {
            return QueryState.MysqlStateType.UNKNOWN.name();
        }
        if (!result.isSetStatus()) {
            return QueryState.MysqlStateType.UNKNOWN.name();
        } else {
            return result.getStatus();
        }
    }

    public ShowResultSet getProxyResultSet() {
        if (result == null) {
            return null;
        }
        if (result.isSetResultSet()) {
            return new ShowResultSet(result.resultSet);
        } else {
            return null;
        }
    }

    public List<ByteBuffer> getQueryResultBufList() {
        return result.isSetQueryResultBufList() ? result.getQueryResultBufList() : Collections.emptyList();
    }

    private Map<String, TExprNode> getForwardUserVariables(Map<String, LiteralExpr> userVariables) {
        Map<String, TExprNode> forwardVariables = Maps.newHashMap();
        for (Map.Entry<String, LiteralExpr> entry : userVariables.entrySet()) {
            LiteralExpr literalExpr = entry.getValue();
            TExpr tExpr = literalExpr.treeToThrift();
            TExprNode tExprNode = tExpr.nodes.get(0);
            forwardVariables.put(entry.getKey(), tExprNode);
        }
        return forwardVariables;
    }

    protected static class ForwardToMasterException extends RuntimeException {
        private static final Map<Integer, String> TYPE_MSG_MAP =
                ImmutableMap.<Integer, String>builder()
                        .put(TTransportException.UNKNOWN, "Unknown exception")
                        .put(TTransportException.NOT_OPEN, "Connection is not open")
                        .put(TTransportException.ALREADY_OPEN, "Connection has already opened up")
                        .put(TTransportException.TIMED_OUT,
                                "Connection timeout, please check network state or enlarge session variable:"
                                        + "`query_timeout`/`insert_timeout`")
                        .put(TTransportException.END_OF_FILE, "EOF")
                        .put(TTransportException.CORRUPTED_DATA, "Corrupted data")
                        .build();

        private final String msg;

        public ForwardToMasterException(String msg, TTransportException exception) {
            this.msg = msg + ", cause: " + TYPE_MSG_MAP.get(exception.getType()) + ", " + exception.getMessage();
        }

        @Override
        public String getMessage() {
            return msg;
        }
    }
}
