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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.common.ClientPool;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

public class MasterOpExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterOpExecutor.class);

    private final OriginStatement originStmt;
    private final ConnectContext ctx;
    private TMasterOpResult result;

    private int waitTimeoutMs;
    // the total time of thrift connectTime add readTime and writeTime
    private int thriftTimeoutMs;

    private boolean shouldNotRetry;

    public MasterOpExecutor(OriginStatement originStmt, ConnectContext ctx, RedirectStatus status, boolean isQuery) {
        this.originStmt = originStmt;
        this.ctx = ctx;
        if (status.isNeedToWaitJournalSync()) {
            this.waitTimeoutMs = ctx.getSessionVariable().getQueryTimeoutS() * 1000;
        } else {
            this.waitTimeoutMs = 0;
        }
        this.thriftTimeoutMs = ctx.getSessionVariable().getQueryTimeoutS() * 1000;
        // if isQuery=false, we shouldn't retry twice when catch exception because of Idempotency
        this.shouldNotRetry = !isQuery;
    }

    public void execute() throws Exception {
        forward();
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        ctx.getCatalog().getJournalObservable().waitOn(result.maxJournalId, waitTimeoutMs);
    }
    
    // Send request to Master
    private void forward() throws Exception {
        if (!ctx.getCatalog().isReady()) {
            throw new Exception("Node catalog is not ready, please wait for a while.");
        }
        String masterHost = ctx.getCatalog().getMasterIp();
        int masterRpcPort = ctx.getCatalog().getMasterRpcPort();
        TNetworkAddress thriftAddress = new TNetworkAddress(masterHost, masterRpcPort);

        FrontendService.Client client = null;
        try {
            client = ClientPool.frontendPool.borrowObject(thriftAddress, thriftTimeoutMs);
        } catch (Exception e) {
            // may throw NullPointerException. add err msg
            throw new Exception("Failed to get master client.", e);
        }
        TMasterOpRequest params = new TMasterOpRequest();
        params.setCluster(ctx.getClusterName());
        params.setSql(originStmt.originStmt);
        params.setStmtIdx(originStmt.idx);
        params.setUser(ctx.getQualifiedUser());
        params.setDb(ctx.getDatabase());
        params.setResourceInfo(ctx.toResourceCtx());
        params.setUserIp(ctx.getRemoteIP());
        params.setStmtId(ctx.getStmtId());
        params.setCurrentUserIdent(ctx.getCurrentUserIdentity().toThrift());

        // query options
        params.setQueryOptions(ctx.getSessionVariable().getQueryOptionVariables());
        // session variables
        params.setSessionVariables(ctx.getSessionVariable().getForwardVariables());

        if (null != ctx.queryId()) {
            params.setQueryId(ctx.queryId());
        }

        LOG.info("Forward statement {} to Master {}", ctx.getStmtId(), thriftAddress);

        boolean isReturnToPool = false;
        try {
            result = client.forward(params);
            isReturnToPool = true;
        } catch (TTransportException e) { 
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            if (shouldNotRetry || e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                LOG.warn("Forward statement "+ ctx.getStmtId() +" to Master " + thriftAddress + " twice", e);
                result = client.forward(params);
                isReturnToPool = true;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
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
    
    public void setResult(TMasterOpResult result) {
        this.result = result;
    }
}

