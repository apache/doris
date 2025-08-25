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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * MasterOpExecutor is used to send request to Master FE.
 * It is inherited from FEOpExecutor. The difference is that MasterOpExecutor may need to wait the journal being
 * synced before returning.
 */
public class MasterOpExecutor extends FEOpExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterOpExecutor.class);
    private final int journalWaitTimeoutMs;

    public MasterOpExecutor(OriginStatement originStmt, ConnectContext ctx, RedirectStatus status, boolean isQuery) {
        super(new TNetworkAddress(ctx.getEnv().getMasterHost(), ctx.getEnv().getMasterRpcPort()),
                originStmt, ctx, isQuery);
        if (status.isNeedToWaitJournalSync()) {
            this.journalWaitTimeoutMs = (int) (ctx.getExecTimeoutS() * 1000 * RPC_TIMEOUT_COEFFICIENT);
        } else {
            this.journalWaitTimeoutMs = 0;
        }
    }

    /**
     * used for simply syncing journal with master under strong consistency mode
     */
    public MasterOpExecutor(ConnectContext ctx) {
        this(null, ctx, RedirectStatus.FORWARD_WITH_SYNC, true);
    }

    @Override
    public void execute() throws Exception {
        super.execute();
        waitOnReplaying();
    }

    @Override
    public void cancel() throws Exception {
        super.cancel();
        waitOnReplaying();
    }

    private void waitOnReplaying() throws DdlException {
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        ctx.getEnv().getJournalObservable().waitOn(result.maxJournalId, journalWaitTimeoutMs);
    }

    public void syncJournal() throws Exception {
        result = forward(buildSyncJournalParams());
        waitOnReplaying();
    }

    public long getGroupCommitLoadBeId(long tableId, String cluster) throws Exception {
        result = forward(buildGetGroupCommitLoadBeIdParmas(tableId, cluster));
        waitOnReplaying();
        return result.groupCommitLoadBeId;
    }

    public void updateLoadData(long tableId, long receiveData) throws Exception {
        result = forward(buildUpdateLoadDataParams(tableId, receiveData));
        waitOnReplaying();
    }

    private TMasterOpRequest buildSyncJournalParams() {
        final TMasterOpRequest params = new TMasterOpRequest();
        // node ident
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setSyncJournalOnly(true);
        params.setDb(ctx.getDatabase());
        params.setUser(ctx.getQualifiedUser());
        // just make the protocol happy
        params.setSql("");
        return params;
    }

    private TMasterOpRequest buildGetGroupCommitLoadBeIdParmas(long tableId, String cluster) {
        final TGroupCommitInfo groupCommitParams = new TGroupCommitInfo();
        groupCommitParams.setGetGroupCommitLoadBeId(true);
        groupCommitParams.setGroupCommitLoadTableId(tableId);
        groupCommitParams.setCluster(cluster);
        return getMasterOpRequestForGroupCommit(groupCommitParams);
    }

    private TMasterOpRequest buildUpdateLoadDataParams(long tableId, long receiveData) {
        final TGroupCommitInfo groupCommitParams = new TGroupCommitInfo();
        groupCommitParams.setUpdateLoadData(true);
        groupCommitParams.setTableId(tableId);
        groupCommitParams.setReceiveData(receiveData);
        return getMasterOpRequestForGroupCommit(groupCommitParams);
    }

    private TMasterOpRequest getMasterOpRequestForGroupCommit(TGroupCommitInfo groupCommitParams) {
        final TMasterOpRequest params = new TMasterOpRequest();
        // node ident
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setGroupCommitInfo(groupCommitParams);
        params.setDb(ctx.getDatabase());
        params.setUser(ctx.getQualifiedUser());
        // just make the protocol happy
        params.setSql("");
        return params;
    }

}
