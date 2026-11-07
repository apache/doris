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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
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
        waitOnReplaying(result);
    }

    @Override
    public void cancel() throws Exception {
        TMasterOpResult cancelResult = cancelForwardedQuery();
        if (cancelResult != null) {
            waitOnReplaying(cancelResult);
        }
    }

    private void waitOnReplaying(TMasterOpResult response) throws DdlException {
        LOG.info("forwarding to master get result max journal id: {}", response.maxJournalId);
        env.getJournalObservable().waitOn(response.maxJournalId, journalWaitTimeoutMs);
    }

    public void syncJournal() throws Exception {
        result = forward(buildSyncJournalParams());
        waitOnReplaying(result);
    }

    /** Synchronizes a metadata request without creating or borrowing a SQL session. */
    public static void syncJournal(Env env, String database, String user) throws Exception {
        TMasterOpRequest request = buildMetadataRequest(env, database, user);
        request.setSyncJournalOnly(true);
        int timeoutMs = metadataRpcTimeoutMs(env, user);
        TMasterOpResult response = forward(env,
                new TNetworkAddress(env.getMasterHost(), env.getMasterRpcPort()), timeoutMs, false, request);
        LOG.info("forwarding to master get result max journal id: {}", response.maxJournalId);
        env.getJournalObservable().waitOn(response.maxJournalId, timeoutMs);
    }

    /** Reports group-commit pressure; this in-memory update does not wait for journal replay. */
    public static void updateLoadData(Env env, long tableId, long receiveData) throws Exception {
        String user = UserIdentity.ADMIN.getQualifiedUser();
        TMasterOpRequest request = buildMetadataRequest(env, "", user);
        request.setGroupCommitInfo(new TGroupCommitInfo().setUpdateLoadData(true)
                .setTableId(tableId).setReceiveData(receiveData));
        forward(env, new TNetworkAddress(env.getMasterHost(), env.getMasterRpcPort()),
                metadataRpcTimeoutMs(env, user), false, request);
    }

    private static TMasterOpRequest buildMetadataRequest(Env env, String database, String user) {
        return new TMasterOpRequest().setClientNodeHost(env.getSelfNode().getHost())
                .setClientNodePort(env.getSelfNode().getPort()).setDb(database).setUser(user).setSql("");
    }

    private static int metadataRpcTimeoutMs(Env env, String user) {
        // These RPCs used a fresh ordinary user's session, never the invoking SQL session's
        // overrides or external temporary-user defaults.
        int userTimeoutS = Strings.isNullOrEmpty(user) ? 0 : env.getAuth().getQueryTimeoutForRegularUser(user);
        int timeoutS = userTimeoutS > 0 ? userTimeoutS : VariableMgr.getDefaultQueryTimeoutS();
        return (int) (timeoutS * 1000 * RPC_TIMEOUT_COEFFICIENT);
    }

    public long getGroupCommitLoadBeId(long tableId, String cluster) throws Exception {
        result = forward(buildGetGroupCommitLoadBeIdParmas(tableId, cluster));
        if (result.isSetStatusCode() && result.getStatusCode() != 0) {
            throw new LoadException(getForwardResultErrorMessage(result));
        }
        if (result.isSetErrMessage()) {
            throw new LoadException(result.getErrMessage());
        }
        return result.groupCommitLoadBeId;
    }

    private static String getForwardResultErrorMessage(TMasterOpResult result) {
        if (result.isSetErrMessage() && result.getErrMessage() != null && !result.getErrMessage().isEmpty()) {
            return result.getErrMessage();
        }
        return "failed to select backend for group commit, status code: " + result.getStatusCode();
    }

    private TMasterOpRequest buildSyncJournalParams() {
        return buildMetadataRequest(env, ctx.getDatabase(), ctx.getQualifiedUser()).setSyncJournalOnly(true);
    }

    private TMasterOpRequest buildGetGroupCommitLoadBeIdParmas(long tableId, String cluster) {
        final TGroupCommitInfo groupCommitParams = new TGroupCommitInfo();
        groupCommitParams.setGetGroupCommitLoadBeId(true);
        groupCommitParams.setGroupCommitLoadTableId(tableId);
        groupCommitParams.setCluster(cluster);
        groupCommitParams.setSupportsSelectionErrorResult(true);
        setGroupCommitLoadSelectionHint(groupCommitParams, ctx);
        return getMasterOpRequestForGroupCommit(groupCommitParams);
    }

    static void setGroupCommitLoadSelectionHint(TGroupCommitInfo groupCommitParams, ConnectContext context) {
        BackendSelection.SelectionHint decision = BackendSelectionManager.resolveLoadSelectionHint(context);
        if (decision == null) {
            return;
        }
        groupCommitParams.setLoadSelectionPreferredKey(decision.getPreferredKey());
        groupCommitParams.setLoadSelectionMode(decision.getMode().name());
    }

    private TMasterOpRequest getMasterOpRequestForGroupCommit(TGroupCommitInfo groupCommitParams) {
        return buildMetadataRequest(env, ctx.getDatabase(), ctx.getQualifiedUser())
                .setGroupCommitInfo(groupCommitParams);
    }

}
