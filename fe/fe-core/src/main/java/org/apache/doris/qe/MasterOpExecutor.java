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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;

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
    public boolean supportNotMasterRedirect() {
        return true;
    }

    @Override
    public void execute() throws Exception {
        TMasterOpRequest params = buildStmtForwardParams();
        result = forward(params);
        if (isNotMasterResult(result)) {
            // The FE we forwarded to is not the master any more (our masterInfo is stale,
            // typically after a master failover while journal replay is lagging).
            // The statement was NOT executed there, so it is safe to re-discover the real
            // master and retry once, even for non-idempotent statements.
            result = redirectAndRetry(params);
        }
        processForwardResult(result);
        waitOnReplaying();
    }

    @Override
    public void cancel() throws Exception {
        super.cancel();
        waitOnReplaying();
    }

    private void processForwardResult(TMasterOpResult result) throws Exception {
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

    private boolean isNotMasterResult(TMasterOpResult result) {
        return isNotMasterResultForTest(result);
    }

    static boolean isNotMasterResultForTest(TMasterOpResult result) {
        return result != null && result.isSetNotMaster() && result.isNotMaster();
    }

    private TNetworkAddress validateHint(TNetworkAddress hint) {
        return validateHintForTest(hint);
    }

    /**
     * Handle a NOT_MASTER rejection: validate the hint carried by the rejecting FE,
     * or discover the current master on our own, then retry the request once against it.
     *
     * The hint is best-effort and must not be trusted blindly: a degraded old master
     * may still keep masterInfo = itself, so a hint pointing back to the failed target
     * (or to this node) is rejected and we fall back to our own discovery.
     */
    private TMasterOpResult redirectAndRetry(TMasterOpRequest params) throws Exception {
        TNetworkAddress newMaster = validateHint(result.getMasterAddress());
        if (newMaster == null) {
            newMaster = discoverMasterByLeader();
        }
        if (newMaster == null) {
            newMaster = discoverMasterByProbe();
        }
        if (newMaster == null) {
            LOG.warn("forward target {} is not master and no new master could be discovered", feAddr);
            throw new MasterRedirectException(
                    "forward to master FE " + feAddr + " failed: target is not master any more"
                            + " and no new master could be discovered. You may need to check FE's status");
        }
        LOG.warn("forward target {} is not master any more, retry against the new master {}",
                feAddr, newMaster);
        TMasterOpResult retryResult = forwardTo(params, newMaster);
        if (isNotMasterResult(retryResult)) {
            // single retry only: never loop on redirects
            throw new MasterRedirectException(
                    "forward to master FE " + newMaster + " also rejected as not-master");
        }
        feAddr = newMaster;
        return retryResult;
    }

    /**
     * Thrown when a forward target rejects the request as NOT_MASTER and no usable
     * new master can be discovered (or the retry target also rejects it).
     */
    public static class MasterRedirectException extends RuntimeException {
        public MasterRedirectException(String msg) {
            super(msg);
        }
    }

    /**
     * A usable hint must be non-empty, and must not point back to the failed target or to
     * this node (both are possible for a degraded old master whose masterInfo = itself).
     */
    TNetworkAddress validateHintForTest(TNetworkAddress hint) {
        if (hint == null || Strings.isNullOrEmpty(hint.hostname) || hint.port <= 0) {
            return null;
        }
        if (hint.hostname.equals(feAddr.getHostname()) && hint.port == feAddr.getPort()) {
            return null;
        }
        String selfHost = Env.getCurrentEnv().getSelfNode().getHost();
        if (hint.hostname.equals(selfHost) && hint.port == Config.rpc_port) {
            return null;
        }
        return hint;
    }

    /**
     * Discover the current master by asking the bdbje group directly (independent of
     * journal replay), then map the leader's (host, editLogPort) to its rpc port via the
     * local frontend list. May fail when the bdbje channel itself is partitioned.
     */
    private TNetworkAddress discoverMasterByLeader() {
        try {
            InetSocketAddress leader = ctx.getEnv().getHaProtocol().getLeader();
            if (leader == null) {
                return null;
            }
            for (Frontend fe : ctx.getEnv().getFrontends(null)) {
                if (fe.getRole() == FrontendNodeType.FOLLOWER
                        && fe.getHost().equals(leader.getHostString())
                        && fe.getEditLogPort() == leader.getPort()) {
                    return new TNetworkAddress(fe.getHost(), fe.getRpcPort());
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to discover master by bdbje leader: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Last-resort discovery, tolerant of a partitioned bdbje channel: probe the thrift
     * endpoints of alive followers (excluding the failed target and this node) with a
     * lightweight isMasterProbe request, bounded to at most a few probes.
     */
    private TNetworkAddress discoverMasterByProbe() {
        int probed = 0;
        final int maxProbes = 2;
        String selfHost = Env.getCurrentEnv().getSelfNode().getHost();
        for (Frontend fe : ctx.getEnv().getFrontends(FrontendNodeType.FOLLOWER)) {
            if (probed >= maxProbes) {
                break;
            }
            if (!fe.isAlive()) {
                continue;
            }
            TNetworkAddress candidate = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            if (candidate.hostname.equals(feAddr.getHostname()) && candidate.port == feAddr.getPort()) {
                continue;
            }
            if (fe.getHost().equals(selfHost)) {
                continue;
            }
            probed++;
            try {
                TMasterOpResult probeResult = forwardTo(buildMasterProbeParams(), candidate);
                if (probeResult.isSetNotMaster() && !probeResult.isNotMaster()) {
                    return candidate;
                }
            } catch (Exception e) {
                LOG.warn("master probe to {} failed: {}", candidate, e.getMessage());
            }
        }
        return null;
    }

    private TMasterOpRequest buildMasterProbeParams() {
        TMasterOpRequest params = new TMasterOpRequest();
        params.setClientNodeHost(Env.getCurrentEnv().getSelfNode().getHost());
        params.setClientNodePort(Env.getCurrentEnv().getSelfNode().getPort());
        params.setIsMasterProbe(true);
        params.setDb(ctx.getDatabase());
        params.setUser(ctx.getQualifiedUser());
        // just make the protocol happy
        params.setSql("");
        return params;
    }

    private void waitOnReplaying() throws DdlException {
        if (isNotMasterResult(result)) {
            // A NOT_MASTER rejection carries no valid journal-sync target; waiting on it
            // (typically journal id 0 or a stale id from the rejecting FE) would hang the
            // client for the whole journal-wait timeout. Surface the error immediately.
            LOG.info("forward result is a NOT_MASTER rejection, skip journal replay wait");
            return;
        }
        LOG.info("forwarding to master get result max journal id: {}", result.maxJournalId);
        ctx.getEnv().getJournalObservable().waitOn(result.maxJournalId, journalWaitTimeoutMs);
    }

    public void syncJournal() throws Exception {
        result = forward(buildSyncJournalParams());
        waitOnReplaying();
    }

    // The master handles the group commit shortcuts without writing a journal, so the result carries
    // no journal id. Waiting on journal 0 is a no-op that only logs one line per request.
    public long getGroupCommitLoadBeId(long tableId, String cluster) throws Exception {
        result = forward(buildGetGroupCommitLoadBeIdParmas(tableId, cluster));
        return result.groupCommitLoadBeId;
    }

    public void updateLoadData(long tableId, long receiveData) throws Exception {
        result = forward(buildUpdateLoadDataParams(tableId, receiveData));
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
