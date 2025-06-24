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

package org.apache.doris.nereids.trees.plans.commands.utils;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.FEOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Utility class for killing queries and connections.
 */
public class KillUtils {
    private static final Logger LOG = LogManager.getLogger(KillUtils.class);

    /**
     * Kill a query by query id or connection id.
     *
     * @param ctx the current connect context
     * @param killConnection true if kill connection, false if only kill query
     * @param queryId the query id to kill
     * @param connectionId the connection id to kill
     * @param stmt the origin kill statement, which may need to be forwarded to other FE
     */
    public static void kill(ConnectContext ctx, boolean killConnection, String queryId, int connectionId,
            OriginStatement stmt) throws UserException {
        if (killConnection) {
            // kill connection connection_id
            // kill connection_id
            Preconditions.checkState(connectionId >= 0, connectionId);
            killByConnectionId(ctx, true, connectionId);
        } else {
            if (!Strings.isNullOrEmpty(queryId)) {
                // kill query "query_id"
                killQueryByQueryId(ctx, queryId, stmt);
            } else {
                // kill query connection_id
                Preconditions.checkState(connectionId >= 0, connectionId);
                killByConnectionId(ctx, false, connectionId);
            }
        }
    }

    /**
     * Kill a query by query id.
     *
     * @param ctx the current connect context
     * @param queryId the query id to kill
     * @param stmt the origin kill statement, which may need to be forwarded to other FE
     */
    @VisibleForTesting
    public static void killQueryByQueryId(ConnectContext ctx, String queryId, OriginStatement stmt)
            throws UserException {
        // 1. First, try to find the query in the current FE and kill it
        if (killByQueryIdOnCurrentNode(ctx, queryId)) {
            return;
        }

        if (ctx.isProxy()) {
            // The query is not found in the current FE, and the command is forwarded from other FE.
            // return error to let the proxy FE to handle it.
            if (LOG.isDebugEnabled()) {
                LOG.debug("kill query '{}' in proxy mode but not found", queryId);
            }
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_QUERY, queryId);
        }

        // 2. Query not found in current FE, try to kill the query in other FE.
        List<String> errMsgs = Lists.newArrayList();
        for (Frontend fe : Env.getCurrentEnv().getFrontends(null /* all */)) {
            if (!fe.isAlive() || fe.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                continue;
            }

            TNetworkAddress feAddr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            FEOpExecutor executor = new FEOpExecutor(feAddr, stmt, ConnectContext.get(), false);
            if (LOG.isDebugEnabled()) {
                LOG.debug("try kill query '{}' to FE: {}", queryId, feAddr.toString());
            }
            try {
                executor.execute();
            } catch (Exception e) {
                throw new DdlException(e.getMessage(), e);
            }
            if (executor.getStatusCode() != TStatusCode.OK.getValue()) {
                // The query is not found in this FE, continue to find in other FEs
                // and save error msg
                errMsgs.add(String.format("failed to apply to fe %s:%s, error message: %s",
                        fe.getHost(), fe.getRpcPort(), executor.getErrMsg()));
            } else {
                // Find query in other FE, just return
                ctx.getState().setOk();
                return;
            }
        }

        // 3. Query not found in any FE, try cancel the query in BE.
        if (LOG.isDebugEnabled()) {
            LOG.debug("not found query '{}' in any FE, try to kill it in BE. Messages: {}",
                    queryId, errMsgs);
        }
        ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_QUERY, queryId);
    }

    /**
     * Kill a query by query id on the current FE.
     *
     * @param ctx the current connect context
     * @param queryId the query id to kill
     * @return true if the query is killed, false if not found
     */
    @VisibleForTesting
    public static boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) throws DdlException {
        ConnectContext killCtx = ExecuteEnv.getInstance().getScheduler().getContextWithQueryId(queryId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("kill query '{}' on current node", queryId);
        }
        if (killCtx != null) {
            // Check auth. Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ctx.getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, queryId);
            }
            killCtx.kill(false);
            ctx.getState().setOk();
            return true;
        }
        return false;
    }

    /**
     * Kill a connection by connection id.
     *
     * @param ctx the current connect context
     * @param connectionId the connection id to kill
     */
    @VisibleForTesting
    public static void killByConnectionId(ConnectContext ctx, boolean killConnection, int connectionId)
            throws DdlException {
        ConnectContext killCtx = ctx.getConnectScheduler().getContext(connectionId);
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, connectionId);
        }
        if (ctx == killCtx) {
            // Suicide
            ctx.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ctx.getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, connectionId);
            }
            killCtx.kill(killConnection);
        }
        ctx.getState().setOk();
    }
}
