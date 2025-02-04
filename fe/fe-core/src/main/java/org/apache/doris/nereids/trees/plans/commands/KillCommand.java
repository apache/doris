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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 * Representation of a Kill command.
 * Acceptable syntax:
 *  KILL (CONNECTION)? INTEGER_VALUE              #killConnection
 *  KILL QUERY (INTEGER_VALUE | STRING_LITERAL)   #killQuery
 */
public class KillCommand extends Command implements NoForward {
    private static final Logger LOG = LogManager.getLogger(KillCommand.class);

    private final boolean isConnectionKill;
    private final int connectionId;
    private final String queryId;

    public KillCommand(boolean isConnectionKill, int connectionId) {
        super(PlanType.KILL_COMMAND);
        this.isConnectionKill = isConnectionKill;
        this.connectionId = connectionId;
        this.queryId = "";
    }

    public KillCommand(String queryId) {
        super(PlanType.KILL_COMMAND);
        this.isConnectionKill = false;
        this.connectionId = -1;
        this.queryId = queryId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        ConnectContext killCtx = null;
        if (connectionId == -1) {
            // when killCtx == null, this means the query not in FE,
            // then we just send kill signal to BE
            killCtx = ctx.getConnectScheduler().getContextWithQueryId(queryId);
        } else {
            killCtx = ctx.getConnectScheduler().getContext(connectionId);
            if (killCtx == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, connectionId);
            }
        }

        if (killCtx == null) {
            TUniqueId tQueryId = null;
            try {
                tQueryId = DebugUtil.parseTUniqueIdFromString(queryId);
            } catch (NumberFormatException e) {
                throw new UserException(e.getMessage());
            }
            LOG.info("kill query {}", queryId);
            Collection<Backend> nodesToPublish = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values();
            for (Backend be : nodesToPublish) {
                if (be.isAlive()) {
                    try {
                        Status cancelReason = new Status(TStatusCode.CANCELLED, "user kill query");
                        BackendServiceProxy.getInstance()
                                .cancelPipelineXPlanFragmentAsync(be.getBrpcAddress(), tQueryId, cancelReason);
                    } catch (Throwable t) {
                        LOG.info("send kill query {} rpc to be {} failed", queryId, be);
                    }
                }
            }
        } else if (ctx == killCtx) {
            // Suicide
            ctx.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ConnectContext.get().getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, connectionId);
            }

            killCtx.kill(isConnectionKill);
        }
        ctx.getState().setOk();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("KILL ");
        if (!isConnectionKill) {
            sb.append("QUERY ");
        }
        sb.append(connectionId);
        return sb.toString();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitKillCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.KILL;
    }
}

