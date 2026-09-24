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

package org.apache.doris.nereids.trees.plans.commands.spm;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Optional;

/**
 * DROP BASELINE PLAN command (design doc 6.8 / 6.9 / 6.17).
 *
 * Syntax:
 *
 *   DROP BASELINE PLAN [IF EXISTS] baseline_id
 *
 * Execution logic: the id itself decides the scope (BaselineScope.ofId), so the command
 * routes by id range instead of guessing from which store happens to contain the id: a
 * SESSION-range id is handled by the connection-local SessionBaselineStore (memory only -
 * a miss means "does not exist" and is NEVER resolved against the shared store), a
 * GLOBAL-range id by BaselineManager.getInstance() (GLOBAL; also deletes the
 * spm_baselines internal table record). Without IF EXISTS a missing baseline is an error.
 *
 * Execution location: a SESSION-range id can only exist in this connection's FE-local
 * store and is dropped locally; a GLOBAL-range id is cluster metadata, so the command
 * runs on the master FE (Redirect/Forward, single writer), like other DDL - even while
 * the local cache does not know the id yet.
 */
public class DropBaselinePlanCommand extends Command implements Forward {

    /** The baseline id to drop. */
    private final long baselineId;

    /** Whether to carry the IF EXISTS semantics. */
    private final boolean ifExists;

    public DropBaselinePlanCommand(long baselineId, boolean ifExists) {
        super(PlanType.DROP_BASELINE_PLAN_COMMAND);
        this.baselineId = baselineId;
        this.ifExists = ifExists;
    }

    public long getBaselineId() {
        return baselineId;
    }

    @Override
    public Optional<Origin> getOrigin() {
        return super.getOrigin();
    }

    public boolean isIfExists() {
        return ifExists;
    }

    /**
     * The id range decides the execution location (BaselineScope.ofId) - never the local
     * store content: a session-range id can only be dropped on this FE, a global-range id
     * always goes to the master FE (single writer). A stale / lagging local cache must
     * not make a global DDL run on a follower.
     */
    @Override
    public RedirectStatus toRedirectStatus() {
        return BaselineScope.ofId(baselineId) == BaselineScope.SESSION
                ? RedirectStatus.NO_FORWARD
                : RedirectStatus.FORWARD_NO_SYNC;
    }

    /**
     * Runs on the FE the user is connected to after the forwarded DROP finished on the
     * master: refresh the local cache right away so the removed baseline stops matching on
     * this FE without waiting for the next BaselineRefreshDaemon cycle.
     */
    @Override
    public void afterForwardToMaster(ConnectContext ctx) {
        BaselineManager.getInstance().refreshFromInternalTable();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // SPM management commands require ADMIN
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(
                ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // ids are self-describing: a session-range id can only exist in this
        // connection's own store (a miss is final - it is never resolved against the
        // shared store), a global-range id only in the shared manager
        boolean dropped = BaselineScope.ofId(baselineId) == BaselineScope.SESSION
                ? ctx.getSessionBaselineStore().dropBaseline(baselineId)
                : BaselineManager.getInstance().dropBaseline(baselineId);
        if (!dropped && !ifExists) {
            throw new AnalysisException("Baseline plan " + baselineId + " does not exist");
        }
        String message = "Dropped baseline plan " + baselineId;
        ctx.getState().setOk(0, 0, message);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropBaselinePlanCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }

    @Override
    public void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        throw new DdlException("DROP BASELINE PLAN is not supported in cloud mode");
    }
}
