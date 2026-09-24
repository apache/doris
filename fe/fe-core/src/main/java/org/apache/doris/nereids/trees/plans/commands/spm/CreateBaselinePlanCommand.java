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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * CREATE BASELINE PLAN command (design doc 6.8 / 6.9 / 6.13).
 *
 * Syntax:
 *
 *   CREATE [GLOBAL | SESSION] BASELINE PLAN
 *     'bindSql'
 *   [ WITH
 *     'planSql' ]
 *
 * The WITH clause is optional: when it is omitted the bindSql is frozen as the
 * planSql too (plan == bind).
 *
 * Execution flow (design doc 6.13.2):
 *
 *   1. Parse bindSql (its value-free full-query digest becomes the matching key).
 *   2. Optimize planSql in SPM mode (SPMOptimizer, state-sensitive rules disabled) and
 *      decompile the best physical plan back to a frozen planSql (SPMPlan2SQLBuilder).
 *   3. Parameterize bindSql and planSql over their WHOLE plan trees with one shared
 *      placeholder builder and assemble the BaselinePlan (SPMPlanner.buildBaselineFromSql).
 *   4. Persist the baseline (GLOBAL via BaselineManager; SESSION keeps it in memory and
 *      is cleared when the session ends).
 *
 * Execution location: a GLOBAL baseline is cluster metadata, so the command runs on the
 * master FE (Redirect/Forward, like CreateFunctionCommand): ids and the duplicate check are
 * allocated by a single writer and no longer race between FEs. A SESSION baseline lives in
 * the FE-local SessionBaselineStore of the connection and always runs locally.
 *
 * Duplicate detection (same digest + planSql) naturally implements the "IF NOT EXISTS"
 * semantics: createBaseline returns the existing id instead of adding a new row.
 */
public class CreateBaselinePlanCommand extends Command implements Forward {

    private static final Logger LOG = LogManager.getLogger(CreateBaselinePlanCommand.class);

    /** Storage scope: GLOBAL (default) / SESSION. */
    private final BaselineScope scope;

    /** The binding SQL (SELECT) used to match future queries. */
    private final String bindSql;

    /** The plan SQL (SELECT, may contain SET_VAR hints) whose plan is frozen. When the
     * WITH clause was omitted this equals bindSql. */
    private final String planSql;

    public CreateBaselinePlanCommand(BaselineScope scope, String bindSql, String planSql) {
        super(PlanType.CREATE_BASELINE_PLAN_COMMAND);
        this.scope = scope;
        this.bindSql = bindSql;
        // WITH omitted -> freeze the bindSql as the planSql
        this.planSql = planSql != null ? planSql : bindSql;
    }

    public BaselineScope getScope() {
        return scope;
    }

    public String getBindSql() {
        return bindSql;
    }

    public String getPlanSql() {
        return planSql;
    }

    @Override
    public Optional<Origin> getOrigin() {
        return super.getOrigin();
    }

    /**
     * GLOBAL baselines are cluster metadata: run them on the master FE (single writer), so
     * the id allocation and the (hash, digest, planSql) dedup no longer race between FEs
     * (same pattern as CreateFunctionCommand). SESSION baselines live in this connection's
     * FE-local store and must run where the connection is. Baseline rows are not journaled
     * metadata, so there is nothing to wait for after the forward (FORWARD_NO_SYNC).
     */
    @Override
    public RedirectStatus toRedirectStatus() {
        return scope == BaselineScope.SESSION
                ? RedirectStatus.NO_FORWARD
                : RedirectStatus.FORWARD_NO_SYNC;
    }

    /**
     * Runs on the FE the user is connected to after the forwarded CREATE finished on the
     * master: refresh the local cache right away so the new baseline becomes visible on
     * this FE without waiting for the next BaselineRefreshDaemon cycle.
     */
    @Override
    public void afterForwardToMaster(ConnectContext ctx) {
        BaselineManager.getInstance().refreshFromInternalTable();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 1. privilege check: SPM DDL requires ADMIN
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(
                ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // 2. build the baseline (parse bindSql + optimize planSql + decompile +
        //    whole-tree parameterize of both SQLs)
        SPMPlanner spmPlanner = new SPMPlanner();
        BaselinePlan baseline = spmPlanner.buildBaselineFromSql(ctx, bindSql, planSql);
        baseline.setSource(org.apache.doris.nereids.spm.BaselineSource.USER);
        baseline.setStatus(org.apache.doris.nereids.spm.BaselineStatus.ENABLED);
        baseline.setScope(scope);
        // Audit correlation: the statement query id (DebugUtil.printId - the exact value
        // the audit log records as query_id, "NaN" when the context carries none)
        // identifies this CREATE statement in __internal_schema.audit_log. Storing it
        // on the baseline lets users look up the audit row - whose stmt text contains
        // this statement's bindSql literal verbatim - by query id (SHOW BASELINE PLANS
        // exposes it as the query_id column, and the internal table persists it).
        baseline.setQueryId(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()));

        // 3. store the baseline: GLOBAL goes to the shared BaselineManager (in-memory
        //    index + __internal_schema.spm_baselines persistence), SESSION stays in the
        //    connection's SessionBaselineStore (memory only, gone with the session)
        long id = scope == BaselineScope.SESSION
                ? ctx.getSessionBaselineStore().createBaseline(baseline)
                : BaselineManager.getInstance().createBaseline(baseline);

        // The statement-level sql_hash (ConnectContext.getSqlHash(), md5Hex of the
        // statement text - the same value the audit log records) is the audit-log
        // correlation key next to the stored queryId; bindSqlHash stays the VALUE-FREE
        // structural key the rewrite matcher uses (see SPMUtils.hashOf). Logging all
        // three keeps SPM baselines correlatable with the audit log without changing
        // the matching semantics.
        LOG.info("SPM baseline created: id={}, scope={}, bindSqlHash={}, statementSqlHash={}, queryId={}",
                id, scope, baseline.getBindSqlHash(), ctx.getSqlHash(), baseline.getQueryId());

        // 4. return the created baseline id as a single-row result set so the caller
        //    (SQL client / regression test) can locate the baseline row by id
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("id", ScalarType.createType(PrimitiveType.BIGINT)))
                .build();
        ShowResultSet resultSet = new ShowResultSet(metaData,
                ImmutableList.of(Lists.newArrayList(String.valueOf(id))));
        executor.sendResultSet(resultSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateBaselinePlanCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    @Override
    public void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        throw new DdlException("CREATE BASELINE PLAN is not supported in cloud mode");
    }
}
