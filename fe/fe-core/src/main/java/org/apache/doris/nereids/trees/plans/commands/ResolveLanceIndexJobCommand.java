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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceIndexMutationValidator;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.nio.charset.StandardCharsets;

/**
 * RESOLVE LANCE INDEX JOB &lt;jobId&gt; AS FORCE_RELEASE COMMENT '&lt;note&gt;' — the operator
 * escape hatch that durably releases a job whose mutation outcome is UNKNOWN (design section
 * 7.1). RESOLVE is deliberately not gated by {@code enable_lance_index_mutation}: the gate
 * controls mutation admission, while FORCE must stay available exactly when the gate is off.
 *
 * <p>The release protocol keeps the fence, the quota charge and the possible-live slot while
 * it performs one authoritative latest-metadata read and one external-table refresh with the
 * current credentials of the surviving catalog, both outside every catalog/manager lock; only
 * then does the durable release transfer inside the admission critical section
 * ({@code captureLanceIndexTarget} → lock-free read/refresh → {@code withLanceIndexAdmission}
 * recheck → manager write lock), serialized against DROP CATALOG and identity ALTER exactly
 * like admission. Any failure before the transfer is the typed
 * {@code ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE}: nothing is written, nothing is released,
 * and the operator fixes the cause and retries the same statement.
 *
 * <p>Orphan branches (design section 7.1 step 1): a fully orphaned job (catalog gone) has no
 * credentials to read with and nothing to invalidate, so it is released directly after global
 * ADMIN authorization; a half-orphan (catalog alive, persisted db/table no longer resolvable)
 * skips the authoritative read and refreshes with {@code ignoreIfNotExists=true} as a
 * best-effort invalidation. A non-null exception while resolving the target is never an
 * orphan verdict — it is treated as a refresh failure so the fence is kept when "table gone"
 * cannot be told apart from "network down".
 *
 * <p>Non-disclosure (design section 8): the job is loaded first and authorized against its
 * persisted target — table-level ALTER when the target resolves, global ADMIN otherwise — and
 * a missing job and an unauthorized job share the same fixed ERR_LANCE_INDEX_JOB_NOT_FOUND
 * response naming only the job id. The 5104 state rejection is only visible to an already
 * authorized caller.
 *
 * <p>Success returns an OK packet carrying one warning row with {@link #LATE_COMMIT_WARNING},
 * the same text persisted as the job's durable {@code forceWarning}: the old worker may still
 * overwrite, remove, or reintroduce the index name; the mutation outcome remains UNKNOWN.
 * Retrying FORCE on an already released job is an idempotent success returning the existing
 * release record, never an error.
 */
public class ResolveLanceIndexJobCommand extends Command implements ForwardWithSync {
    /**
     * The late-commit warning (design section 7.1), returned in the OK packet and persisted
     * verbatim as the durable {@code forceWarning}; bounded well under
     * {@link LanceIndexJob#MAX_FORCE_TEXT_BYTES}.
     */
    static final String LATE_COMMIT_WARNING =
            "the old worker may still overwrite, remove, or reintroduce the index name; "
                    + "the mutation outcome remains UNKNOWN";

    private final long jobId;
    private final String comment;

    public ResolveLanceIndexJobCommand(long jobId, String comment) {
        super(PlanType.RESOLVE_LANCE_INDEX_JOB_COMMAND);
        this.jobId = jobId;
        this.comment = comment;
    }

    public long getJobId() {
        return jobId;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        Env env = Env.getCurrentEnv();
        LanceIndexJobManager manager = env.getLanceIndexJobManager();
        // 1. Load the job without disclosing any field (design section 7.1 step 1).
        LanceIndexJob job = manager.getJob(jobId);
        if (job == null) {
            throw notFound();
        }
        // 2. Authorize against the persisted target before any state is revealed: table-level
        //    ALTER when the target resolves, global ADMIN for an orphan or half-orphan.
        CatalogMgr catalogMgr = env.getCatalogMgr();
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = catalogMgr.getCatalog(job.getCatalogId());
        boolean targetResolves = ShowLanceIndexJobsCommand.targetResolves(catalog, job);
        boolean authorized = targetResolves
                ? env.getAccessManager().checkTblPriv(ctx, catalog.getName(), job.getDbName(), job.getTableName(),
                        PrivPredicate.ALTER)
                : env.getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        if (!authorized) {
            throw notFound();
        }
        // 3. Idempotent replay: a retry returns the existing release record (section 7.1).
        if (job.isForceReleased()) {
            ctx.getState().setOk(0, 1, LATE_COMMIT_WARNING);
            return;
        }
        // 4. Only UNKNOWN may be force-released; a null mutation state reads as UNKNOWN,
        //    same as the manager's own gate.
        if (job.getMutationState() != null && job.getMutationState() != LanceIndexJobMutationState.UNKNOWN) {
            throw new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN.formatErrorMsg(jobId),
                    ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN);
        }
        // 5. The grammar makes COMMENT mandatory; here the note must also be non-empty after
        //    trimming and fit the durable force text bound.
        String note = comment == null ? "" : comment.trim();
        if (note.isEmpty()) {
            throw new AnalysisException("force release note must not be empty",
                    ErrorCode.ERR_LANCE_INDEX_INVALID);
        }
        if (note.getBytes(StandardCharsets.UTF_8).length > LanceIndexJob.MAX_FORCE_TEXT_BYTES) {
            throw new AnalysisException("force release note exceeds " + LanceIndexJob.MAX_FORCE_TEXT_BYTES
                    + " UTF-8 bytes", ErrorCode.ERR_LANCE_INDEX_INVALID);
        }
        // 6-9. Branch on the orphan state, then the durable release transfer.
        String actor = ctx.getQualifiedUser();
        boolean released;
        if (catalog == null) {
            // Full orphan: no credentials survive to read with and nothing can be
            // invalidated, so the release goes straight to the manager write lock.
            released = manager.forceRelease(jobId, job.getRevision(), actor, note, LATE_COMMIT_WARNING);
        } else {
            released = releaseWithLiveCatalog(env, catalogMgr, manager, catalog, targetResolves, job, actor, note);
        }
        if (!released) {
            // 10. The expected-revision transfer lost a race. A concurrent FORCE_RELEASE
            //     that already landed makes this an idempotent success; anything else means
            //     the job left UNKNOWN concurrently (UNKNOWN has no other outgoing
            //     transition), so the pinned not-UNKNOWN wording stays accurate.
            LanceIndexJob reread = manager.getJob(jobId);
            if (reread != null && reread.isForceReleased()) {
                ctx.getState().setOk(0, 1, LATE_COMMIT_WARNING);
                return;
            }
            throw new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN.formatErrorMsg(jobId),
                    ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN);
        }
        // 11. The OK packet carries the late-commit warning; it survives the forward chain
        //     byte-identically (proxyExecute serializes the master state).
        ctx.getState().setOk(0, 1, LATE_COMMIT_WARNING);
    }

    /**
     * The live-catalog release path: capture the target identity, then — holding no lock —
     * perform the authoritative read and the refresh, and finally transfer the release inside
     * the admission critical section. Lock order stays CatalogMgr then LanceIndexJobManager.
     */
    private boolean releaseWithLiveCatalog(Env env, CatalogMgr catalogMgr, LanceIndexJobManager manager,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog, boolean targetResolves, LanceIndexJob job,
            String actor, String note) throws Exception {
        if (!(catalog instanceof LanceExternalCatalog)
                || ((LanceExternalCatalog) catalog).isRestCatalogConfigured()) {
            // Defensive: admission never targets a REST catalog and a non-Lance catalog
            // cannot hold a Lance fence, so no UNKNOWN job should ever resolve here.
            LanceIndexMutationValidator.rejectUnsupportedOperation(
                    "RESOLVE LANCE INDEX JOB AS FORCE_RELEASE", "catalog '" + catalog.getName() + "'");
        }
        LanceExternalCatalog lanceCatalog = (LanceExternalCatalog) catalog;
        CatalogMgr.LanceIndexTarget target;
        try {
            target = catalogMgr.captureLanceIndexTarget(lanceCatalog);
        } catch (DdlException e) {
            throw incompleteResolution(e.getMessage());
        }
        if (targetResolves) {
            // 7. One authoritative latest-metadata read with current credentials, proving
            //    the dataset is reachable before anything is released.
            authoritativeRead(lanceCatalog, catalog, job);
        }
        // 8. Invalidate the external table and broadcast the refresh to every FE. A
        //    half-orphan target is invalidated best-effort (missing db/table is a no-op).
        try {
            env.getRefreshManager().handleRefreshTable(catalog.getName(), job.getDbName(), job.getTableName(),
                    !targetResolves);
        } catch (DdlException e) {
            throw incompleteResolution(e.getMessage());
        }
        // 9. The durable transfer rechecks the catalog identity under the read lock, then
        //    runs the revision-checked release under the manager write lock.
        try {
            return catalogMgr.withLanceIndexAdmission(lanceCatalog, target,
                    () -> manager.forceRelease(jobId, job.getRevision(), actor, note, LATE_COMMIT_WARNING));
        } catch (DdlException e) {
            throw incompleteResolution(e.getMessage());
        }
    }

    /**
     * The authoritative latest-metadata read over the remote names of the resolved db/table,
     * outside every lock (the loader owns its deadline-bound JNI read). Every loader failure
     * already passed through the catalog's sanitized root-cause chain, so its message is safe
     * to echo — locator, credentials and dataset uri are masked there.
     */
    private void authoritativeRead(LanceExternalCatalog lanceCatalog,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog, LanceIndexJob job) throws AnalysisException {
        DatabaseIf<? extends TableIf> db;
        TableIf table;
        try {
            db = catalog.getDbNullable(job.getDbName());
            table = db == null ? null : db.getTableNullable(job.getTableName());
        } catch (RuntimeException e) {
            // A remote-metadata blip during resolution is not an orphan verdict; the provider
            // message is not echoed here because it never crossed the sanitized chain.
            throw incompleteResolution("the persisted target could not be resolved with current catalog"
                    + " metadata; see fe.log for the cause");
        }
        if (db == null || table == null) {
            // The target resolved at authorization time but is gone now: keep the fence and
            // let the retry take the half-orphan branch under global ADMIN.
            throw incompleteResolution("the persisted target table no longer resolves; retry the statement");
        }
        String remoteDb = ((ExternalDatabase) db).getRemoteName();
        String remoteTable = ((ExternalTable) table).getRemoteName();
        try {
            lanceCatalog.loadTableIndexAdmissionSnapshot(remoteDb, remoteTable);
        } catch (Exception e) {
            throw incompleteResolution(e.getMessage());
        }
    }

    private AnalysisException notFound() {
        return new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND.formatErrorMsg(jobId),
                ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND);
    }

    private AnalysisException incompleteResolution(String detail) {
        return new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE.formatErrorMsg(jobId, detail),
                ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitResolveLanceIndexJobCommand(this, context);
    }
}
