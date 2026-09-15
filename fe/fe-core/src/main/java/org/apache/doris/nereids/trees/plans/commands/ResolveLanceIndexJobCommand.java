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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * <p>Target resolution (design section 7.1 step 1) is three-valued. RESOLVED means the
 * persisted names resolve and the catalog's current durable dataset locator still matches
 * the job's — the same revalidation SHOW LANCE INDEX JOBS applies, so a repointed dataset
 * reusing the same names never turns a stale name into table-level authorization. MISSING
 * means the catalog, database or table is verifiably absent, or the locator positively
 * points at a different dataset: that is the orphan family — a full orphan (catalog gone)
 * has no credentials to read with and nothing to invalidate, so it is released directly
 * after global ADMIN authorization, while a half-orphan skips the authoritative read and
 * refreshes with {@code ignoreIfNotExists=true} as a best-effort invalidation. FAILED means
 * a resolution that errors out, or a locator that cannot be resolved right now: never an
 * orphan verdict — after ADMIN authorization the statement fails with the typed 5105 so the
 * fence is kept when "table gone" cannot be told apart from "network down". SHOW fails the
 * same uncertainty closed by hiding the row; RESOLVE fails it closed by not releasing.
 *
 * <p>Non-disclosure (design section 8): the job is loaded first and authorized against its
 * persisted target — table-level ALTER when the target resolves, global ADMIN otherwise — and
 * a missing job and an unauthorized job share the same fixed ERR_LANCE_INDEX_JOB_NOT_FOUND
 * response naming only the job id. The 5104 state rejection and the 5105 resolution failure
 * are only visible to an already authorized caller.
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

    private static final Logger LOG = LogManager.getLogger(ResolveLanceIndexJobCommand.class);

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
        // 2. Resolve and authorize against the persisted target before any state is revealed:
        //    table-level ALTER when the target resolves, global ADMIN for the orphan family
        //    and for a target whose resolution failed outright.
        CatalogMgr catalogMgr = env.getCatalogMgr();
        CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = catalogMgr.getCatalog(job.getCatalogId());
        TargetResolution resolution = resolveTarget(catalog, job);
        boolean authorized = resolution == TargetResolution.RESOLVED
                ? env.getAccessManager().checkTblPriv(ctx, catalog.getName(), job.getDbName(), job.getTableName(),
                        PrivPredicate.ALTER)
                : env.getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        if (!authorized) {
            throw notFound();
        }
        // 3. Idempotent replay: a retry returns the existing release record (section 7.1).
        //    This deliberately precedes the resolution-failure rejection: once the release
        //    has landed, a retry during a provider outage is a success, not a 5105.
        if (job.isForceReleased()) {
            ctx.getState().setOk(0, 1, LATE_COMMIT_WARNING);
            return;
        }
        // 4. Only UNKNOWN may be force-released; a null mutation state reads as UNKNOWN,
        //    same as the manager's own gate. The state rejection also precedes the
        //    resolution-failure rejection: for a terminal job the accurate answer is 5104,
        //    not a 5105 claiming the job still holds its fence.
        if (job.getMutationState() != null && job.getMutationState() != LanceIndexJobMutationState.UNKNOWN) {
            throw new AnalysisException(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN.formatErrorMsg(jobId),
                    ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN);
        }
        if (resolution == TargetResolution.FAILED) {
            // Never an orphan verdict: "table gone" cannot be told apart from "network down",
            // so nothing is released and nothing beyond the typed error is disclosed; the
            // operator fixes the cause and retries the same statement (design 7.1 step 4).
            throw incompleteResolution("the persisted target could not be resolved with current catalog"
                    + " metadata; see fe.log for the cause");
        }
        boolean targetResolves = resolution == TargetResolution.RESOLVED;
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
        } catch (Exception e) {
            // A remote-metadata blip during resolution is not an orphan verdict; the provider
            // message is not echoed here because it never crossed the sanitized chain.
            LOG.warn("lance index job {}: target re-resolution failed before the authoritative read",
                    job.getJobId(), e);
            throw incompleteResolution("the persisted target could not be resolved with current catalog"
                    + " metadata; see fe.log for the cause");
        }
        if (db == null || table == null) {
            // The target resolved at authorization time but is gone now: keep the fence and
            // let the retry take the half-orphan branch under global ADMIN.
            throw incompleteResolution("the persisted target table no longer resolves; retry the statement");
        }
        if (!(db instanceof ExternalDatabase) || !(table instanceof ExternalTable)) {
            // Boundary guard: a Lance catalog must serve external relations, but the command
            // boundary does not trust that invariant (no raw ClassCastException to the user).
            LOG.warn("lance index job {}: target relation is not external: db={}, table={}",
                    job.getJobId(), db.getClass().getName(), table.getClass().getName());
            throw incompleteResolution("the persisted target is not an external relation;"
                    + " see fe.log for the cause");
        }
        String remoteDb = ((ExternalDatabase) db).getRemoteName();
        String remoteTable = ((ExternalTable) table).getRemoteName();
        try {
            lanceCatalog.loadTableIndexAdmissionSnapshot(remoteDb, remoteTable);
        } catch (Exception e) {
            throw incompleteResolution(e.getMessage());
        }
    }

    /**
     * The three-way verdict on the job's persisted target. Deliberately not SHOW's
     * {@code targetResolves}: SHOW must keep listing through provider outages, so it folds
     * every failed resolution into its orphan rule; RESOLVE takes a durable action on the
     * verdict and only releases on positive evidence, so a failed resolution stays its own
     * outcome here.
     */
    private enum TargetResolution {
        /** Names resolve and the catalog's current durable locator still matches the job's. */
        RESOLVED,
        /** Catalog, database or table verifiably absent, or the locator positively repointed. */
        MISSING,
        /** Resolution errored out, or the locator cannot be resolved right now. */
        FAILED
    }

    /**
     * Resolves the persisted target once, up front, distinguishing "verifiably gone" (the
     * orphan family, releasable under global ADMIN) from "could not tell" (fail with 5105,
     * keep the fence). The locator leg mirrors {@link ShowLanceIndexJobsCommand}: a null
     * current locator means the provider is unreachable or the names no longer resolve
     * remotely, which is absence of evidence either way — it fails closed here instead of
     * granting the half-orphan release path.
     */
    static TargetResolution resolveTarget(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
            LanceIndexJob job) {
        if (catalog == null) {
            return TargetResolution.MISSING;
        }
        DatabaseIf<? extends TableIf> db;
        try {
            db = catalog.getDbNullable(job.getDbName());
        } catch (Exception e) {
            LOG.warn("lance index job {}: target database resolution failed", job.getJobId(), e);
            return TargetResolution.FAILED;
        }
        if (db == null) {
            return TargetResolution.MISSING;
        }
        TableIf table;
        try {
            table = db.getTableNullable(job.getTableName());
        } catch (Exception e) {
            LOG.warn("lance index job {}: target table resolution failed", job.getJobId(), e);
            return TargetResolution.FAILED;
        }
        if (table == null) {
            return TargetResolution.MISSING;
        }
        if (!(catalog instanceof LanceExternalCatalog)) {
            return TargetResolution.RESOLVED;
        }
        String currentLocator = ((LanceExternalCatalog) catalog).resolveCurrentIndexJobLocator(
                job.getDbName(), job.getTableName());
        if (currentLocator == null) {
            // The catalog folds provider outages and unresolvable names into null (and logs
            // nothing), so the cause trail for the 5105 starts here.
            LOG.warn("lance index job {}: current dataset locator could not be resolved", job.getJobId());
            return TargetResolution.FAILED;
        }
        return currentLocator.equals(job.getNormalizedLocator())
                ? TargetResolution.RESOLVED : TargetResolution.MISSING;
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
