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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import mockit.VerificationsInOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers RESOLVE LANCE INDEX JOB jobId AS FORCE_RELEASE COMMENT 'note': the job is loaded
 * first and authorized against its persisted target before any state is revealed (a missing
 * job and an unauthorized job share the fixed ERR_LANCE_INDEX_JOB_NOT_FOUND text naming only
 * the job id); only an UNKNOWN job may be released; every failure before the durable
 * transfer is the typed ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE that leaves the job, its
 * fence and its quota untouched; success and idempotent retries return the OK packet
 * triple (0 affected rows, 1 warning, LATE_COMMIT_WARNING).
 */
public class ResolveLanceIndexJobCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private LanceIndexJobManager lanceIndexJobManager;
    @Mocked
    private CatalogMgr catalogMgr;
    @Mocked
    private RefreshManager refreshManager;
    @Mocked
    private QueryState queryState;
    @Mocked
    private LanceExternalCatalog lanceCatalog;
    @Mocked
    private ExternalDatabase database;
    @Mocked
    private ExternalTable table;

    private static LanceIndexJob newUnknownJob(long jobId) {
        LanceIndexJob job = new LanceIndexJob(jobId, "creator", 10L, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, "s3://bucket/dataset",
                "idx1", LanceIndexNameNormalizer.normalize("idx1"),
                LanceIndexJobMutationType.CREATE, true, false, "IVF_PQ", "v", null, 7L, null);
        job.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        job.setRevision(3L);
        return job;
    }

    private void expectEnv(LanceIndexJob job) {
        expectEnvBase();
        new Expectations() {
            {
                lanceIndexJobManager.getJob(anyLong);
                minTimes = 0;
                result = job;
            }
        };
    }

    private void expectEnvBase() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getLanceIndexJobManager();
                minTimes = 0;
                result = lanceIndexJobManager;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                env.getRefreshManager();
                minTimes = 0;
                result = refreshManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getState();
                minTimes = 0;
                result = queryState;

                connectContext.getQualifiedUser();
                minTimes = 0;
                result = "operator";
            }
        };
    }

    private void expectResolvableTarget(boolean alterGranted) {
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = alterGranted;
            }
        };
    }

    /**
     * The admission critical section is mocked but still runs the action, so the command's
     * manager call happens exactly as in production; the manager's release result is the
     * given value.
     */
    private void expectAdmissionTransfer(final boolean releaseResult) throws Exception {
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                catalogMgr.withLanceIndexAdmission((LanceExternalCatalog) any, (CatalogMgr.LanceIndexTarget) any,
                        (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                minTimes = 0;
                result = new Delegate<Object>() {
                    Object runAdmission(LanceExternalCatalog catalog, CatalogMgr.LanceIndexTarget target,
                            CatalogMgr.LanceIndexAdmissionAction<?> action) throws Exception {
                        return action.run();
                    }
                };

                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                minTimes = 0;
                result = releaseResult;
            }
        };
    }

    @Test
    public void testMissingJobAndUnauthorizedShareSameResponse() {
        // Missing job: 5103 naming only the job id.
        expectEnv(null);
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, missing.getMysqlErrorCode());
        Assertions.assertTrue(missing.getMessage().contains("Lance index job not found: 42"));

        // Resolvable target but no table ALTER privilege: byte-identical response.
        expectEnv(newUnknownJob(42L));
        expectResolvableTarget(false);
        AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(missing.getMessage(), denied.getMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, denied.getMysqlErrorCode());
    }

    @Test
    public void testOrphanRequiresAdmin() {
        expectEnv(newUnknownJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, e.getMysqlErrorCode());
        new Verifications() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                times = 1;
                accessControllerManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                times = 0;
            }
        };
    }

    @Test
    public void testHalfOrphanRequiresAdmin() {
        // Catalog resolves but the persisted db no longer does: same ADMIN-only rule.
        expectEnv(newUnknownJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, e.getMysqlErrorCode());
    }

    @Test
    public void testProxyContextCarriesIdentity() throws Exception {
        // The privilege check runs against the context handed to run, not the thread-local.
        ConnectContext proxyCtx = new ConnectContext();
        LanceIndexJob job = newUnknownJob(42L);
        job.setForceReleased(true);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv((ConnectContext) any, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };

        new ResolveLanceIndexJobCommand(42L, "note").run(proxyCtx, null);
        new Verifications() {
            {
                accessControllerManager.checkTblPriv(proxyCtx, "lance_ctl", "db1", "tbl1", PrivPredicate.ALTER);
                times = 1;
            }
        };
    }

    @Test
    public void testBlankNoteRejected() throws Exception {
        expectEnv(newUnknownJob(42L));
        expectResolvableTarget(true);
        AnalysisException blank = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "   ").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, blank.getMysqlErrorCode());
        Assertions.assertTrue(blank.getMessage().contains("force release note must not be empty"));

        AnalysisException nullNote = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, null).run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, nullNote.getMysqlErrorCode());
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testTooLongNoteRejected() throws Exception {
        expectEnv(newUnknownJob(42L));
        expectResolvableTarget(true);
        String ascii = String.join("", Collections.nCopies(LanceIndexJob.MAX_FORCE_TEXT_BYTES + 1, "a"));
        AnalysisException tooLong = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, ascii).run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, tooLong.getMysqlErrorCode());
        Assertions.assertTrue(tooLong.getMessage().contains("exceeds 1024 UTF-8 bytes"));

        // The bound is on UTF-8 bytes, not characters: 513 two-byte characters overflow it.
        String multibyte = String.join("", Collections.nCopies(513, "é"));
        AnalysisException multibyteTooLong = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, multibyte).run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID, multibyteTooLong.getMysqlErrorCode());
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testNonUnknownStateRejected() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        LanceIndexJobMutationState[] nonUnknown = {
            LanceIndexJobMutationState.PENDING, LanceIndexJobMutationState.RUNNING,
            LanceIndexJobMutationState.COMMITTED, LanceIndexJobMutationState.NOT_COMMITTED};
        for (LanceIndexJobMutationState state : nonUnknown) {
            job.setMutationState(state);
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN, e.getMysqlErrorCode());
            Assertions.assertTrue(e.getMessage().contains("cannot be resolved: not in UNKNOWN state"));
        }
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testAlreadyForceReleasedIsIdempotentSuccess() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        job.setForceReleased(true);
        expectEnv(job);
        expectResolvableTarget(true);

        new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null);
        new Verifications() {
            {
                queryState.setOk(0L, 1, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testFullOrphanSuccessSkipsReadRefreshAndAdmission() throws Exception {
        // A fully orphaned job (catalog gone) is released straight in the manager write
        // lock: no target capture, no authoritative read, no refresh can even be attempted.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                minTimes = 0;
                result = true;
            }
        };
        new ResolveLanceIndexJobCommand(42L, "  release fence  ").run(connectContext, null);
        new Verifications() {
            {
                // The manager receives the job's current revision and the trimmed note.
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "release fence",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                times = 0;
                catalogMgr.withLanceIndexAdmission((LanceExternalCatalog) any, (CatalogMgr.LanceIndexTarget) any,
                        (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                times = 0;
                queryState.setOk(0L, 1, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
    }

    @Test
    public void testHalfOrphanSuccessUsesBestEffortRefresh() throws Exception {
        // Catalog alive but persisted db no longer resolvable: skip the authoritative read,
        // invalidate best-effort (ignoreIfNotExists=true), then release in the critical
        // section.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        expectAdmissionTransfer(true);
        new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null);
        new Verifications() {
            {
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable("lance_ctl", "db1", "tbl1", true);
                times = 1;
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "note",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                queryState.setOk(0L, 1, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
    }

    @Test
    public void testNormalPathSuccessRunsCaptureReadRefreshAdmissionInOrder() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        expectAdmissionTransfer(true);

        new ResolveLanceIndexJobCommand(42L, "release fence").run(connectContext, null);
        new VerificationsInOrder() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                refreshManager.handleRefreshTable("lance_ctl", "db1", "tbl1", false);
                catalogMgr.withLanceIndexAdmission((LanceExternalCatalog) any, (CatalogMgr.LanceIndexTarget) any,
                        (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "release fence",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
            }
        };
        new Verifications() {
            {
                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                times = 1;
                queryState.setOk(0L, 1, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
    }

    @Test
    public void testRefreshFailureKeepsJobUnresolved() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                minTimes = 0;
                result = new DdlException("refresh boom");
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("was not released"));
        Assertions.assertTrue(e.getMessage().contains("refresh boom"));
        // Nothing was released and the job record is untouched.
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, job.getMutationState());
        Assertions.assertFalse(job.isForceReleased());
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testAuthoritativeReadFailureKeepsJobUnresolved() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = new AnalysisException("dataset unreachable");
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("dataset unreachable"));
        new Verifications() {
            {
                // The read precedes the refresh: neither the refresh nor the release ran.
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testTargetResolutionBlipKeepsJobUnresolved() throws Exception {
        // A non-null exception while re-resolving the target is not an orphan verdict; its
        // provider message never crossed the sanitized chain and must not be echoed.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        AtomicInteger resolveCalls = new AtomicInteger();
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = new Delegate<DatabaseIf>() {
                    DatabaseIf resolve(String name) {
                        if (resolveCalls.incrementAndGet() == 1) {
                            return database;
                        }
                        throw new RuntimeException("s3://bucket/dataset meta blip");
                    }
                };

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;

                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("could not be resolved with current catalog metadata"));
        Assertions.assertFalse(e.getMessage().contains("s3://bucket/dataset"));
        new Verifications() {
            {
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testTargetVanishedBeforeReadKeepsJobUnresolved() throws Exception {
        // The target resolves at authorization time but is gone when the read re-resolves
        // it: keep the fence and let the retry take the half-orphan branch under ADMIN.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        AtomicInteger resolveCalls = new AtomicInteger();
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = new Delegate<DatabaseIf>() {
                    DatabaseIf resolve(String name) {
                        return resolveCalls.incrementAndGet() == 1 ? database : null;
                    }
                };

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;

                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("no longer resolves"));
        new Verifications() {
            {
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testTargetCaptureFailureKeepsJobUnresolved() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;
                result = new DdlException("Lance catalog changed during index admission; retry the statement");
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        new Verifications() {
            {
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testAdmissionRecheckFailureKeepsJobUnresolved() throws Exception {
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                catalogMgr.withLanceIndexAdmission((LanceExternalCatalog) any, (CatalogMgr.LanceIndexTarget) any,
                        (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                minTimes = 0;
                result = new DdlException("Lance catalog target changed during index admission");
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("target changed"));
        new Verifications() {
            {
                // The refresh did run; only the durable transfer was refused.
                refreshManager.handleRefreshTable("lance_ctl", "db1", "tbl1", false);
                times = 1;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testRestCatalogDefensiveRejection() throws Exception {
        // Unreachable in practice (admission never targets a REST catalog), but the guard
        // must refuse before any read/refresh/release happens.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_OPERATION_NOT_SUPPORTED, e.getMysqlErrorCode());
        new Verifications() {
            {
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(anyString, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testStaleRevisionRereadForceReleasedIsIdempotent() throws Exception {
        // The revision CAS lost a race to a concurrent FORCE_RELEASE that already landed:
        // this retry is an idempotent success returning the existing release record.
        LanceIndexJob job = newUnknownJob(42L);
        LanceIndexJob reread = newUnknownJob(42L);
        reread.setRevision(4L);
        reread.setForceReleased(true);
        expectEnvBase();
        new Expectations() {
            {
                // Consecutive results: the initial load, then the reread after the
                // revision CAS reports a loss.
                lanceIndexJobManager.getJob(anyLong);
                minTimes = 0;
                result = job;
                result = reread;
            }
        };
        expectResolvableTarget(true);
        expectAdmissionTransfer(false);
        new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null);
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "note",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                queryState.setOk(0L, 1, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
    }

    @Test
    public void testStaleRevisionRereadStillUnresolvedIsNotUnknown() throws Exception {
        // The revision CAS lost and no concurrent release landed: UNKNOWN has no other
        // outgoing transition, so the pinned not-UNKNOWN wording stays accurate.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        expectAdmissionTransfer(false);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("cannot be resolved: not in UNKNOWN state"));
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "note",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testRedirectStatus() {
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC,
                new ResolveLanceIndexJobCommand(1L, "note").toRedirectStatus());
    }
}
