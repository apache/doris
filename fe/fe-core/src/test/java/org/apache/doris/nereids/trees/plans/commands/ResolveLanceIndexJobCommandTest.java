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
import org.apache.doris.datasource.lance.LanceIndexAdmissionSnapshot;
import org.apache.doris.datasource.lance.LanceIndexDatasetCheck;
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
 * the job id); the target verdict is three-valued — resolved names plus a matching durable
 * locator take table ALTER, a verifiably absent or repointed target takes the ADMIN orphan
 * branch, and a resolution that fails outright keeps the fence; only an UNKNOWN job may be
 * released; every failure before the durable transfer is the typed
 * ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE that leaves the job, its fence and its quota
 * untouched; success and idempotent retries return the OK packet with 0 affected rows,
 * 0 warning rows (SHOW WARNINGS is a stub answering every statement with an empty set,
 * so no warning count is ever advertised), and LATE_COMMIT_WARNING in the info field.
 */
public class ResolveLanceIndexJobCommandTest {
    /** The fixture job's persisted normalized locator; locator stubs must return exactly this. */
    private static final String JOB_LOCATOR = "s3://bucket/dataset";

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

    /** The authoritative read's answer for an unchanged target: the job's own dataset. */
    private static LanceIndexAdmissionSnapshot jobSnapshot() {
        return new LanceIndexAdmissionSnapshot(7L, JOB_LOCATOR, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
    }

    private static LanceIndexJob newUnknownJob(long jobId) {
        LanceIndexJob job = new LanceIndexJob(jobId, "creator", 10L, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, JOB_LOCATOR,
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

    private void expectResolvableTarget(boolean alterGranted) throws Exception {
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getId();
                minTimes = 0;
                result = 10L;

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

                // The namespace is case-sensitive and the job persists local names, so
                // the dataset check runs on the relations' remote names.
                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

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
    public void testMissingJobAndUnauthorizedShareSameResponse() throws Exception {
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
    public void testOrphanRequiresAdmin() throws Exception {
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
    public void testHalfOrphanRequiresAdmin() throws Exception {
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

                // A local null is not an orphan verdict: the namespace must positively
                // answer that the names are gone (queried by the local names here,
                // since no resolved relation carries a remote one).
                lanceCatalog.checkIndexJobDataset("db1", "tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.verifiedAbsent();

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
    public void testAuthResolutionFailureFailsClosed() throws Exception {
        // A resolution that errors out during authorization is never an orphan verdict. An
        // unauthorized caller still sees only the fixed 5103; an authorized one sees the
        // typed 5105 and nothing is released, read or refreshed.
        expectEnv(newUnknownJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = new RuntimeException("provider down");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, denied.getMysqlErrorCode());

        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("could not be resolved with current catalog metadata"));
        Assertions.assertFalse(e.getMessage().contains("provider down"));

        // The db resolves but the table lookup errors out: same fail-closed verdict.
        new Expectations() {
            {
                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = new RuntimeException("meta blip");
            }
        };
        AnalysisException tableBlip = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE,
                tableBlip.getMysqlErrorCode());
        new Verifications() {
            {
                // Authorization never claimed table-level ALTER, and no durable action ran.
                accessControllerManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                times = 0;
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testLocatorUnresolvableFailsClosed() throws Exception {
        // Names resolve locally but the current dataset locator cannot be resolved (provider
        // unreachable): absence of evidence is not an orphan verdict, so the fence is kept.
        expectEnv(newUnknownJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.unresolved();

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("could not be resolved with current catalog metadata"));
        new Verifications() {
            {
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testLocatorMismatchTakesHalfOrphanBranch() throws Exception {
        // The names now point at a different dataset than the job was admitted against:
        // a verifiable repoint, so the half-orphan rule applies — global ADMIN, no
        // authoritative read, best-effort refresh, then the durable release.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getId();
                minTimes = 0;
                result = 10L;

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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present("s3://bucket/reused-by-new-dataset");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, denied.getMysqlErrorCode());

        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        expectAdmissionTransfer(true);
        new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null);
        new Verifications() {
            {
                accessControllerManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                times = 0;
                lanceCatalog.loadTableIndexAdmissionSnapshot(anyString, anyString);
                times = 0;
                refreshManager.handleRefreshTable(10L, "db1", "tbl1", true);
                times = 1;
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "note",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

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
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testAlreadyForceReleasedSurvivesAResolutionFailure() throws Exception {
        // The release already landed, so a retry during a provider outage — the
        // target resolution errors out — is still the idempotent OK, never the
        // typed 5105: the idempotent short-circuit precedes the resolution-failure
        // rejection.
        LanceIndexJob job = newUnknownJob(42L);
        job.setForceReleased(true);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = new RuntimeException("provider down");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null);
        new Verifications() {
            {
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testNonUnknownJobWithFailedResolutionIsRejectedAsNotUnknown() throws Exception {
        // The state gate also precedes the resolution-failure rejection: for a
        // job that already left UNKNOWN the accurate answer is the 5104 state
        // rejection, not a 5105 claiming it still holds its fence.
        LanceIndexJob job = newUnknownJob(42L);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = new RuntimeException("provider down");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("cannot be resolved: not in UNKNOWN state"));
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
    public void testFullOrphanSuccessSkipsReadRefreshAndAdmission() throws Exception {
        // A fully orphaned job (catalog gone) is released straight in the manager write
        // lock, inside the orphan critical section that reconfirms absence under the
        // catalog lock: no target capture, no authoritative read, no refresh can even
        // be attempted.
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

                catalogMgr.withLanceIndexOrphanRelease(anyLong, (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                minTimes = 0;
                result = new Delegate<Object>() {
                    Object runOrphan(long catalogId, CatalogMgr.LanceIndexAdmissionAction<?> action)
                            throws Exception {
                        return action.run();
                    }
                };

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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
                times = 0;
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                times = 0;
                catalogMgr.withLanceIndexAdmission((LanceExternalCatalog) any, (CatalogMgr.LanceIndexTarget) any,
                        (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                times = 0;
                // Absence was still reconfirmed under the catalog lock.
                catalogMgr.withLanceIndexOrphanRelease(10L, (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                times = 1;
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
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

                lanceCatalog.getId();
                minTimes = 0;
                result = 10L;

                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                // A local null is not an orphan verdict: the namespace must positively
                // answer that the names are gone (queried by the local names here,
                // since no resolved relation carries a remote one).
                lanceCatalog.checkIndexJobDataset("db1", "tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.verifiedAbsent();

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
                refreshManager.handleRefreshTable(10L, "db1", "tbl1", true);
                times = 1;
                lanceIndexJobManager.forceRelease(42L, 3L, "operator", "note",
                        ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
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
                refreshManager.handleRefreshTable(10L, "db1", "tbl1", false);
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
                // The dataset check runs on the relations' remote names, not the job's
                // persisted local names (the namespace is case-sensitive).
                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                times = 1;
                lanceCatalog.checkIndexJobDataset("db1", "tbl1");
                times = 0;
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
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

                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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
                refreshManager.handleRefreshTable(10L, "db1", "tbl1", false);
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

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

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
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
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
                queryState.setOk(0L, 0, ResolveLanceIndexJobCommand.LATE_COMMIT_WARNING);
                times = 1;
            }
        };
    }

    @Test
    public void testStaleRevisionRereadStillUnknownIsRetryable() throws Exception {
        // The revision CAS lost to a transition that left the job UNKNOWN (a termination
        // proof bumps the revision without leaving UNKNOWN): telling the operator "not in
        // UNKNOWN state" would be wrong, so the retryable incomplete-resolution answer
        // comes back instead and the same statement can simply be retried.
        LanceIndexJob job = newUnknownJob(42L);
        LanceIndexJob reread = newUnknownJob(42L);
        reread.setRevision(4L);
        expectEnvBase();
        new Expectations() {
            {
                lanceIndexJobManager.getJob(anyLong);
                minTimes = 0;
                result = job;
                result = reread;
            }
        };
        expectResolvableTarget(true);
        expectAdmissionTransfer(false);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("changed concurrently while still UNKNOWN"));
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
    public void testStaleRevisionRereadLeftUnknownIsNotUnknown() throws Exception {
        // The revision CAS lost and the reread genuinely left UNKNOWN (a callback
        // converged it): only then is the pinned not-UNKNOWN wording accurate.
        LanceIndexJob job = newUnknownJob(42L);
        LanceIndexJob reread = newUnknownJob(42L);
        reread.setRevision(4L);
        reread.setMutationState(LanceIndexJobMutationState.NOT_COMMITTED);
        expectEnvBase();
        new Expectations() {
            {
                lanceIndexJobManager.getJob(anyLong);
                minTimes = 0;
                result = job;
                result = reread;
            }
        };
        expectResolvableTarget(true);
        expectAdmissionTransfer(false);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("cannot be resolved: not in UNKNOWN state"));
        new Verifications() {
            {
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testForceReleasedNonUnknownRecordIsNotIdempotentSuccess() throws Exception {
        // A malformed replayed record that claims forceReleased while its state is not
        // UNKNOWN still holds its fence and quota (the manager refuses to treat it as
        // released), so the command must not assure the operator of a release: the
        // shortcut only applies to a released UNKNOWN record.
        LanceIndexJob job = newUnknownJob(42L);
        job.setForceReleased(true);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        expectEnv(job);
        expectResolvableTarget(true);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_UNKNOWN, e.getMysqlErrorCode());
        new Verifications() {
            {
                queryState.setOk(anyLong, anyInt, anyString);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testLocalMissWithNamespacePresentFailsClosed() throws Exception {
        // The local layer came back cold but the namespace positively resolves the
        // names: not an orphan verdict and not table authorization either — the fence
        // is kept and the operator retries once the local cache warms.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                lanceCatalog.checkIndexJobDataset("db1", "tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        new Verifications() {
            {
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testLocalMissWithoutNamespaceProofFailsClosed() throws Exception {
        // Neither the local layer nor the namespace can answer: "table gone" cannot be
        // told apart from "network down", so the half-orphan branch stays unreachable.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                lanceCatalog.checkIndexJobDataset("db1", "tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.unresolved();

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testSnapshotDatasetMismatchKeepsTheFence() throws Exception {
        // The authoritative read resolves the namespace again on its own: a dataset
        // re-registered under the same names between the two reads must not release
        // this job's fence under table ALTER — the retry takes the orphan path.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = new LanceIndexAdmissionSnapshot(9L, "s3://bucket/reused-by-new-dataset",
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("repointed concurrently"));
        new Verifications() {
            {
                refreshManager.handleRefreshTable(anyLong, anyString, anyString, anyBoolean);
                times = 0;
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testRefreshRuntimeExceptionKeepsJobUnresolved() throws Exception {
        // An unchecked failure out of the metadata path is answered with the typed
        // resolution-incomplete response, never a raw provider error, and the job
        // stays unresolved for the retry.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        expectResolvableTarget(true);
        new Expectations() {
            {
                catalogMgr.captureLanceIndexTarget((LanceExternalCatalog) any);
                minTimes = 0;

                refreshManager.handleRefreshTable(10L, "db1", "tbl1", false);
                minTimes = 0;
                result = new RuntimeException("metadata enumeration exploded");
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("the metadata refresh failed"));
        Assertions.assertFalse(e.getMessage().contains("enumeration exploded"));
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testRenameAfterAuthorizationKeepsTheFence() throws Exception {
        // ALTER was granted on the name captured at authorization; a rename landing
        // before the transfer changes that name, so the release is refused as
        // retryable instead of proceeding under a name the caller never held ALTER on.
        LanceIndexJob job = newUnknownJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = lanceCatalog;

                lanceCatalog.getId();
                minTimes = 0;
                result = 10L;

                // Consecutive names: the authorization read, then the recheck inside
                // the admission critical section.
                lanceCatalog.getName();
                minTimes = 0;
                result = "lance_ctl";
                result = "lance_ctl_renamed";

                lanceCatalog.isRestCatalogConfigured();
                minTimes = 0;
                result = false;

                lanceCatalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                lanceCatalog.checkIndexJobDataset("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = LanceIndexDatasetCheck.present(JOB_LOCATOR);

                lanceCatalog.loadTableIndexAdmissionSnapshot("remote_db1", "remote_tbl1");
                minTimes = 0;
                result = jobSnapshot();

                database.getRemoteName();
                minTimes = 0;
                result = "remote_db1";

                table.getRemoteName();
                minTimes = 0;
                result = "remote_tbl1";

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
        expectAdmissionTransfer(true);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("renamed after authorization"));
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testFullOrphanRecheckSeesTheCatalogAgain() throws Exception {
        // The initial id lookup hit the rename window (the id is briefly removed and
        // re-added): the orphan critical section rechecks absence under the catalog
        // lock, sees the catalog again, and refuses the release as retryable instead
        // of releasing a job whose catalog and dataset survived the rename.
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

                catalogMgr.withLanceIndexOrphanRelease(anyLong, (CatalogMgr.LanceIndexAdmissionAction<?>) any);
                minTimes = 0;
                result = new Delegate<Object>() {
                    Object runOrphan(long catalogId, CatalogMgr.LanceIndexAdmissionAction<?> action)
                            throws Exception {
                        // The rename finished before the lock: the id resolves again.
                        throw new org.apache.doris.common.DdlException(
                                "Lance catalog id 10 resolves again (renamed concurrently); retry the statement");
                    }
                };
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ResolveLanceIndexJobCommand(42L, "note").run(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_RESOLUTION_INCOMPLETE, e.getMysqlErrorCode());
        Assertions.assertTrue(e.getMessage().contains("resolves again"));
        new Verifications() {
            {
                lanceIndexJobManager.forceRelease(anyLong, anyLong, anyString, anyString, anyString);
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
