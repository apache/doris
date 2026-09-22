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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobCompletionReason;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexJobRefreshState;
import org.apache.doris.datasource.lance.job.LanceIndexJobResult;
import org.apache.doris.datasource.lance.job.LanceIndexJobResultCode;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;
import org.apache.doris.datasource.lance.job.LanceIndexTerminationProof;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSet;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import mockit.VerificationsInOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Covers SHOW LANCE INDEX JOB jobId: the record is loaded first and authorized against its
 * persisted target before any field is returned; a missing job, a job whose target the
 * user cannot see (orphan or half-orphan without ADMIN, a resolution that fails outright,
 * a repointed or unresolvable dataset locator, or a resolvable target without table
 * SHOW), all share the same fixed ERR_LANCE_INDEX_JOB_NOT_FOUND response naming only the
 * job id. Also covers full detail-column rendering with and without result/dispatch/force
 * fields.
 */
public class ShowLanceIndexJobCommandTest {
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
    private SessionVariable sessionVariable;
    @Mocked
    private CatalogIf catalog;
    @Mocked
    private DatabaseIf database;
    @Mocked
    private TableIf table;
    @Mocked
    private LanceExternalCatalog lanceCatalog;
    @Mocked
    private LanceExternalDatabase lanceDatabase;
    @Mocked
    private LanceExternalTable lanceTable;

    private static LanceIndexJob newJob(long jobId) {
        LanceIndexJob job = new LanceIndexJob(jobId, "creator", 10L, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, "s3://bucket/dataset",
                "idx1", LanceIndexNameNormalizer.normalize("idx1"),
                LanceIndexJobMutationType.CREATE, true, false, "IVF_PQ", "v", null, 7L, null);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        job.setRefreshState(LanceIndexJobRefreshState.NOT_REQUIRED);
        job.setCreateTimeMs(1000L);
        job.setUpdateTimeMs(2000L);
        return job;
    }

    private void expectEnv(LanceIndexJob job) {
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

                lanceIndexJobManager.getJob(anyLong);
                minTimes = 0;
                result = job;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.getTimeZone();
                minTimes = 0;
                result = "Asia/Shanghai";
            }
        };
    }

    private void expectResolvableCatalog(boolean tableVisible) {
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = tableVisible;
            }
        };
    }

    private static int colIndex(ShowResultSet resultSet, String name) {
        for (int i = 0; i < resultSet.getMetaData().getColumns().size(); i++) {
            if (resultSet.getMetaData().getColumns().get(i).getName().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("column not found: " + name);
    }

    /**
     * Catalog id 10 resolves as a Lance catalog whose db1.tbl1 names resolve and whose
     * current dataset locator is the given value: "s3://bucket/dataset" is what
     * {@link #newJob} persists, anything else simulates a table repointed at a new
     * dataset, and null a locator resolution that fails outright.
     */
    private void expectLanceCatalog(String currentLocator, boolean tableVisible) {
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
                result = lanceDatabase;

                lanceDatabase.getTableNullable("tbl1");
                minTimes = 0;
                result = lanceTable;

                lanceCatalog.resolveCurrentIndexJobLocator("db1", "tbl1");
                minTimes = 0;
                result = currentLocator;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = tableVisible;
            }
        };
    }

    @Test
    public void testMissingJobAndUnauthorizedShareSameResponse() {
        // Missing job: 5103 naming only the job id.
        expectEnv(null);
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, missing.getMysqlErrorCode());
        Assertions.assertTrue(missing.getMessage().contains("Lance index job not found: 42"));

        // Resolvable target but no table SHOW privilege: byte-identical response.
        expectEnv(newJob(42L));
        expectResolvableCatalog(false);
        AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(missing.getMessage(), denied.getMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, denied.getMysqlErrorCode());
    }

    @Test
    public void testOrphanRequiresAdmin() {
        expectEnv(newJob(42L));
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
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, e.getMysqlErrorCode());
    }

    @Test
    public void testOrphanNonAdminMatchesMissingResponse() {
        expectEnv(null);
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertTrue(missing.getMessage().contains("Lance index job not found: 42"));

        // Catalog deleted: a non-ADMIN caller gets the byte-identical response as for a
        // missing job, so the persisted catalog id behind a job is never disclosed.
        expectEnv(newJob(42L));
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
        AnalysisException orphan = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(missing.getMessage(), orphan.getMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, orphan.getMysqlErrorCode());
        // The job record was loaded and the ADMIN check actually ran, so the orphan branch
        // (not the missing branch) produced the response above.
        new Verifications() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                times = 1;
            }
        };
    }

    @Test
    public void testHalfOrphanRequiresAdmin() {
        // Catalog resolves but the persisted db no longer does: same ADMIN-only rule.
        expectEnv(newJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, e.getMysqlErrorCode());
    }

    @Test
    public void testProviderFailureResolvingTargetMatchesMissingResponse() {
        expectEnv(null);
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertTrue(missing.getMessage().contains("Lance index job not found: 42"));

        // A valid job whose target resolution fails outright (provider error, expired
        // credentials) gets the same fixed response for a non-ADMIN caller; the provider
        // error never surfaces.
        expectEnv(newJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = new RuntimeException("provider exploded");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException failed = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(missing.getMessage(), failed.getMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, failed.getMysqlErrorCode());
    }

    @Test
    public void testLocatorMatchAuthorizedThroughTableShow() throws Exception {
        // The Lance catalog still points db1.tbl1 at the dataset the job was admitted
        // against, so ordinary table-level SHOW is enough to read the detail row.
        expectEnv(newJob(42L));
        expectLanceCatalog("s3://bucket/dataset", true);

        ShowResultSet resultSet = new ShowLanceIndexJobCommand(42L).doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("42", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testLocatorMismatchMatchesMissingResponse() {
        expectEnv(null);
        AnalysisException missing = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));

        // db1.tbl1 was repointed at a different dataset after the job terminated: even a
        // caller who would hold SHOW on the new target gets the fixed not-found response.
        expectEnv(newJob(42L));
        expectLanceCatalog("s3://bucket/repointed", true);
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(missing.getMessage(), denied.getMessage());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, denied.getMysqlErrorCode());
    }

    @Test
    public void testLocatorResolutionFailureIsOrphan() {
        // The current locator cannot be resolved right now (provider failure): the target
        // is treated as not resolved - orphan semantics - instead of granting SHOW
        // through the stale names.
        expectEnv(newJob(42L));
        expectLanceCatalog(null, true);
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_JOB_NOT_FOUND, e.getMysqlErrorCode());
    }

    @Test
    public void testOrphanVisibleToAdminWithEmptyCatalogName() throws Exception {
        LanceIndexJob job = newJob(42L);
        expectEnv(job);
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        ShowResultSet resultSet = new ShowLanceIndexJobCommand(42L).doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("42", row.get(colIndex(resultSet, "JobId")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "CatalogName")));
        Assertions.assertEquals("db1", row.get(colIndex(resultSet, "DbName")));
    }

    @Test
    public void testNullResultAndNullDispatchFieldsRenderEmpty() throws Exception {
        LanceIndexJob job = newJob(42L);
        Assertions.assertNull(job.getResult());
        expectEnv(job);
        expectResolvableCatalog(true);

        ShowResultSet resultSet = Assertions.assertDoesNotThrow(
                () -> new ShowLanceIndexJobCommand(42L).doRun(connectContext, null));
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("creator", row.get(colIndex(resultSet, "Creator")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "Message")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ResultCode")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "CompletionReason")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ExternalMetadataAdvanced")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "BackendId")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "BeProcessEpoch")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "InvocationId")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "DeadlineMs")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ForceNote")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ForceWarning")));
        Assertions.assertEquals("7", row.get(colIndex(resultSet, "AdmittedDatasetVersion")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "SchemaContractVersion")));
        Assertions.assertEquals("0", row.get(colIndex(resultSet, "Revision")));
        Assertions.assertEquals("YES", row.get(colIndex(resultSet, "IfNotExists")));
        Assertions.assertEquals("NO", row.get(colIndex(resultSet, "IfExists")));
        Assertions.assertEquals("IVF_PQ", row.get(colIndex(resultSet, "IndexType")));
        Assertions.assertEquals("v", row.get(colIndex(resultSet, "ColumnName")));
        Assertions.assertEquals("NONE", row.get(colIndex(resultSet, "TerminationProof")));
        Assertions.assertEquals(TimeUtils.longToTimeString(1000L), row.get(colIndex(resultSet, "CreateTime")));
    }

    @Test
    public void testResultDispatchAndForceFieldsRender() throws Exception {
        LanceIndexSchemaContract contract = new LanceIndexSchemaContract(Collections.singletonList(
                new LanceIndexSchemaContract.IndexedField(1, "v", "fixed_size_list", false, 128,
                        "float32", false)));
        LanceIndexJob job = new LanceIndexJob(42L, "creator", 10L, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, "s3://bucket/dataset",
                "idx1", LanceIndexNameNormalizer.normalize("idx1"),
                LanceIndexJobMutationType.CREATE, true, false, "IVF_PQ", "v", null, 7L, contract);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        job.setRefreshState(LanceIndexJobRefreshState.NOT_REQUIRED);
        job.setCreateTimeMs(1000L);
        job.setUpdateTimeMs(2000L);
        job.setResult(new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                LanceIndexJobCompletionReason.NONE, "ok", true));
        job.setBackendId(1001L);
        job.setBeProcessEpoch(55L);
        job.setInvocationId("inv-1");
        job.setDeadlineMs(9999L);
        job.setTerminationProof(LanceIndexTerminationProof.CHILD_REAPED);
        job.setRevision(3L);
        job.setForceReleased(true);
        job.setForceActor("admin");
        job.setForceTimeMs(3000L);
        job.setForceNote("note");
        job.setForceWarning("warn");
        expectEnv(job);
        expectResolvableCatalog(true);

        ShowResultSet resultSet = new ShowLanceIndexJobCommand(42L).doRun(connectContext, null);
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("NATIVE_OK", row.get(colIndex(resultSet, "ResultCode")));
        Assertions.assertEquals("NONE", row.get(colIndex(resultSet, "CompletionReason")));
        Assertions.assertEquals("YES", row.get(colIndex(resultSet, "ExternalMetadataAdvanced")));
        Assertions.assertEquals("ok", row.get(colIndex(resultSet, "Message")));
        Assertions.assertEquals("1001", row.get(colIndex(resultSet, "BackendId")));
        Assertions.assertEquals("55", row.get(colIndex(resultSet, "BeProcessEpoch")));
        Assertions.assertEquals("inv-1", row.get(colIndex(resultSet, "InvocationId")));
        Assertions.assertEquals("9999", row.get(colIndex(resultSet, "DeadlineMs")));
        Assertions.assertEquals("CHILD_REAPED", row.get(colIndex(resultSet, "TerminationProof")));
        Assertions.assertEquals("3", row.get(colIndex(resultSet, "Revision")));
        Assertions.assertEquals("YES", row.get(colIndex(resultSet, "ForceReleased")));
        Assertions.assertEquals("admin", row.get(colIndex(resultSet, "ForceActor")));
        Assertions.assertEquals(TimeUtils.longToTimeString(3000L), row.get(colIndex(resultSet, "ForceTime")));
        Assertions.assertEquals("note", row.get(colIndex(resultSet, "ForceNote")));
        Assertions.assertEquals("warn", row.get(colIndex(resultSet, "ForceWarning")));
        Assertions.assertEquals("1", row.get(colIndex(resultSet, "SchemaContractVersion")));
    }

    @Test
    public void testRedirectStatus() {
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowLanceIndexJobCommand(1L).toRedirectStatus());
    }

    @Test
    public void testProxyContextCarriesIdentity() throws Exception {
        // V3-F16.8: the privilege check runs against the context handed to doRun.
        ConnectContext proxyCtx = new ConnectContext();
        expectEnv(newJob(42L));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("tbl1");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv((ConnectContext) any, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = true;
            }
        };
        new ShowLanceIndexJobCommand(42L).doRun(proxyCtx, null);
        new Verifications() {
            {
                accessControllerManager.checkTblPriv(proxyCtx, "lance_ctl", "db1", "tbl1", PrivPredicate.SHOW);
                times = 1;
            }
        };
    }

    @Test
    public void testJobLoadsBeforeTargetAuthorization() throws Exception {
        expectEnv(newJob(42L));
        expectResolvableCatalog(true);

        new ShowLanceIndexJobCommand(42L).doRun(connectContext, null);

        // The record is loaded first; authorization then runs against its persisted target.
        new VerificationsInOrder() {
            {
                lanceIndexJobManager.getJob(42L);
                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl1",
                        PrivPredicate.SHOW);
            }
        };
    }
}
