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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexJobRefreshState;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Covers SHOW LANCE INDEX JOBS: row-level SHOW privilege filtering, orphan and
 * half-orphan rows restricted to global ADMIN with no existence or count leakage,
 * provider failures during target resolution treated as orphans, dataset-locator
 * revalidation (matching, repointed and unresolvable locators, resolved once per
 * target within one listing), FROM db-name normalization through the target catalog's
 * own naming rules, FROM/WHERE in-memory filtering, null-result rendering, FORCE audit
 * columns, the deliberately narrowed WHERE grammar (EqualTo + AND only),
 * FORWARD_NO_SYNC redirect, and that authorization runs against the connection context
 * handed to the command (the forwarded original user identity in proxy mode).
 */
public class ShowLanceIndexJobsCommandTest {
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

    private static LanceIndexJob newJob(long jobId, long catalogId, String dbName, String tableName,
            String indexName) {
        LanceIndexJob job = new LanceIndexJob(jobId, "creator", catalogId, dbName, tableName,
                LanceIndexFenceKey.PROVIDER_DIRECTORY, "s3://bucket/dataset",
                indexName, LanceIndexNameNormalizer.normalize(indexName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        job.setRefreshState(LanceIndexJobRefreshState.NOT_REQUIRED);
        job.setCreateTimeMs(1000L);
        job.setUpdateTimeMs(2000L);
        return job;
    }

    private void expectEnv(List<LanceIndexJob> jobs) {
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

                lanceIndexJobManager.getAllJobsSnapshot();
                minTimes = 0;
                result = jobs;

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

                database.getFullName();
                minTimes = 0;
                result = "db1";

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

    private static int colIndex(ShowResultSet resultSet, String name) {
        for (int i = 0; i < resultSet.getMetaData().getColumns().size(); i++) {
            if (resultSet.getMetaData().getColumns().get(i).getName().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("column not found: " + name);
    }

    @Test
    public void testMetadataColumns() throws Exception {
        expectEnv(Collections.emptyList());
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        List<String> expected = Arrays.asList("JobId", "CatalogName", "DbName", "TableName", "IndexName",
                "Operation", "State", "RefreshState", "PossibleLive", "CreateTime", "UpdateTime", "Message",
                "ForceReleased", "ForceActor", "ForceTime");
        Assertions.assertEquals(expected.size(), resultSet.getMetaData().getColumns().size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), resultSet.getMetaData().getColumns().get(i).getName());
        }
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());
    }

    @Test
    public void testRowLevelAuthFiltering() throws Exception {
        LanceIndexJob visible = newJob(1L, 10L, "db1", "tbl1", "idx1");
        LanceIndexJob hidden = newJob(2L, 10L, "db1", "tbl2", "idx2");
        expectEnv(Arrays.asList(visible, hidden));
        expectResolvableCatalog(true);
        new Expectations() {
            {
                database.getTableNullable("tbl2");
                minTimes = 0;
                result = table;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl2",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = false;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("1", row.get(colIndex(resultSet, "JobId")));
        Assertions.assertEquals("lance_ctl", row.get(colIndex(resultSet, "CatalogName")));
        Assertions.assertEquals("db1", row.get(colIndex(resultSet, "DbName")));
        Assertions.assertEquals("tbl1", row.get(colIndex(resultSet, "TableName")));
        Assertions.assertEquals("idx1", row.get(colIndex(resultSet, "IndexName")));
        Assertions.assertEquals("CREATE", row.get(colIndex(resultSet, "Operation")));
        Assertions.assertEquals("PENDING", row.get(colIndex(resultSet, "State")));
        Assertions.assertEquals("NOT_REQUIRED", row.get(colIndex(resultSet, "RefreshState")));
        Assertions.assertEquals("NO", row.get(colIndex(resultSet, "PossibleLive")));
        Assertions.assertEquals(TimeUtils.longToTimeString(1000L), row.get(colIndex(resultSet, "CreateTime")));
        Assertions.assertEquals(TimeUtils.longToTimeString(2000L), row.get(colIndex(resultSet, "UpdateTime")));
    }

    @Test
    public void testOrphanRowHiddenFromNonAdmin() throws Exception {
        LanceIndexJob orphan = newJob(3L, 999L, "db1", "tbl1", "idx1");
        expectEnv(Collections.singletonList(orphan));
        new Expectations() {
            {
                catalogMgr.getCatalog(999L);
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        // The orphan row contributes nothing: no placeholder, no count leak.
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());
    }

    @Test
    public void testOrphanRowVisibleToAdmin() throws Exception {
        LanceIndexJob orphan = newJob(3L, 999L, "gone_db", "gone_tbl", "idx1");
        expectEnv(Collections.singletonList(orphan));
        new Expectations() {
            {
                catalogMgr.getCatalog(999L);
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        List<String> row = resultSet.getResultRows().get(0);
        // The catalog name is not persisted on the job; an orphan row renders it empty.
        Assertions.assertEquals("", row.get(colIndex(resultSet, "CatalogName")));
        Assertions.assertEquals("gone_db", row.get(colIndex(resultSet, "DbName")));
        Assertions.assertEquals("gone_tbl", row.get(colIndex(resultSet, "TableName")));
    }

    @Test
    public void testHalfOrphanDbMissingIsAdminOnly() throws Exception {
        LanceIndexJob halfOrphan = newJob(4L, 10L, "gone_db", "tbl1", "idx1");
        expectEnv(Collections.singletonList(halfOrphan));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("gone_db");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        Assertions.assertTrue(command.doRun(connectContext, null).getResultRows().isEmpty());
    }

    @Test
    public void testHalfOrphanTableMissingIsAdminOnly() throws Exception {
        LanceIndexJob halfOrphan = newJob(4L, 10L, "db1", "gone_tbl", "idx1");
        expectEnv(Collections.singletonList(halfOrphan));
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

                database.getTableNullable("gone_tbl");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("lance_ctl",
                resultSet.getResultRows().get(0).get(colIndex(resultSet, "CatalogName")));
    }

    @Test
    public void testHalfOrphanTableMissingHiddenFromNonAdmin() throws Exception {
        // Pairs with testHalfOrphanTableMissingIsAdminOnly: the same unresolvable persisted
        // table is invisible to a non-ADMIN user - omitted entirely, no count leak.
        LanceIndexJob halfOrphan = newJob(4L, 10L, "db1", "gone_tbl", "idx1");
        expectEnv(Collections.singletonList(halfOrphan));
        new Expectations() {
            {
                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("db1");
                minTimes = 0;
                result = database;

                database.getTableNullable("gone_tbl");
                minTimes = 0;
                result = null;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        Assertions.assertTrue(command.doRun(connectContext, null).getResultRows().isEmpty());
    }

    @Test
    public void testProviderFailureResolvingTargetIsOrphan() throws Exception {
        // A provider failure while resolving a job's target (expired credentials, remote
        // fault) is indistinguishable from a missing target: no provider error surfaces,
        // the row is omitted for non-ADMIN and visible to ADMIN.
        expectEnv(Collections.singletonList(newJob(5L, 10L, "db1", "tbl1", "idx1")));
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
                result = new RuntimeException("provider exploded");

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        Assertions.assertTrue(Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, null))
                .getResultRows().isEmpty());
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("5", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testLocatorMatchVisibleThroughTableShow() throws Exception {
        // The Lance catalog still points db1.tbl1 at the dataset (s3://bucket/dataset) the
        // job was admitted against, so ordinary table-level SHOW is enough to see the row.
        expectEnv(Collections.singletonList(newJob(1L, 10L, "db1", "tbl1", "idx1")));
        expectLanceCatalog("s3://bucket/dataset", true);

        ShowResultSet resultSet = new ShowLanceIndexJobsCommand(null, null).doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("1", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testLocatorMismatchIsAdminOnly() throws Exception {
        // db1.tbl1 was repointed at a different dataset after the job terminated: a user
        // who would hold SHOW on the new target still sees nothing (no count leak), while
        // ADMIN keeps the row.
        expectEnv(Collections.singletonList(newJob(1L, 10L, "db1", "tbl1", "idx1")));
        expectLanceCatalog("s3://bucket/repointed", true);
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        Assertions.assertTrue(command.doRun(connectContext, null).getResultRows().isEmpty());
        Assertions.assertEquals(1, command.doRun(connectContext, null).getResultRows().size());
    }

    @Test
    public void testLocatorResolutionFailureIsAdminOnly() throws Exception {
        // The current locator cannot be resolved right now (provider failure): the target
        // is treated as not resolved - orphan semantics - instead of granting SHOW
        // through the stale names.
        expectEnv(Collections.singletonList(newJob(1L, 10L, "db1", "tbl1", "idx1")));
        expectLanceCatalog(null, true);
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        Assertions.assertTrue(command.doRun(connectContext, null).getResultRows().isEmpty());
        Assertions.assertEquals(1, command.doRun(connectContext, null).getResultRows().size());
    }

    @Test
    public void testLocatorResolvedOncePerTargetWithinListing() throws Exception {
        // Two jobs share one persisted target: one listing resolves its current locator
        // once (each resolution is a provider round trip), not once per job row.
        expectEnv(Arrays.asList(newJob(1L, 10L, "db1", "tbl1", "idx1"),
                newJob(2L, 10L, "db1", "tbl1", "idx2")));
        expectLanceCatalog("s3://bucket/dataset", true);

        ShowResultSet resultSet = new ShowLanceIndexJobsCommand(null, null).doRun(connectContext, null);
        Assertions.assertEquals(2, resultSet.getResultRows().size());

        new Verifications() {
            {
                lanceCatalog.resolveCurrentIndexJobLocator("db1", "tbl1");
                times = 1;
            }
        };
    }

    @Test
    public void testFromDbNameNormalizedThroughCatalog() throws Exception {
        // lower_case_database_names=1/2: jobs persist the resolved full name db1, so
        // FROM DB1 must still select those rows instead of missing them case-sensitively.
        LanceIndexJob match = newJob(1L, 10L, "db1", "tbl1", "idx1");
        LanceIndexJob otherDb = newJob(4L, 10L, "db2", "tbl1", "idx4");
        expectEnv(Arrays.asList(match, otherDb));
        expectResolvableCatalog(true);
        new Expectations() {
            {
                connectContext.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("DB1");
                minTimes = 0;
                result = database;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(Lists.newArrayList("DB1"), null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("1", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testFromDbNameResolutionFailureKeepsRawName() throws Exception {
        // The catalog cannot resolve the FROM name (provider failure): the statement must
        // not fail, and the filter falls back to the exact raw-string comparison, which
        // matches no persisted (already resolved) db name.
        expectEnv(Collections.singletonList(newJob(1L, 10L, "db1", "tbl1", "idx1")));
        new Expectations() {
            {
                connectContext.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getName();
                minTimes = 0;
                result = "lance_ctl";

                catalog.getDbNullable("DB1");
                minTimes = 0;
                result = new RuntimeException("provider exploded");

                catalogMgr.getCatalog(10L);
                minTimes = 0;
                result = catalog;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(Lists.newArrayList("DB1"), null);
        ShowResultSet resultSet = Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, null));
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());
    }

    @Test
    public void testFromAndWhereFilters() throws Exception {
        LanceIndexJob match = newJob(1L, 10L, "db1", "tbl1", "idx1");
        LanceIndexJob otherTable = newJob(2L, 10L, "db1", "tbl2", "idx2");
        LanceIndexJob otherState = newJob(3L, 10L, "db1", "tbl1", "idx3");
        otherState.setMutationState(LanceIndexJobMutationState.RUNNING);
        LanceIndexJob otherDb = newJob(4L, 10L, "db2", "tbl1", "idx4");
        expectEnv(Arrays.asList(match, otherTable, otherState, otherDb));
        expectResolvableCatalog(true);
        new Expectations() {
            {
                database.getTableNullable("tbl2");
                minTimes = 0;
                result = table;

                catalog.getDbNullable("db2");
                minTimes = 0;
                result = database;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db1", "tbl2",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, "lance_ctl", "db2", "tbl1",
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = true;
            }
        };

        Expression where = new And(
                new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")), new StringLiteral("tbl1")),
                new EqualTo(new UnboundSlot(Lists.newArrayList("State")), new StringLiteral("pending")));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(
                Lists.newArrayList("lance_ctl", "db1"), where);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("1", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testVarcharLiteralPredicateAccepted() throws Exception {
        // The parser hands the command a VarcharLiteral (not a StringLiteral) for
        // ordinary-length strings, so the real SQL WHERE shape must be accepted: no
        // WHERE hint, and both the TableName and State values filter correctly.
        LanceIndexJob match = newJob(1L, 10L, "db1", "tbl1", "idx1");
        LanceIndexJob otherTable = newJob(2L, 10L, "db1", "tbl2", "idx2");
        LanceIndexJob otherState = newJob(3L, 10L, "db1", "tbl1", "idx3");
        otherState.setMutationState(LanceIndexJobMutationState.RUNNING);
        expectEnv(Arrays.asList(match, otherTable, otherState));
        expectResolvableCatalog(true);

        Expression where = new And(
                new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")), new VarcharLiteral("tbl1", 4)),
                new EqualTo(new UnboundSlot(Lists.newArrayList("State")), new VarcharLiteral("PENDING", 7)));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        ShowResultSet resultSet = Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, null));
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("1", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testOneSegmentFromUsesCurrentCatalog(@Mocked CatalogIf otherCatalog) throws Exception {
        LanceIndexJob inCurrent = newJob(1L, 10L, "db1", "tbl1", "idx1");
        LanceIndexJob inOther = newJob(2L, 20L, "db1", "tbl1", "idx2");
        expectEnv(Arrays.asList(inCurrent, inOther));
        expectResolvableCatalog(true);
        new Expectations() {
            {
                connectContext.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalogMgr.getCatalog(20L);
                minTimes = 0;
                result = otherCatalog;

                otherCatalog.getName();
                minTimes = 0;
                result = "other_ctl";
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(Lists.newArrayList("db1"), null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("1", resultSet.getResultRows().get(0).get(colIndex(resultSet, "JobId")));
    }

    @Test
    public void testNullResultAndNoForceRenderEmptyColumns() throws Exception {
        LanceIndexJob job = newJob(1L, 10L, "db1", "tbl1", "idx1");
        Assertions.assertNull(job.getResult());
        expectEnv(Collections.singletonList(job));
        expectResolvableCatalog(true);

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, null));
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("", row.get(colIndex(resultSet, "Message")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ForceReleased")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ForceActor")));
        Assertions.assertEquals("", row.get(colIndex(resultSet, "ForceTime")));
    }

    @Test
    public void testForceReleasedRowRendersAuditColumns() throws Exception {
        LanceIndexJob job = newJob(1L, 10L, "db1", "tbl1", "idx1");
        job.setForceReleased(true);
        job.setForceActor("admin");
        job.setForceTimeMs(3000L);
        expectEnv(Collections.singletonList(job));
        expectResolvableCatalog(true);

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(connectContext, null);
        List<String> row = resultSet.getResultRows().get(0);
        Assertions.assertEquals("YES", row.get(colIndex(resultSet, "ForceReleased")));
        Assertions.assertEquals("admin", row.get(colIndex(resultSet, "ForceActor")));
        Assertions.assertEquals(TimeUtils.longToTimeString(3000L), row.get(colIndex(resultSet, "ForceTime")));
    }

    @Test
    public void testRedirectStatus() {
        Assertions.assertEquals(RedirectStatus.FORWARD_NO_SYNC,
                new ShowLanceIndexJobsCommand(null, null).toRedirectStatus());
    }

    @Test
    public void testProxyContextCarriesIdentity() throws Exception {
        // V3-F16.8: authorization must run against the context handed to doRun, which in
        // proxy (forwarded) mode carries the original user identity, never a system account.
        ConnectContext proxyCtx = new ConnectContext();
        LanceIndexJob job = newJob(1L, 10L, "db1", "tbl1", "idx1");
        expectEnv(Collections.singletonList(job));
        expectResolvableCatalog(true);
        new Expectations() {
            {
                accessControllerManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, null);
        ShowResultSet resultSet = command.doRun(proxyCtx, null);
        Assertions.assertEquals(1, resultSet.getResultRows().size());

        new Verifications() {
            {
                accessControllerManager.checkTblPriv(proxyCtx, "lance_ctl", "db1", "tbl1", PrivPredicate.SHOW);
                times = 1;
            }
        };
    }

    @Test
    public void testFromWithTooManyPartsRejected() {
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(
                Lists.newArrayList("ctl1", "db1", "tbl1"), null);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testOrPredicateRejected() {
        Expression where = new Or(
                new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")), new StringLiteral("tbl1")),
                new EqualTo(new UnboundSlot(Lists.newArrayList("State")), new StringLiteral("PENDING")));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> command.doRun(connectContext, null));
        Assertions.assertTrue(e.getMessage().contains("AND"));
    }

    @Test
    public void testNonEqualToPredicateRejected() {
        Expression where = new GreaterThan(new UnboundSlot(Lists.newArrayList("TableName")),
                new StringLiteral("tbl1"));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testLikeIsDeliberatelyRejected() {
        // T6: Like is a deliberate narrowing versus the ShowCopy precedent; it is rejected.
        Expression where = new Like(new UnboundSlot(Lists.newArrayList("TableName")),
                new StringLiteral("tbl%"));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testReversedLiteralPredicateRejected() {
        // WHERE "tbl" = TableName parses as EqualTo(StringLiteral, UnboundSlot); the narrowed
        // grammar requires the slot on the left, so the reversed shape gets the WHERE hint.
        Expression where = new EqualTo(new StringLiteral("tbl1"),
                new UnboundSlot(Lists.newArrayList("TableName")));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> command.doRun(connectContext, null));
        Assertions.assertTrue(e.getMessage().contains("Where clause should looks like"));
    }

    @Test
    public void testNonStringLiteralRightSideRejected() {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")),
                new IntegerLiteral(1));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testUnknownWhereKeyRejected() {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("Foo")), new StringLiteral("x"));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testDuplicateWhereColumnRejected() {
        Expression where = new And(
                new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")), new StringLiteral("a")),
                new EqualTo(new UnboundSlot(Lists.newArrayList("tablename")), new StringLiteral("b")));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(connectContext, null));
    }

    @Test
    public void testInvalidStateValueRejected() {
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("State")), new StringLiteral("BOGUS"));
        ShowLanceIndexJobsCommand command = new ShowLanceIndexJobsCommand(null, where);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> command.doRun(connectContext, null));
        Assertions.assertTrue(e.getMessage().contains("BOGUS"));
    }
}
