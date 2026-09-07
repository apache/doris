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

package org.apache.doris.datasource;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.job.LanceIndexFenceKey;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationType;
import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.statistics.query.QueryStats;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Coverage for the Lance catalog DDL guard in {@link CatalogMgr}: while the catalog has
 * unresolved Lance index jobs, ALTER CATALOG ... SET PROPERTIES must reject actual value
 * changes of the five target identity keys (lance.catalog.type, warehouse,
 * lance.namespace.parent, lance.namespace.delimiter, lance.namespace.root_database) and
 * DROP CATALOG must be rejected; credential-only/unrelated property changes, same-value
 * rewrites, renames, and the replay path stay unguarded. The guard message must stay
 * neutral: no locator and no release-statement syntax.
 */
public class CatalogMgrLanceGuardTest {
    private static final long CATALOG_ID = 10L;
    private static final String CATALOG_NAME = "lance_catalog";
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final String[] IDENTITY_KEYS = {
        "lance.catalog.type", "warehouse",
        "lance.namespace.parent", "lance.namespace.delimiter", "lance.namespace.root_database"};

    private static final class Fixture {
        private final CatalogMgr catalogMgr = new CatalogMgr();
        private final LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        private final TestJobManager jobManager = new TestJobManager();
        private final Env env = Mockito.mock(Env.class);
        private final EditLog editLog = Mockito.mock(EditLog.class);
        private final ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        private final Map<String, String> oldProperties = new HashMap<>();

        Fixture(String lanceCatalogType) throws Exception {
            oldProperties.put("type", "lance");
            oldProperties.put("lance.catalog.type", lanceCatalogType);
            oldProperties.put("warehouse", "s3://bucket/warehouse");
            oldProperties.put("lance.namespace.parent", "s3://bucket/ns");
            oldProperties.put("lance.namespace.delimiter", ".");
            oldProperties.put("lance.namespace.root_database", "root_db");
            oldProperties.put("s3.access_key", "ak");
            oldProperties.put("s3.endpoint", "https://s3.example.com");

            AtomicReference<String> catalogName = new AtomicReference<>(CATALOG_NAME);
            Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
            Mockito.when(catalog.getType()).thenReturn("lance");
            Mockito.when(catalog.getName()).thenAnswer(invocation -> catalogName.get());
            Mockito.doAnswer(invocation -> {
                catalogName.set(invocation.getArgument(0));
                return null;
            }).when(catalog).modifyCatalogName(Mockito.anyString());
            Mockito.when(catalog.getProperties()).thenReturn(oldProperties);
            registerCatalog(catalogMgr, catalog);

            Mockito.when(env.getLanceIndexJobManager()).thenReturn(jobManager);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
            Mockito.when(env.getRefreshManager()).thenReturn(Mockito.mock(RefreshManager.class));
            Mockito.when(env.getQueryStats()).thenReturn(Mockito.mock(QueryStats.class));
            Mockito.when(metaCacheMgr.withCatalogLifecycleLock(Mockito.anyLong(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        Supplier<?> action = invocation.getArgument(1);
                        return action.get();
                    });
        }

        void admitUnresolvedJob() throws Exception {
            jobManager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        }

        MockedStatic<Env> mockCurrentEnv() {
            MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            return mockedEnv;
        }
    }

    @SuppressWarnings("unchecked")
    private static void registerCatalog(CatalogMgr catalogMgr, CatalogIf catalog) throws Exception {
        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        ((ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                idToCatalogField.get(catalogMgr)).put(catalog.getId(), catalog);
        Field nameToCatalogField = CatalogMgr.class.getDeclaredField("nameToCatalog");
        nameToCatalogField.setAccessible(true);
        ((Map<String, CatalogIf>) nameToCatalogField.get(catalogMgr)).put(catalog.getName(), catalog);
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static void assertNeutralGuardMessage(DdlException e) {
        String message = e.getMessage();
        Assertions.assertNotNull(message);
        Assertions.assertTrue(message.contains("unresolved Lance index jobs"), message);
        Assertions.assertFalse(message.contains("RESOLVE"), message);
        Assertions.assertFalse(message.contains(LOCATOR), message);
        Assertions.assertFalse(message.contains("s3://"), message);
    }

    @Test
    public void testEveryIdentityKeyValueChangeIsBlocked() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            for (String key : IDENTITY_KEYS) {
                Map<String, String> newProperties = ImmutableMap.of(key, "changed-value");
                DdlException e = Assertions.assertThrows(DdlException.class,
                        () -> fixture.catalogMgr.alterCatalogProps(CATALOG_NAME, newProperties), key);
                assertNeutralGuardMessage(e);
            }
            Mockito.verify(fixture.editLog, Mockito.never())
                    .logCatalogLog(Mockito.anyShort(), Mockito.any());
            Mockito.verify(fixture.catalog, Mockito.never()).modifyCatalogProps(Mockito.any());
        }
    }

    @Test
    public void testAddingAbsentIdentityKeyIsBlocked() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.oldProperties.remove("lance.namespace.root_database");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalogMgr.alterCatalogProps(CATALOG_NAME,
                            ImmutableMap.of("lance.namespace.root_database", "other_db")));
            assertNeutralGuardMessage(e);
        }
    }

    @Test
    public void testSameValueRewriteIsAllowed() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            // Re-setting every property (identity keys included) to its current value is
            // an idempotent no-op and must not be blocked.
            fixture.catalogMgr.alterCatalogProps(CATALOG_NAME, fixture.oldProperties);
            Mockito.verify(fixture.editLog, Mockito.times(1))
                    .logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        }
    }

    @Test
    public void testCaseVariantIdentityKeyChangeIsBlocked() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            // Catalog property keys are not normalized upstream; a case variant with a
            // changed value is still a target identity change.
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalogMgr.alterCatalogProps(CATALOG_NAME,
                            ImmutableMap.of("WAREHOUSE", "s3://other/warehouse")));
            assertNeutralGuardMessage(e);
            Mockito.verify(fixture.catalog, Mockito.never()).modifyCatalogProps(Mockito.any());
        }
    }

    @Test
    public void testCredentialAndUnrelatedKeyChangesAreAllowed() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        Map<String, String> newProperties = new HashMap<>();
        newProperties.put("s3.access_key", "rotated-ak");
        newProperties.put("s3.secret_key", "rotated-sk");
        newProperties.put("s3.endpoint", "https://s3.other-example.com");
        newProperties.put("custom.unrelated.property", "value");
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            fixture.catalogMgr.alterCatalogProps(CATALOG_NAME, newProperties);
            Mockito.verify(fixture.editLog, Mockito.times(1))
                    .logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        }
    }

    @Test
    public void testDropCatalogIsBlockedWithUnresolvedJob() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalogMgr.dropCatalog(CATALOG_NAME, false));
            assertNeutralGuardMessage(e);
            Assertions.assertSame(fixture.catalog, fixture.catalogMgr.getCatalog(CATALOG_NAME));
            Mockito.verify(fixture.editLog, Mockito.never())
                    .logCatalogLog(Mockito.anyShort(), Mockito.any());
        }
    }

    @Test
    public void testDropCatalogIsAllowedWithoutUnresolvedJobs() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            fixture.catalogMgr.dropCatalog(CATALOG_NAME, false);
            Assertions.assertNull(fixture.catalogMgr.getCatalog(CATALOG_NAME));
            Mockito.verify(fixture.editLog, Mockito.times(1))
                    .logCatalogLog(Mockito.eq(OperationType.OP_DROP_CATALOG), Mockito.any());
        }
    }

    @Test
    public void testRenameIsAllowedWithUnresolvedJob() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            // The fence identity is the persistent catalog id, so a rename is not target-changing.
            fixture.catalogMgr.alterCatalogName(CATALOG_NAME, "lance_catalog_renamed");
            Assertions.assertSame(fixture.catalog, fixture.catalogMgr.getCatalog("lance_catalog_renamed"));
            Assertions.assertNull(fixture.catalogMgr.getCatalog(CATALOG_NAME));
            Mockito.verify(fixture.editLog, Mockito.times(1))
                    .logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_NAME), Mockito.any());
        }
    }

    @Test
    public void testIdentityKeyChangeIsAllowedWithoutUnresolvedJobs() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            fixture.catalogMgr.alterCatalogProps(CATALOG_NAME,
                    ImmutableMap.of("warehouse", "s3://bucket/elsewhere"));
            Mockito.verify(fixture.editLog, Mockito.times(1))
                    .logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        }
    }

    @Test
    public void testRestCatalogIsNotAffected() throws Exception {
        // Admission rejects REST catalogs, so a REST catalog can never hold a job and the
        // guard is a natural no-op for it.
        Fixture fixture = new Fixture("rest");
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            fixture.catalogMgr.alterCatalogProps(CATALOG_NAME,
                    ImmutableMap.of("warehouse", "s3://bucket/elsewhere"));
            fixture.catalogMgr.dropCatalog(CATALOG_NAME, false);
            Assertions.assertNull(fixture.catalogMgr.getCatalog(CATALOG_NAME));
        }
    }

    @Test
    public void testReplayAlterCatalogPropsIsNotGuarded() throws Exception {
        Fixture fixture = new Fixture("filesystem");
        fixture.admitUnresolvedJob();
        CatalogLog log = new CatalogLog();
        log.setCatalogId(CATALOG_ID);
        log.setNewProps(ImmutableMap.of("warehouse", "s3://bucket/elsewhere"));
        try (MockedStatic<Env> mockedEnv = fixture.mockCurrentEnv()) {
            // Replay must apply verbatim: the guard lives only on the master DDL path.
            fixture.catalogMgr.replayAlterCatalogProps(log, fixture.oldProperties, true);
            Mockito.verify(fixture.catalog).modifyCatalogProps(log.getNewProps());
        }
    }

    private static class TestJobManager extends LanceIndexJobManager {
        @Override
        protected void writeEditLog(LanceIndexJob job) {
            // No journal in a pure DDL-guard unit test.
        }
    }
}
