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

import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.test.TestExternalDatabase;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Re-migrates #63068's {@code ExternalDatabaseSessionContextTest} onto the SPI architecture: the DATA-FLOW proof
 * (not just the bypass DECISION, which {@link PluginDrivenExternalCatalogSessionBypassTest} pins) that a
 * {@code iceberg.rest.session=user} catalog serves PER-USER metadata live and never through the shared
 * (catalog+name-keyed, NOT user-keyed) name cache — the cross-user leakage guard (Trino CVE-2026-34214).
 *
 * <p>The bypass reads {@link SessionContext#current()} for both the decision and the live listing, so the token is
 * driven through a {@code mockStatic}; the catalog overrides the remote listing to return each user's own
 * databases and to record the token it listed under. Because every read records a fresh token (even a repeat of
 * an earlier token), we prove no read was served from a shared cache; because the per-user results are disjoint,
 * we prove no user's database set leaks to another. #63068 asserted the same via a "bootstrap" shared read, which
 * on this branch fail-closes (a session=user catalog has no shared identity to bootstrap with) — so the live
 * per-read token record is the equivalent, architecture-correct observable.
 */
public class ExternalDatabaseSessionContextTest {

    private static SessionContext ctxFor(String token) {
        return SessionContext.of(token, new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN, token));
    }

    @Test
    public void delegatedSessionDatabaseNamesGoLivePerTokenAndNeverShareTheCache() {
        SessionAwareCatalog catalog = new SessionAwareCatalog();
        // Build the per-token contexts with the REAL SessionContext.of BEFORE mocking the static current()
        // (calling a mocked static inside when(...).thenReturn(...) would corrupt the stubbing).
        SessionContext ctxA = ctxFor("token_a");
        SessionContext ctxB = ctxFor("token_b");
        try (MockedStatic<SessionContext> sc = Mockito.mockStatic(SessionContext.class)) {
            sc.when(SessionContext::current).thenReturn(ctxA);
            List<String> aDbs = catalog.getDbNames();
            sc.when(SessionContext::current).thenReturn(ctxB);
            List<String> bDbs = catalog.getDbNames();
            // Repeat token_a: if any read were served from a shared cache this would NOT re-list live.
            sc.when(SessionContext::current).thenReturn(ctxA);
            List<String> aDbsAgain = catalog.getDbNames();

            // Per-user visibility: each token sees only its own database — no cross-user leakage.
            Assertions.assertTrue(aDbs.contains("db_a") && !aDbs.contains("db_b"),
                    "token_a must see only its own database");
            Assertions.assertTrue(bDbs.contains("db_b") && !bDbs.contains("db_a"),
                    "token_b must NOT see token_a's database (shared cache would have leaked it)");
            Assertions.assertEquals(aDbs, aDbsAgain, "the repeat read re-lists token_a's live view");
            // Every read listed live under its OWN token -> nothing was served from a shared cache.
            Assertions.assertEquals(Lists.newArrayList("token_a", "token_b", "token_a"),
                    catalog.tokensUsedToListDatabases,
                    "each getDbNames must go live with the current user's token, never hit a shared cache");
            // System databases stay visible under a per-user listing.
            Assertions.assertTrue(aDbs.contains(InfoSchemaDb.DATABASE_NAME) && aDbs.contains(MysqlDb.DATABASE_NAME),
                    "information_schema + mysql must remain visible under the per-user bypass");
        }
    }

    @Test
    public void delegatedSessionDatabaseLookupGoesLivePerTokenAndNeverSharesTheCache() {
        SessionAwareCatalog catalog = new SessionAwareCatalog();
        SessionContext ctxA = ctxFor("token_a");
        SessionContext ctxB = ctxFor("token_b");
        try (MockedStatic<SessionContext> sc = Mockito.mockStatic(SessionContext.class)) {
            sc.when(SessionContext::current).thenReturn(ctxA);
            ExternalDatabase<? extends ExternalTable> firstA = catalog.getDbNullable("db_a");
            sc.when(SessionContext::current).thenReturn(ctxB);
            ExternalDatabase<? extends ExternalTable> leakedA = catalog.getDbNullable("db_a");
            sc.when(SessionContext::current).thenReturn(ctxA);
            ExternalDatabase<? extends ExternalTable> secondA = catalog.getDbNullable("db_a");

            Assertions.assertNotNull(firstA);
            Assertions.assertEquals("db_a", firstA.getFullName());
            Assertions.assertEquals("db_a", firstA.getRemoteName());
            Assertions.assertNull(leakedA, "token_b must not resolve token_a's database");
            Assertions.assertNotNull(secondA);
            Assertions.assertNotSame(firstA, secondA, "the bypass must not reuse a shared database object");
            Assertions.assertEquals(Lists.newArrayList("token_a", "token_b", "token_a"),
                    catalog.tokensUsedToListDatabases);
            Assertions.assertFalse(catalog.hasSharedDatabaseCacheState(firstA.getId()),
                    "live database lookup must not publish names, object, or ID state");
        }
    }

    @Test
    public void delegatedSessionTableNamesAndLookupGoLivePerTokenAndNeverShareTheCache() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(0);
        SessionAwareDatabase db = new SessionAwareDatabase(catalog, 5L, "db1", "db1");
        SessionContext ctxA = ctxFor("token_a");
        SessionContext ctxB = ctxFor("token_b");
        try (MockedStatic<SessionContext> sc = Mockito.mockStatic(SessionContext.class)) {
            sc.when(SessionContext::current).thenReturn(ctxA);
            Set<String> tableNames = db.getTableNamesWithLock();
            PluginDrivenExternalTable firstA = db.getTableNullable("table_a");
            sc.when(SessionContext::current).thenReturn(ctxB);
            PluginDrivenExternalTable leakedA = db.getTableNullable("table_a");
            sc.when(SessionContext::current).thenReturn(ctxA);
            PluginDrivenExternalTable secondA = db.getTableNullable("table_a");

            Assertions.assertEquals(1, tableNames.size());
            Assertions.assertTrue(tableNames.contains("table_a"));
            Assertions.assertNotNull(firstA);
            Assertions.assertEquals("table_a", firstA.getName());
            Assertions.assertEquals("table_a", firstA.getRemoteName());
            Assertions.assertNull(leakedA, "token_b must not resolve token_a's table");
            Assertions.assertNotNull(secondA);
            Assertions.assertNotSame(firstA, secondA, "the bypass must not reuse a shared table object");
            Assertions.assertEquals(Lists.newArrayList("token_a", "token_a", "token_b", "token_a"),
                    catalog.tokensUsedToListTables);
            assertSharedTableCacheStaysCold(db, firstA);
        }

        SessionAwareCatalog mixedCatalog = new SessionAwareCatalog(2);
        SessionAwareDatabase mixedDb = new SessionAwareDatabase(mixedCatalog, 6L, "db1", "db1");
        withSession("token_mixed", () -> {
            PluginDrivenExternalTable table = mixedDb.getTableNullable("table_a");
            Assertions.assertNotNull(table);
            Assertions.assertEquals("Table_A", table.getName());
            Assertions.assertEquals("Table_A", table.getRemoteName());
            assertSharedTableCacheStaysCold(mixedDb, table);
        });
        Assertions.assertEquals(Lists.newArrayList("token_mixed"), mixedCatalog.tokensUsedToListTables);
    }

    @Test
    public void delegatedSessionModeZeroIsTableExistUsesPointLookup() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(0);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 2L, "db1", "db1");

        withSession("token_a", () -> Assertions.assertTrue(db.isTableExist("table_a")));

        Assertions.assertTrue(catalog.tokensUsedToListTables.isEmpty());
        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToCheckTableExist);
    }

    @Test
    public void delegatedSessionModeOneIsTableExistResolvesMixedCaseRemoteName() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(1);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 3L, "db1", "db1");

        withSession("token_mixed", () -> Assertions.assertTrue(db.isTableExist("table_a")));

        Assertions.assertEquals(Lists.newArrayList("token_mixed"), catalog.tokensUsedToListTables);
        Assertions.assertEquals(Lists.newArrayList("token_mixed"), catalog.tokensUsedToCheckTableExist);
    }

    @Test
    public void delegatedSessionModeTwoIsTableExistResolvesRemoteNameWithoutSharedCache() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(2);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 4L, "db1", "db1");

        withSession("token_a", () -> Assertions.assertTrue(db.isTableExist("TABLE_A")));

        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToListTables);
        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToCheckTableExist);
    }

    @Test
    public void delegatedSessionGetTablesListsRemotelyOnlyOnceForManyTables() {
        // #66025: bypass-mode getTables() used to list remotely 1 + N times for N tables — once in
        // getTableNamesWithLock() and once more per table inside findTableNamePairWithoutCache().
        SessionAwareCatalog catalog = new SessionAwareCatalog(0, Lists.newArrayList("t1", "t2", "t3"));
        SessionAwareDatabase db = new SessionAwareDatabase(catalog, 7L, "db1", "db1");
        withSession("token_a", () -> {
            List<PluginDrivenExternalTable> tables = db.getTables();
            Assertions.assertEquals(3, tables.size());
            List<String> names = tables.stream().map(PluginDrivenExternalTable::getName)
                    .collect(Collectors.toList());
            Assertions.assertTrue(names.containsAll(Lists.newArrayList("t1", "t2", "t3")));
            Assertions.assertTrue(tables.stream().allMatch(t -> t.getRemoteName().equals(t.getName())));
        });
        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToListTables,
                "getTables must enumerate remote table names exactly once, not 1 + N times");
    }

    @Test
    public void delegatedSessionGetTablesStillFailsOnCaseInsensitiveConflicts() {
        // The single-enumeration fast path must keep the case-insensitive conflict check: conflicting
        // remote names fail the whole listing instead of silently building an ambiguous table set.
        SessionAwareCatalog catalog = new SessionAwareCatalog(1, Lists.newArrayList("Table_A", "table_a"));
        SessionAwareDatabase db = new SessionAwareDatabase(catalog, 8L, "db1", "db1");
        withSession("token_a", () -> {
            RuntimeException e = Assertions.assertThrows(RuntimeException.class, db::getTables);
            Assertions.assertTrue(e.getMessage().contains(ExternalCatalog.FOUND_CONFLICTING),
                    "conflicting remote names must still surface the conflict error, got: " + e.getMessage());
        });
    }

    @Test
    public void delegatedSessionGetTablesKeepsFirstRemoteNameForMappedLocalName() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(
                0, Lists.newArrayList("remote_first", "remote_second")) {
            @Override
            public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
                return "local_name";
            }
        };
        SessionAwareDatabase db = new SessionAwareDatabase(catalog, 9L, "db1", "db1");

        withSession("token_a", () -> {
            List<PluginDrivenExternalTable> tables = db.getTables();
            Assertions.assertEquals(1, tables.size());
            Assertions.assertEquals("local_name", tables.get(0).getName());
            Assertions.assertEquals("remote_first", tables.get(0).getRemoteName());
        });
        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToListTables);
    }

    @Test
    public void delegatedSessionGetTablesSkipsIndividualBuildFailures() {
        SessionAwareCatalog catalog = new SessionAwareCatalog(
                0, Lists.newArrayList("good", "bad", "also_good"));
        FailingSessionAwareDatabase db = new FailingSessionAwareDatabase(catalog, 10L, "db1", "db1");

        withSession("token_a", () -> {
            List<String> names = db.getTables().stream().map(PluginDrivenExternalTable::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, names.size());
            Assertions.assertTrue(names.containsAll(Lists.newArrayList("good", "also_good")));
        });
        Assertions.assertEquals(Lists.newArrayList("token_a"), catalog.tokensUsedToListTables);
    }

    private static void withSession(String token, Runnable action) {
        SessionContext context = ctxFor(token);
        try (MockedStatic<SessionContext> sc = Mockito.mockStatic(SessionContext.class)) {
            sc.when(SessionContext::current).thenReturn(context);
            action.run();
        }
    }

    private static void assertSharedTableCacheStaysCold(
            SessionAwareDatabase db, PluginDrivenExternalTable table) {
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest(table.getName()));
        Assertions.assertNull(db.getCachedTableNameByIdForTest(table.getId()));
    }

    /**
     * A {@code session=user} plugin catalog whose remote database listing is per-token (each token sees only
     * {@code db_<suffix>}) and records the token it listed under. Pre-initialized so {@code getDbNames} skips the
     * Env-dependent metaCache build; the credentialed bypass path never touches that cache anyway.
     */
    private static class SessionAwareCatalog extends PluginDrivenExternalCatalog {
        private final List<String> tokensUsedToListDatabases = new ArrayList<>();
        private final List<String> tokensUsedToListTables = new ArrayList<>();
        private final List<String> tokensUsedToCheckTableExist = new ArrayList<>();
        // When non-null, the remote table listing returns this fixed list for any token.
        private final List<String> remoteTables;

        SessionAwareCatalog() {
            this(0);
        }

        SessionAwareCatalog(int lowerCaseTableNames) {
            this(lowerCaseTableNames, null);
        }

        SessionAwareCatalog(int lowerCaseTableNames, List<String> remoteTables) {
            super(1L, "test_ctl", null, props(lowerCaseTableNames), "", userSessionConnector());
            this.remoteTables = remoteTables;
            this.initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // no-op: the connector is injected via the constructor and the catalog is pre-initialized.
        }

        @Override
        protected List<String> listDatabaseNames() {
            String token = SessionContext.current().getDelegatedCredential().get().getToken();
            tokensUsedToListDatabases.add(token);
            // per-user: token_a -> [db_a], token_b -> [db_b]
            return Lists.newArrayList("db_" + token.substring("token_".length()));
        }

        @Override
        public String fromRemoteDatabaseName(String remoteDatabaseName) {
            // identity mapping (avoids routing through the mocked connector's metadata for the local name)
            return remoteDatabaseName;
        }

        @Override
        public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
            // identity mapping; these tests exercise lower_case_table_names rather than connector name mapping.
            return remoteTableName;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            String token = ctx.getDelegatedCredential().get().getToken();
            tokensUsedToListTables.add(token);
            if (remoteTables != null) {
                return remoteTables;
            }
            if ("token_mixed".equals(token)) {
                return Lists.newArrayList("Table_A");
            }
            return Lists.newArrayList("table_" + token.substring("token_".length()));
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tableName) {
            String token = ctx.getDelegatedCredential().get().getToken();
            tokensUsedToCheckTableExist.add(token);
            if ("token_mixed".equals(token)) {
                return "Table_A".equals(tableName);
            }
            return ("table_" + token.substring("token_".length())).equals(tableName);
        }

        private static Connector userSessionConnector() {
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getCapabilities())
                    .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_USER_SESSION));
            return connector;
        }

        private static Map<String, String> props(int lowerCaseTableNames) {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            props.put(ExternalCatalog.LOWER_CASE_TABLE_NAMES, String.valueOf(lowerCaseTableNames));
            return props;
        }

        boolean hasSharedDatabaseCacheState(long dbId) {
            return databaseNames != null || databases != null || dbIdNameIndex.getName(dbId) != null;
        }
    }

    private static final class SessionAwareDatabase extends PluginDrivenExternalDatabase {
        SessionAwareDatabase(ExternalCatalog catalog, long id, String name, String remoteName) {
            super(catalog, id, name, remoteName);
            initialized = true;
        }
    }

    private static final class FailingSessionAwareDatabase extends PluginDrivenExternalDatabase {
        FailingSessionAwareDatabase(ExternalCatalog catalog, long id, String name, String remoteName) {
            super(catalog, id, name, remoteName);
            initialized = true;
        }

        @Override
        protected PluginDrivenExternalTable buildTableInternal(String remoteTableName, String localTableName,
                long tblId, ExternalCatalog catalog, ExternalDatabase db) {
            if ("bad".equals(localTableName)) {
                throw new IllegalStateException("expected test build failure");
            }
            return super.buildTableInternal(remoteTableName, localTableName, tblId, catalog, db);
        }
    }
}
