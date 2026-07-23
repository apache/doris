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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergRestExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalDatabaseSessionContextTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDelegatedSessionTableNamesBypassSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog();
        IcebergExternalDatabase db = new IcebergExternalDatabase(catalog, 2L, "db1", "db1");

        withDelegatedToken("token_a", () -> Assertions.assertEquals(
                Collections.singleton("table_a"), db.getTableNamesWithLock()));
        withDelegatedToken("token_b", () -> Assertions.assertEquals(
                Collections.singleton("table_b"), db.getTableNamesWithLock()));
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b"), catalog.tokensUsedToListTables);
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));
    }

    @Test
    public void testDelegatedSessionTableLookupBypassesSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog();
        IcebergExternalDatabase db = new IcebergExternalDatabase(catalog, 3L, "db1", "db1");

        // User-session table lookup should resolve names from the remote token-scoped view instead of shared caches.
        catalog.clearTableTokenTrace();
        withDelegatedToken("token_a", () -> {
            IcebergExternalTable table = db.getTableNullable("table_a");
            Assertions.assertNotNull(table);
            Assertions.assertEquals("table_a", table.getName());
            Assertions.assertEquals("table_a", table.getRemoteName());
        });
        assertOnlyTableTokens(catalog, "token_a");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_b", () -> Assertions.assertNull(db.getTableNullable("table_a")));
        assertOnlyTableTokens(catalog, "token_b");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_b", () -> {
            IcebergExternalTable table = db.getTableNullable("table_b");
            Assertions.assertNotNull(table);
            Assertions.assertEquals("table_b", table.getName());
            Assertions.assertEquals("table_b", table.getRemoteName());
        });
        assertOnlyTableTokens(catalog, "token_b");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));
    }

    @Test
    public void testDelegatedSessionIsTableExistBypassesSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog();
        IcebergExternalDatabase db = new IcebergExternalDatabase(catalog, 4L, "db1", "db1");

        // Mode 0 requires no name conversion, so isTableExist() should use a point lookup without listing tables.
        catalog.clearTableTokenTrace();
        withDelegatedToken("token_a", () -> Assertions.assertTrue(db.isTableExist("table_a")));
        assertOnlyTableExistTokens(catalog, "token_a");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_b", () -> Assertions.assertFalse(db.isTableExist("table_a")));
        assertOnlyTableExistTokens(catalog, "token_b");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_b", () -> Assertions.assertTrue(db.isTableExist("table_b")));
        assertOnlyTableExistTokens(catalog, "token_b");
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
        Assertions.assertNull(db.getCachedTableForTest("table_b"));
    }

    @Test
    public void testDelegatedSessionIsTableExistResolvesModeTwoNameWithoutSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog(
                Collections.singletonMap("lower_case_table_names", "2"));
        IcebergExternalDatabase db = new IcebergExternalDatabase(catalog, 5L, "db1", "db1");

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_a", () -> Assertions.assertTrue(db.isTableExist("TABLE_A")));
        assertOnlyTableTokens(catalog, "token_a");
        Assertions.assertEquals(Collections.singletonList("token_a"), catalog.tokensUsedToCheckTableExist);
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
    }

    @Test
    public void testDelegatedSessionIsTableExistResolvesModeOneRemoteNameWithoutSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog(
                Collections.singletonMap("lower_case_table_names", "1"));
        IcebergExternalDatabase db = new IcebergExternalDatabase(catalog, 6L, "db1", "db1");

        catalog.clearTableTokenTrace();
        withDelegatedToken("token_mixed", () -> Assertions.assertTrue(db.isTableExist("table_a")));
        assertOnlyTableTokens(catalog, "token_mixed");
        Assertions.assertEquals(Collections.singletonList("token_mixed"), catalog.tokensUsedToCheckTableExist);
        Assertions.assertNull(db.getCachedTableNamesForTest());
        Assertions.assertNull(db.getCachedTableForTest("table_a"));
    }

    @Test
    public void testDelegatedSessionDatabaseLookupBypassesSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog();

        withDelegatedToken("token_a", () -> Assertions.assertNotNull(catalog.getDbNullable("db_a")));
        withDelegatedToken("token_b", () -> Assertions.assertNull(catalog.getDbNullable("db_a")));
        withDelegatedToken("token_b", () -> Assertions.assertNotNull(catalog.getDbNullable("db_b")));
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b", "token_b"),
                catalog.tokensUsedToListDatabases);
    }

    @Test
    public void testDelegatedSessionDatabaseNamesDoNotPopulateSharedCache() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog();

        withDelegatedToken("token_a", () -> Assertions.assertEquals(
                Lists.newArrayList("db_a", InfoSchemaDb.DATABASE_NAME, MysqlDb.DATABASE_NAME), catalog.getDbNames()));
        withDelegatedToken("token_b", () -> Assertions.assertEquals(
                Lists.newArrayList("db_b", InfoSchemaDb.DATABASE_NAME, MysqlDb.DATABASE_NAME), catalog.getDbNames()));
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b"), catalog.tokensUsedToListDatabases);

        withDelegatedToken("token_a", () -> {
            List<String> sharedDatabaseNames = catalog.getSharedDatabaseNames();
            Assertions.assertTrue(sharedDatabaseNames.contains("db1"));
            Assertions.assertFalse(sharedDatabaseNames.contains("db_a"));
        });
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b", "bootstrap"),
                catalog.tokensUsedToListDatabases);
    }

    @Test
    public void testDelegatedSessionDatabaseLookupUsesLocalNameMapping() {
        SessionAwareIcebergCatalog catalog = new SessionAwareIcebergCatalog(
                Collections.singletonMap("lower_case_database_names", "1"));

        withDelegatedToken("token_upper", () -> {
            Assertions.assertEquals(Lists.newArrayList("salesdb", InfoSchemaDb.DATABASE_NAME, MysqlDb.DATABASE_NAME),
                    catalog.getDbNames());
            ExternalDatabase<?> db = catalog.getDbNullable("salesdb");
            Assertions.assertNotNull(db);
            Assertions.assertEquals("salesdb", db.getFullName());
            Assertions.assertEquals("SalesDB", db.getRemoteName());
        });
        Assertions.assertEquals(Lists.newArrayList("token_upper", "token_upper"),
                catalog.tokensUsedToListDatabases);
    }

    private static void withDelegatedToken(String token, Runnable action) {
        ConnectContext context = new ConnectContext();
        context.setSessionContext(SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ACCESS_TOKEN, token)));
        context.setThreadLocalInfo();
        try {
            action.run();
        } finally {
            ConnectContext.remove();
        }
    }

    private static void assertOnlyTableTokens(SessionAwareIcebergCatalog catalog, String expectedToken) {
        Assertions.assertFalse(catalog.tokensUsedToListTables.isEmpty());
        Assertions.assertTrue(catalog.tokensUsedToListTables.stream().allMatch(expectedToken::equals));
    }

    private static void assertOnlyTableExistTokens(SessionAwareIcebergCatalog catalog, String expectedToken) {
        Assertions.assertTrue(catalog.tokensUsedToListTables.isEmpty());
        Assertions.assertEquals(Collections.singletonList(expectedToken), catalog.tokensUsedToCheckTableExist);
    }

    private static class SessionAwareIcebergCatalog extends IcebergRestExternalCatalog {
        private final List<String> tokensUsedToListTables = Lists.newArrayList();
        private final List<String> tokensUsedToCheckTableExist = Lists.newArrayList();
        private final List<String> tokensUsedToListDatabases = Lists.newArrayList();

        private SessionAwareIcebergCatalog() {
            this(Collections.emptyMap());
        }

        private SessionAwareIcebergCatalog(Map<String, String> overrideProps) {
            super(1L, "session_catalog", null, catalogProperties(overrideProps), "");
        }

        private static Map<String, String> catalogProperties(Map<String, String> overrideProps) {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            props.put("iceberg.catalog.type", "rest");
            props.put("iceberg.rest.uri", "http://localhost:8181");
            props.put("iceberg.rest.security.type", "oauth2");
            props.put("iceberg.rest.session", "user");
            props.put("iceberg.rest.oauth2.credential", "client_credentials");
            props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
            props.putAll(overrideProps);
            return props;
        }

        @Override
        protected void initLocalObjectsImpl() {
            executionAuthenticator = new ExecutionAuthenticator() {
            };
        }

        @Override
        protected List<String> listDatabaseNames() {
            tokensUsedToListDatabases.add("bootstrap");
            return databaseNamesForToken("bootstrap");
        }

        @Override
        protected List<String> listDatabaseNames(SessionContext ctx) {
            String token = token(ctx);
            tokensUsedToListDatabases.add(token);
            return databaseNamesForToken(token);
        }

        private List<String> databaseNamesForToken(String token) {
            if ("token_a".equals(token)) {
                return Lists.newArrayList("db_a");
            }
            if ("token_b".equals(token)) {
                return Lists.newArrayList("db_b");
            }
            if ("token_upper".equals(token)) {
                return Lists.newArrayList("SalesDB");
            }
            return Lists.newArrayList("db1");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            String token = token(ctx);
            tokensUsedToListTables.add(token);
            if ("token_a".equals(token)) {
                return Lists.newArrayList("table_a");
            }
            if ("token_b".equals(token)) {
                return Lists.newArrayList("table_b");
            }
            if ("token_mixed".equals(token)) {
                return Lists.newArrayList("Table_A");
            }
            return Lists.newArrayList("bootstrap_table");
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            String token = token(ctx);
            tokensUsedToCheckTableExist.add(token);
            if ("token_a".equals(token)) {
                return "table_a".equals(tblName);
            }
            if ("token_b".equals(token)) {
                return "table_b".equals(tblName);
            }
            if ("token_mixed".equals(token)) {
                return "Table_A".equals(tblName);
            }
            return "bootstrap_table".equals(tblName);
        }

        @Override
        public boolean isIcebergRestUserSessionEnabled() {
            return true;
        }

        private void clearTableTokenTrace() {
            tokensUsedToListTables.clear();
            tokensUsedToCheckTableExist.clear();
        }

        private List<String> getSharedDatabaseNames() {
            makeSureInitialized();
            return databaseNames.get("").localNames();
        }

        private static String token(SessionContext ctx) {
            return ctx.getDelegatedCredential()
                    .map(DelegatedCredential::getToken)
                    .orElse("bootstrap");
        }
    }
}
