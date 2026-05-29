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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
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
                Lists.newArrayList("db_a"), catalog.getDbNames()));
        withDelegatedToken("token_b", () -> Assertions.assertEquals(
                Lists.newArrayList("db_b"), catalog.getDbNames()));
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b"), catalog.tokensUsedToListDatabases);

        withDelegatedToken("token_a", () -> {
            List<String> sharedDatabaseNames = catalog.getSharedDatabaseNames();
            Assertions.assertTrue(sharedDatabaseNames.contains("db1"));
            Assertions.assertFalse(sharedDatabaseNames.contains("db_a"));
        });
        Assertions.assertEquals(Lists.newArrayList("token_a", "token_b", "bootstrap"),
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

    private static class SessionAwareIcebergCatalog extends IcebergExternalCatalog {
        private final List<String> tokensUsedToListTables = Lists.newArrayList();
        private final List<String> tokensUsedToListDatabases = Lists.newArrayList();

        private SessionAwareIcebergCatalog() {
            super(1L, "session_catalog", "");
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            props.put("iceberg.catalog.type", "rest");
            props.put("iceberg.rest.uri", "http://localhost:8181");
            props.put("iceberg.rest.security.type", "oauth2");
            props.put("iceberg.rest.session", "user");
            props.put("iceberg.rest.oauth2.credential", "client_credentials");
            props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");
            catalogProperty = new CatalogProperty(null, props);
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

        @Override
        protected boolean databaseExists(SessionContext ctx, String dbName) {
            String token = token(ctx);
            tokensUsedToListDatabases.add(token);
            return databaseNamesForToken(token).contains(dbName);
        }

        private List<String> databaseNamesForToken(String token) {
            if ("token_a".equals(token)) {
                return Lists.newArrayList("db_a");
            }
            if ("token_b".equals(token)) {
                return Lists.newArrayList("db_b");
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
            return Lists.newArrayList("bootstrap_table");
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return listTableNamesFromRemote(ctx, dbName).contains(tblName);
        }

        @Override
        public boolean isIcebergRestUserSessionEnabled() {
            return true;
        }

        private List<String> getSharedDatabaseNames() {
            makeSureInitialized();
            return metaCache.listNames();
        }

        private static String token(SessionContext ctx) {
            return ctx.getDelegatedCredential()
                    .map(DelegatedCredential::getToken)
                    .orElse("bootstrap");
        }
    }
}
