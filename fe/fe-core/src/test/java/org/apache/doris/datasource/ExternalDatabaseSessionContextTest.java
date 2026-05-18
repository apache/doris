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
import java.util.List;

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

        private SessionAwareIcebergCatalog() {
            super(1L, "session_catalog", "");
            catalogProperty = new CatalogProperty(null, Collections.emptyMap());
        }

        @Override
        protected void initLocalObjectsImpl() {
            executionAuthenticator = new ExecutionAuthenticator() {
            };
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.singletonList("db1");
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            String token = ctx.getDelegatedCredential()
                    .map(DelegatedCredential::getToken)
                    .orElse("bootstrap");
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
    }
}
