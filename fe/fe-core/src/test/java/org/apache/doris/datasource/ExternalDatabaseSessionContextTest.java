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
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;

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

    /**
     * A {@code session=user} plugin catalog whose remote database listing is per-token (each token sees only
     * {@code db_<suffix>}) and records the token it listed under. Pre-initialized so {@code getDbNames} skips the
     * Env-dependent metaCache build; the credentialed bypass path never touches that cache anyway.
     */
    private static final class SessionAwareCatalog extends PluginDrivenExternalCatalog {
        private final List<String> tokensUsedToListDatabases = new ArrayList<>();

        SessionAwareCatalog() {
            super(1L, "test_ctl", null, props(), "", userSessionConnector());
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

        private static Connector userSessionConnector() {
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getCapabilities())
                    .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_USER_SESSION));
            return connector;
        }

        private static Map<String, String> props() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            return props;
        }
    }
}
