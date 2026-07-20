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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorDelegatedCredential;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Verifies the {@code iceberg.rest.session=user} per-request routing that {@link IcebergConnector} wires into the
 * scan / write / procedure providers: each provider loads its table through a
 * {@code Function<ConnectorSession, IcebergCatalogOps>} resolver applied with the CURRENT operation's session, so a
 * session=user catalog resolves the querying user's per-request delegated catalog (fail-closed) instead of the
 * session-less shared {@code asCatalog(empty)} that the REST server would reject.
 *
 * <p>The connector passes {@code this::newCatalogBackedOps} as the resolver; these tests stand in a resolver that
 * mirrors its two observable behaviours — reject a credential-less session (the fail-closed
 * {@code IcebergSessionCatalogAdapter.delegatedCatalog}) and hand a credential-bearing session the per-user ops —
 * and assert (a) the provider applies the resolver with the EXACT call session, and (b) the fail-closed rejection
 * surfaces verbatim rather than wrapped. The legacy (session-less) constructors are covered by the existing
 * provider tests, which build with a constant {@code s -> catalogOps} and stay byte-identical.
 */
public class IcebergProviderSessionRoutingTest {

    private static final ConnectorDelegatedCredential CRED =
            new ConnectorDelegatedCredential(ConnectorDelegatedCredential.Type.ACCESS_TOKEN, "user-token");

    /**
     * A resolver mirroring {@code IcebergConnector.newCatalogBackedOps(session)} for a session=user catalog: a
     * session with no delegated credential is rejected fail-closed (never served a shared identity), a
     * credential-bearing session gets {@code perUserOps}. Records every session it is applied with so a test can
     * assert the provider threaded the call session through, not some stashed/default one.
     */
    private static Function<ConnectorSession, IcebergCatalogOps> failClosedResolver(
            List<ConnectorSession> seen, IcebergCatalogOps perUserOps) {
        return session -> {
            seen.add(session);
            if (!session.getDelegatedCredential().isPresent()) {
                throw new DorisConnectorException("Iceberg REST user session requires a delegated credential");
            }
            return perUserOps;
        };
    }

    @Test
    public void scanProviderAppliesResolverWithCallSessionAndFailsClosed() {
        List<ConnectorSession> seen = new ArrayList<>();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(),
                failClosedResolver(seen, new RecordingIcebergCatalogOps()), null, null, null);
        RoutingSession noCred = new RoutingSession(null);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.getScanNodeProperties(noCred, new IcebergTableHandle("db1", "t1"),
                        Collections.emptyList(), Optional.empty()));

        Assertions.assertSame(noCred, seen.get(0),
                "scan planning must resolve the ops with the exact call session (per-user routing)");
        Assertions.assertTrue(e.getMessage().contains("delegated credential"),
                "the fail-closed rejection surfaces verbatim, not wrapped in a generic scan error");
    }

    @Test
    public void scanProviderRoutesCredentialedSessionToPerUserOps() {
        List<ConnectorSession> seen = new ArrayList<>();
        RecordingIcebergCatalogOps perUserOps = new RecordingIcebergCatalogOps();
        // Record the loadTable call, then stop the method — the routing target is all this test asserts, not the
        // downstream split planning (which would need a live table).
        perUserOps.throwOnLoadTable = true;
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(Collections.emptyMap(),
                failClosedResolver(seen, perUserOps), null, null, null);
        RoutingSession cred = new RoutingSession(CRED);

        Assertions.assertThrows(RuntimeException.class,
                () -> provider.getScanNodeProperties(cred, new IcebergTableHandle("db1", "t1"),
                        Collections.emptyList(), Optional.empty()));

        Assertions.assertSame(cred, seen.get(0));
        Assertions.assertTrue(perUserOps.log.contains("loadTable:db1.t1"),
                "the credentialed session's per-user ops (not a shared identity) loaded the table");
    }

    @Test
    public void writeProviderAppliesResolverWithCallSessionAndFailsClosed() {
        List<ConnectorSession> seen = new ArrayList<>();
        IcebergWritePlanProvider provider = new IcebergWritePlanProvider(Collections.emptyMap(),
                failClosedResolver(seen, new RecordingIcebergCatalogOps()), null);
        RoutingSession noCred = new RoutingSession(null);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.getWriteSortColumns(noCred, new IcebergTableHandle("db1", "t1")));

        Assertions.assertSame(noCred, seen.get(0),
                "the write-side table read must resolve the ops with the exact call session (per-user routing)");
        Assertions.assertTrue(e.getMessage().contains("delegated credential"),
                "the fail-closed rejection surfaces verbatim");
    }

    @Test
    public void procedureProviderAppliesResolverWithCallSessionAndFailsClosed() {
        List<ConnectorSession> seen = new ArrayList<>();
        IcebergProcedureOps procOps = new IcebergProcedureOps(Collections.emptyMap(),
                failClosedResolver(seen, new RecordingIcebergCatalogOps()), null);
        RoutingSession noCred = new RoutingSession(null);

        // A valid, fully-argumented procedure: createAction + validate pass, so the flow reaches the auth scope
        // where the ops resolver is applied — and there the credential-less session is rejected fail-closed.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> procOps.execute(noCred, new IcebergTableHandle("db1", "t1"), "rollback_to_snapshot",
                        Collections.singletonMap("snapshot_id", "1"), null, Collections.emptyList()));

        Assertions.assertSame(noCred, seen.get(0),
                "ALTER TABLE ... EXECUTE must resolve the ops with the exact call session (per-user routing)");
        Assertions.assertTrue(e.getMessage().contains("delegated credential"),
                "the fail-closed rejection surfaces verbatim");
    }

    /** Minimal {@link ConnectorSession} double carrying an optional delegated credential. */
    private static final class RoutingSession implements ConnectorSession {
        private final ConnectorDelegatedCredential credential;

        RoutingSession(ConnectorDelegatedCredential credential) {
            this.credential = credential;
        }

        @Override
        public Optional<ConnectorDelegatedCredential> getDelegatedCredential() {
            return Optional.ofNullable(credential);
        }

        @Override
        public String getQueryId() {
            return "q1";
        }

        @Override
        public String getUser() {
            return "u1";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "ice";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
