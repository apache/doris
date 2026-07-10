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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorDelegatedCredential;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTransaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link ConnectorSessionImpl} and {@link ConnectorSessionBuilder}.
 */
public class ConnectorSessionImplTest {

    @Test
    public void testGetPropertyFromCatalogProperties() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("jdbc_url", "jdbc:mysql://host:3306/db");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .build();

        Assertions.assertEquals("jdbc:mysql://host:3306/db",
                session.getProperty("jdbc_url", String.class));
    }

    @Test
    public void testGetPropertyFromSessionProperties() {
        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("file_split_size", "134217728");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withSessionProperties(sessionProps)
                .build();

        Assertions.assertEquals("134217728",
                session.getProperty("file_split_size", String.class));
    }

    @Test
    public void testSessionPropertyOverridesCatalogProperty() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("timeout", "3000");

        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("timeout", "5000");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .withSessionProperties(sessionProps)
                .build();

        // Session properties should take precedence
        Assertions.assertEquals("5000",
                session.getProperty("timeout", String.class));
        Assertions.assertEquals(5000,
                session.getProperty("timeout", Integer.class));
    }

    @Test
    public void testGetPropertyReturnsNullForMissingKey() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertNull(session.getProperty("nonexistent", String.class));
    }

    @Test
    public void testGetPropertyTypeConversions() {
        Map<String, String> props = new HashMap<>();
        props.put("int_val", "42");
        props.put("long_val", "9999999999");
        props.put("bool_val", "true");
        props.put("str_val", "hello");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(props)
                .build();

        Assertions.assertEquals(42, session.getProperty("int_val", Integer.class));
        Assertions.assertEquals(42, session.getProperty("int_val", int.class));
        Assertions.assertEquals(9999999999L, session.getProperty("long_val", Long.class));
        Assertions.assertEquals(9999999999L, session.getProperty("long_val", long.class));
        Assertions.assertEquals(true, session.getProperty("bool_val", Boolean.class));
        Assertions.assertEquals(true, session.getProperty("bool_val", boolean.class));
        Assertions.assertEquals("hello", session.getProperty("str_val", String.class));
    }

    @Test
    public void testGetPropertyRejectsUnsupportedType() {
        Map<String, String> props = new HashMap<>();
        props.put("val", "1.5");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(props)
                .build();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> session.getProperty("val", Double.class));
    }

    @Test
    public void testGetPropertyNullNameThrows() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertThrows(NullPointerException.class,
                () -> session.getProperty(null, String.class));
    }

    @Test
    public void testGetPropertyNullTypeThrows() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertThrows(NullPointerException.class,
                () -> session.getProperty("key", null));
    }

    @Test
    public void testCatalogAndSessionPropertiesAreImmutable() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("k1", "v1");
        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("k2", "v2");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .withSessionProperties(sessionProps)
                .build();

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> session.getCatalogProperties().put("k3", "v3"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> session.getSessionProperties().put("k4", "v4"));
    }

    @Test
    public void testSessionMetadata() {
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withQueryId("query-123")
                .withUser("admin")
                .withTimeZone("Asia/Shanghai")
                .withCatalogId(42)
                .withCatalogName("my_catalog")
                .build();

        Assertions.assertEquals("query-123", session.getQueryId());
        Assertions.assertEquals("admin", session.getUser());
        Assertions.assertEquals("Asia/Shanghai", session.getTimeZone());
        Assertions.assertEquals(42, session.getCatalogId());
        Assertions.assertEquals("my_catalog", session.getCatalogName());
    }

    @Test
    public void testDefaultValues() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertEquals("", session.getQueryId());
        Assertions.assertEquals("", session.getUser());
        Assertions.assertEquals("UTC", session.getTimeZone());
        Assertions.assertEquals("en_US", session.getLocale());
        Assertions.assertEquals("", session.getCatalogName());
    }

    // ──────────────── transaction binding (P4-T06a W-a / gap G1) ────────────────

    // The session is otherwise immutable, but the insert executor binds a connector
    // transaction onto it at write time (setCurrentTransaction) so the connector's
    // planWrite can read it back (getCurrentTransaction). If this round-trip regresses,
    // the maxcompute write plan fails loud ("no transaction on session") at bind time.

    @Test
    public void testCurrentTransactionIsEmptyBeforeBinding() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();
        Assertions.assertEquals(Optional.empty(), session.getCurrentTransaction(),
                "a freshly built session must carry no transaction");
    }

    @Test
    public void testSetCurrentTransactionBindsThenReadsBackSameInstance() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();
        ConnectorTransaction txn = new StubConnectorTransaction(1234L);

        session.setCurrentTransaction(txn);

        Optional<ConnectorTransaction> bound = session.getCurrentTransaction();
        Assertions.assertTrue(bound.isPresent(), "transaction must be present after binding");
        Assertions.assertSame(txn, bound.get(),
                "getCurrentTransaction must return the exact instance the executor bound, "
                        + "because planWrite stamps that transaction's id into the sink");
    }

    @Test
    public void testSetCurrentTransactionNullUnbindsToEmpty() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();
        session.setCurrentTransaction(new StubConnectorTransaction(1L));

        session.setCurrentTransaction(null);

        Assertions.assertEquals(Optional.empty(), session.getCurrentTransaction(),
                "binding null must clear the transaction back to empty (Optional.ofNullable semantics)");
    }

    // ──────────── delegated-credential injection (SUPPORTS_USER_SESSION gate, #63068) ────────────

    @Test
    public void capableConnectorReceivesDelegatedCredentialAndSessionId() {
        // A SUPPORTS_USER_SESSION connector: the offered credential + stable session id are carried onto the
        // session so the connector can project the user's identity onto the remote metadata source.
        ConnectorDelegatedCredential cred =
                new ConnectorDelegatedCredential(ConnectorDelegatedCredential.Type.ACCESS_TOKEN, "user-token");
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withQueryId("q1")
                .withUserSessionCapability(true)
                .withSessionId("stable-session-1")
                .withDelegatedCredential(cred)
                .build();

        Assertions.assertTrue(session.getDelegatedCredential().isPresent(),
                "a capable connector receives the delegated credential");
        Assertions.assertSame(cred, session.getDelegatedCredential().get());
        Assertions.assertEquals("stable-session-1", session.getSessionId(),
                "the stable session id (AuthSession key) is carried, not the queryId");
    }

    @Test
    public void nonCapableConnectorSkipsDelegatedCredential() {
        // Least-privilege: without withUserSessionCapability(true) (the default), the offered credential is NOT
        // carried — a connector that would never consume the OIDC token never receives it. The session id then
        // falls back to the queryId.
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withQueryId("q1")
                .withSessionId("stable-session-1")
                .withDelegatedCredential(
                        new ConnectorDelegatedCredential(ConnectorDelegatedCredential.Type.ACCESS_TOKEN, "tok"))
                .build();

        Assertions.assertFalse(session.getDelegatedCredential().isPresent(),
                "a non-capable connector must never receive the delegated credential (least-privilege)");
        Assertions.assertEquals("q1", session.getSessionId(),
                "with no credential carried, the session id falls back to the queryId");
    }

    @Test
    public void capableConnectorWithNoOfferedCredentialCarriesNone() {
        // The capability gate alone does not manufacture a credential: a capable session with none offered still
        // exposes an empty credential (the connector then fails closed on the actual metadata read).
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withQueryId("q1")
                .withUserSessionCapability(true)
                .build();

        Assertions.assertFalse(session.getDelegatedCredential().isPresent());
    }

    /** Minimal hand-written {@link ConnectorTransaction}; only identity matters for this test. */
    private static final class StubConnectorTransaction implements ConnectorTransaction {
        private final long txnId;

        private StubConnectorTransaction(long txnId) {
            this.txnId = txnId;
        }

        @Override
        public long getTransactionId() {
            return txnId;
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }
    }
}
