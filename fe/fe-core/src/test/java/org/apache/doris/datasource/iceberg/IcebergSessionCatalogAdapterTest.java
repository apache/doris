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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties.DelegatedTokenMode;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.BaseSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergSessionCatalogAdapterTest {

    @Test
    public void testAccessTokenMapsToIcebergOAuthBearerTokenCredential() {
        SessionContext context = SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ACCESS_TOKEN, "access-token"));
        org.apache.iceberg.catalog.SessionCatalog.SessionContext icebergContext =
                IcebergSessionCatalogAdapter.toIcebergSessionContext(context);
        org.apache.iceberg.catalog.SessionCatalog.SessionContext secondIcebergContext =
                IcebergSessionCatalogAdapter.toIcebergSessionContext(context);

        Assertions.assertEquals(context.getSessionId(), icebergContext.sessionId());
        Assertions.assertEquals(icebergContext.sessionId(), secondIcebergContext.sessionId());
        Assertions.assertEquals("access-token", icebergContext.credentials().get(OAuth2Properties.TOKEN));
        Assertions.assertEquals(1, icebergContext.credentials().size());
    }

    @Test
    public void testIdTokenUsesBearerTokenCredentialByDefault() {
        SessionContext context = SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ID_TOKEN, "oidc-login-token"));

        org.apache.iceberg.catalog.SessionCatalog.SessionContext icebergContext =
                IcebergSessionCatalogAdapter.toIcebergSessionContext(context);

        Assertions.assertEquals("oidc-login-token", icebergContext.credentials().get(OAuth2Properties.TOKEN));
        Assertions.assertEquals(1, icebergContext.credentials().size());
    }

    @Test
    public void testIdTokenUsesTokenExchangeCredentialWhenConfigured() {
        SessionContext context = SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ID_TOKEN, "id-token"));

        org.apache.iceberg.catalog.SessionCatalog.SessionContext icebergContext =
                IcebergSessionCatalogAdapter.toIcebergSessionContext(context, DelegatedTokenMode.TOKEN_EXCHANGE);

        Assertions.assertEquals("id-token", icebergContext.credentials().get(OAuth2Properties.ID_TOKEN_TYPE));
        Assertions.assertEquals(1, icebergContext.credentials().size());
    }

    @Test
    public void testDelegatedCatalogUsesIcebergSessionCredentials() {
        RecordingSessionCatalog sessionCatalog = new RecordingSessionCatalog();
        SessionBackedCatalog catalog = new SessionBackedCatalog(sessionCatalog);
        IcebergSessionCatalogAdapter adapter = new IcebergSessionCatalogAdapter(catalog);
        SessionContext context = SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ACCESS_TOKEN, "access-token"));

        adapter.catalog(context).tableExists(TableIdentifier.of("db", "tbl"));

        Map<String, String> credentials = sessionCatalog.lastContext.credentials();
        Assertions.assertEquals("access-token", credentials.get(OAuth2Properties.TOKEN));
        Assertions.assertFalse(catalog.tableExistsCalled);
    }

    @Test
    public void testDelegatedNamespacesUseIcebergSessionCredentials() {
        RecordingSessionCatalog sessionCatalog = new RecordingSessionCatalog();
        SessionBackedCatalog catalog = new SessionBackedCatalog(sessionCatalog);
        IcebergSessionCatalogAdapter adapter = new IcebergSessionCatalogAdapter(catalog);
        SessionContext context = SessionContext.of(new DelegatedCredential(
                DelegatedCredential.Type.ACCESS_TOKEN, "access-token"));

        adapter.namespaces(context).listNamespaces(Namespace.empty());

        Map<String, String> credentials = sessionCatalog.lastContext.credentials();
        Assertions.assertEquals("access-token", credentials.get(OAuth2Properties.TOKEN));
        Assertions.assertFalse(catalog.listNamespacesCalled);
    }

    @Test
    public void testPlainCatalogIsUsedWithoutDelegatedCredential() {
        RecordingSessionCatalog sessionCatalog = new RecordingSessionCatalog();
        SessionBackedCatalog catalog = new SessionBackedCatalog(sessionCatalog);
        IcebergSessionCatalogAdapter adapter = new IcebergSessionCatalogAdapter(catalog);

        adapter.catalog(SessionContext.empty()).tableExists(TableIdentifier.of("db", "tbl"));

        Assertions.assertTrue(catalog.tableExistsCalled);
        Assertions.assertNull(sessionCatalog.lastContext);
    }

    @Test
    public void testDelegatedCatalogRequiresDelegatedCredential() {
        RecordingSessionCatalog sessionCatalog = new RecordingSessionCatalog();
        SessionBackedCatalog catalog = new SessionBackedCatalog(sessionCatalog);
        IcebergSessionCatalogAdapter adapter = new IcebergSessionCatalogAdapter(catalog);

        IllegalStateException exception = Assertions.assertThrows(
                IllegalStateException.class,
                () -> adapter.delegatedCatalog(SessionContext.empty()));

        Assertions.assertTrue(exception.getMessage().contains("requires delegated credential"));
        Assertions.assertFalse(catalog.tableExistsCalled);
        Assertions.assertNull(sessionCatalog.lastContext);
    }

    private static class SessionBackedCatalog implements Catalog, SupportsNamespaces {
        private final BaseSessionCatalog sessionCatalog;
        private boolean tableExistsCalled;
        private boolean listNamespacesCalled;

        private SessionBackedCatalog(BaseSessionCatalog sessionCatalog) {
            this.sessionCatalog = sessionCatalog;
        }

        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(TableIdentifier ident) {
            tableExistsCalled = true;
            return true;
        }

        @Override
        public Table loadTable(TableIdentifier ident) {
            return Mockito.mock(Table.class);
        }

        @Override
        public void invalidateTable(TableIdentifier ident) {
        }

        @Override
        public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean dropTable(TableIdentifier ident) {
            return false;
        }

        @Override
        public boolean dropTable(TableIdentifier ident, boolean purge) {
            return false;
        }

        @Override
        public void renameTable(TableIdentifier from, TableIdentifier to) {
        }

        @Override
        public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        }

        @Override
        public List<Namespace> listNamespaces(Namespace namespace) {
            listNamespacesCalled = true;
            return Collections.emptyList();
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
            return Collections.emptyMap();
        }

        @Override
        public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
            return false;
        }

        @Override
        public boolean setProperties(Namespace namespace, Map<String, String> properties) {
            return false;
        }

        @Override
        public boolean removeProperties(Namespace namespace, Set<String> properties) {
            return false;
        }

        @Override
        public boolean namespaceExists(Namespace namespace) {
            return true;
        }
    }

    private static class RecordingSessionCatalog extends BaseSessionCatalog {
        private org.apache.iceberg.catalog.SessionCatalog.SessionContext lastContext;

        @Override
        public List<TableIdentifier> listTables(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, Namespace namespace) {
            lastContext = context;
            return Collections.emptyList();
        }

        @Override
        public Catalog.TableBuilder buildTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context,
                TableIdentifier ident, Schema schema) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table registerTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context,
                TableIdentifier ident, String metadataFileLocation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, TableIdentifier ident) {
            lastContext = context;
            return true;
        }

        @Override
        public Table loadTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, TableIdentifier ident) {
            lastContext = context;
            return Mockito.mock(Table.class);
        }

        @Override
        public boolean dropTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, TableIdentifier ident) {
            return false;
        }

        @Override
        public boolean purgeTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, TableIdentifier ident) {
            return false;
        }

        @Override
        public void renameTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context,
                TableIdentifier from, TableIdentifier to) {
        }

        @Override
        public void invalidateTable(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, TableIdentifier ident) {
        }

        @Override
        public void createNamespace(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context,
                Namespace namespace, Map<String, String> metadata) {
        }

        @Override
        public List<Namespace> listNamespaces(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, Namespace namespace) {
            lastContext = context;
            return Collections.emptyList();
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, Namespace namespace) {
            lastContext = context;
            return Collections.emptyMap();
        }

        @Override
        public boolean dropNamespace(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context, Namespace namespace) {
            return false;
        }

        @Override
        public boolean updateNamespaceMetadata(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext context,
                Namespace namespace, Map<String, String> updates, Set<String> removals) {
            return false;
        }
    }
}
