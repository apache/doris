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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergMetadataOpTest {

    @Test
    public void testGetNamespaces() {
        Namespace ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1.db2.db3");
        Assert.assertEquals(3, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1..db2");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "db1");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "");
        Assert.assertEquals(0, ns.length());
    }

    @Test
    public void testListTableNamesSkipsViewsWhenRestViewDisabled() {
        IcebergRestExternalCatalog dorisCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));

        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");
        props.put("iceberg.rest.view-enabled", "false");

        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, props));

        Namespace namespace = Namespace.of("PUBLIC");
        TableIdentifier table = TableIdentifier.of(namespace, "DORIS_HORIZON_T");
        Mockito.when(icebergCatalog.listTables(namespace)).thenReturn(Collections.singletonList(table));

        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);
        List<String> tableNames = ops.listTableNames("PUBLIC");

        Assert.assertEquals(Collections.singletonList("DORIS_HORIZON_T"), tableNames);
        Mockito.verify((ViewCatalog) icebergCatalog, Mockito.never()).listViews(Mockito.any());
    }

    @Test
    public void testListTableNamesFiltersViewsWhenRestViewEnabled() {
        IcebergRestExternalCatalog dorisCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        // The default Catalog handed to IcebergMetadataOps is asCatalog(empty); it is NOT a ViewCatalog.
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        RESTSessionCatalog sessionCatalog = Mockito.mock(RESTSessionCatalog.class);
        ViewCatalog viewCatalog = Mockito.mock(ViewCatalog.class);

        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.useSessionCatalog(Mockito.any())).thenReturn(false);
        Mockito.when(dorisCatalog.isViewEnabled()).thenReturn(true);
        Mockito.when(dorisCatalog.getRestSessionCatalog()).thenReturn(sessionCatalog);
        Mockito.when(dorisCatalog.getDelegatedTokenMode())
                .thenReturn(IcebergRestProperties.DelegatedTokenMode.ACCESS_TOKEN);
        Mockito.when(sessionCatalog.asViewCatalog(Mockito.any())).thenReturn(viewCatalog);

        Namespace namespace = Namespace.of("PUBLIC");
        TableIdentifier table = TableIdentifier.of(namespace, "DORIS_HORIZON_T");
        TableIdentifier view = TableIdentifier.of(namespace, "DORIS_HORIZON_V");
        Mockito.when(icebergCatalog.listTables(namespace)).thenReturn(Arrays.asList(table, view));
        Mockito.when(viewCatalog.listViews(namespace)).thenReturn(Collections.singletonList(view));

        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);
        List<String> tableNames = ops.listTableNames("PUBLIC");

        Assert.assertEquals(Collections.singletonList("DORIS_HORIZON_T"), tableNames);
        Mockito.verify(viewCatalog).listViews(namespace);
    }

    @Test
    public void testRejectsRequestWithoutCredentialWhenDynamicIdentityEnabled() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");
        props.put("iceberg.rest.security.type", "oauth2");
        props.put("iceberg.rest.session", "user");
        props.put("iceberg.rest.oauth2.credential", "client_credentials");
        props.put("iceberg.rest.oauth2.server-uri", "http://auth.example.com/token");

        IcebergRestExternalCatalog catalog =
                new IcebergRestExternalCatalog(1, "rest_user_session", null, props, "");

        // Dynamic identity is configured but the session has no delegated credential (e.g. a password login):
        // rejected, never falls back to a shared/borrowed identity.
        Assertions.assertThrows(IllegalStateException.class,
                () -> catalog.useSessionCatalog(SessionContext.empty()));

        // With a delegated credential, the per-user session catalog is used.
        SessionContext withCredential = SessionContext.of(
                new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN, "delegated-access-token"));
        Assert.assertTrue(catalog.useSessionCatalog(withCredential));
    }

    @Test
    public void testNoSessionCatalogWhenDynamicIdentityDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");

        IcebergRestExternalCatalog catalog =
                new IcebergRestExternalCatalog(1, "rest_plain", null, props, "");

        // Without dynamic identity, no request uses the session catalog and none is rejected.
        Assert.assertFalse(catalog.useSessionCatalog(SessionContext.empty()));
        Assert.assertFalse(catalog.useSessionCatalog(SessionContext.of(
                new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN, "delegated-access-token"))));
    }
}
