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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class IcebergRestDropPurgeTest {
    private static final String PURGE_PROPERTY = "iceberg.rest.drop-with-purge-enabled";
    private static final Namespace DB = Namespace.of("db");
    private static final TableIdentifier FIRST = TableIdentifier.of(DB, "t1");
    private static final TableIdentifier SECOND = TableIdentifier.of(DB, "t2");

    @Test
    public void restDropCanUnregisterWithoutPurging() throws Exception {
        Fixture fixture = new Fixture("rest", "false");
        fixture.ops.dropTableImpl(fixture.table, false);
        Mockito.verify(fixture.catalog).dropTable(FIRST, false);
    }

    @Test
    public void restDropStillPurgesByDefaultAndWhenExplicitlyEnabled() throws Exception {
        for (String enabled : Arrays.asList(null, "true", "TRUE")) {
            Fixture fixture = new Fixture("rest", enabled);
            fixture.ops.dropTableImpl(fixture.table, false);
            fixture.ops.dropDbImpl("db", false, true);
            Mockito.verify(fixture.catalog, Mockito.times(2)).dropTable(FIRST, true);
            Mockito.verify(fixture.catalog).dropTable(SECOND, true);
        }
    }

    @Test
    public void forceDropUnregistersEveryTableAndStillDropsViews() throws Exception {
        Fixture fixture = new Fixture("rest", "false");
        ViewCatalog views = (ViewCatalog) fixture.catalog;
        TableIdentifier view = TableIdentifier.of(DB, "v");
        Mockito.when(views.listViews(DB)).thenReturn(Collections.singletonList(view));
        fixture.ops.dropDbImpl("db", false, true);
        InOrder order = Mockito.inOrder(fixture.catalog);
        order.verify(fixture.catalog).dropTable(FIRST, false);
        order.verify(fixture.catalog).dropTable(SECOND, false);
        order.verify(views).dropView(view);
        order.verify((SupportsNamespaces) fixture.catalog).dropNamespace(DB);
    }

    @Test
    public void restOptionDoesNotChangeOtherCatalogDropSemantics() throws Exception {
        for (String flavor : Arrays.asList("hms", "hadoop", "jdbc", "glue", "dlf", "s3tables")) {
            Fixture fixture = new Fixture(flavor, "false");
            fixture.ops.dropTableImpl(fixture.table, false);
            fixture.ops.dropDbImpl("db", false, true);
            Mockito.verify(fixture.catalog, Mockito.times(2)).dropTable(FIRST, true);
            Mockito.verify(fixture.catalog).dropTable(SECOND, true);
        }
    }

    @Test
    public void rejectsInvalidBooleanDuringCreateAndAlterValidation() {
        for (String invalid : Arrays.asList("", "yes", "0", " false ")) {
            Map<String, String> raw = properties("rest", invalid);
            IcebergRestExternalCatalog catalog = new IcebergRestExternalCatalog(1L, "rest", null, raw, "");
            DdlException error = Assertions.assertThrows(DdlException.class, catalog::checkProperties);
            Assertions.assertEquals(PURGE_PROPERTY + " must be true or false", error.getDetailMessage());
            // The normal ALTER fallback temporarily publishes candidate properties before validation.
            Map<String, String> updated = properties("rest", invalid);
            IcebergRestExternalCatalog altered = new IcebergRestExternalCatalog(2L, "altered", null,
                    properties("rest", "true"), "");
            DdlException alterError = Assertions.assertThrows(DdlException.class, () -> {
                if (!altered.validatePropertiesBeforeUpdate(altered.getProperties(), updated)) {
                    altered.getCatalogProperty().modifyCatalogProps(updated);
                    altered.checkProperties();
                }
            });
            Assertions.assertEquals(PURGE_PROPERTY + " must be true or false", alterError.getDetailMessage());
        }
    }

    @Test
    public void validBooleansBindWithoutChangingSdkProperties() throws Exception {
        for (String valid : Arrays.asList("true", "false", "TRUE", "FALSE")) {
            IcebergRestProperties bound = (IcebergRestProperties) MetastoreProperties.create(properties("rest", valid));
            Assertions.assertEquals(Boolean.parseBoolean(valid), bound.isIcebergRestDropWithPurgeEnabled());
            Assertions.assertFalse(bound.getIcebergRestCatalogProperties().containsKey(PURGE_PROPERTY));
        }
        // Replay binding must not introduce new statement-time validation.
        Assertions.assertDoesNotThrow(() -> MetastoreProperties.create(properties("rest", "invalid")));
    }

    @Test
    public void failedTableUnregisterStopsTheForceCascade() throws Exception {
        Fixture fixture = new Fixture("rest", "false");
        Mockito.when(fixture.catalog.dropTable(FIRST, false))
                .thenThrow(new IllegalStateException("unregister rejected"));
        DdlException error = Assertions.assertThrows(DdlException.class,
                () -> fixture.ops.dropDbImpl("db", false, true));
        Assertions.assertTrue(error.getMessage().contains("unregister rejected"), error.getMessage());
        Mockito.verify(fixture.catalog, Mockito.never()).dropTable(Mockito.eq(SECOND), Mockito.anyBoolean());
        Mockito.verify((SupportsNamespaces) fixture.catalog, Mockito.never()).dropNamespace(DB);
    }

    @Test
    public void restServerThatRejectsPurgeAcceptsMetadataOnlyDrop() throws Exception {
        List<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                if ("GET".equals(exchange.getRequestMethod())) {
                    byte[] config = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, config.length);
                    exchange.getResponseBody().write(config);
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    String query = exchange.getRequestURI().getRawQuery();
                    queries.add(query);
                    if (query != null && query.contains("purgeRequested=true")) {
                        byte[] error = ("{\"error\":{\"message\":\"purge disabled\","
                                + "\"type\":\"ForbiddenException\",\"code\":403}}")
                                .getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(403, error.length);
                        exchange.getResponseBody().write(error);
                    } else {
                        exchange.sendResponseHeaders(204, -1);
                    }
                } else {
                    exchange.sendResponseHeaders(405, -1);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        try (RESTCatalog catalog = Mockito.spy(new RESTCatalog())) {
            catalog.initialize("test", Collections.singletonMap(CatalogProperties.URI,
                    "http://127.0.0.1:" + server.getAddress().getPort()));
            Mockito.doReturn(true).when(catalog).tableExists(Mockito.any(TableIdentifier.class));
            Mockito.doReturn(false).when(catalog).viewExists(Mockito.any(TableIdentifier.class));
            Fixture defaultDrop = new Fixture("rest", null, catalog);
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> defaultDrop.ops.dropTableImpl(defaultDrop.table, false));
            Assertions.assertTrue(error.getMessage().contains("purge disabled"), error.getMessage());
            Fixture metadataOnly = new Fixture("rest", "false", catalog);
            metadataOnly.ops.dropTableImpl(metadataOnly.table, false);
            Assertions.assertEquals(2, queries.size());
            Assertions.assertEquals("purgeRequested=true", queries.get(0));
            Assertions.assertNull(queries.get(1), "metadata-only REST drop must omit purgeRequested");
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, String> properties(String flavor, String enabled) {
        Map<String, String> raw = new HashMap<>();
        raw.put("type", "iceberg");
        raw.put(IcebergExternalCatalog.ICEBERG_CATALOG_TYPE, flavor);
        raw.put("iceberg.rest.uri", "http://127.0.0.1:8181");
        if (enabled != null) {
            raw.put(PURGE_PROPERTY, enabled);
        }
        return raw;
    }

    private static class Fixture {
        private final Catalog catalog;
        private final IcebergMetadataOps ops;
        private final ExternalTable table = Mockito.mock(ExternalTable.class);

        Fixture(String flavor, String enabled) {
            this(flavor, enabled, Mockito.mock(Catalog.class,
                    Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class)));
            Mockito.when(catalog.tableExists(Mockito.any(TableIdentifier.class))).thenReturn(true);
            Mockito.when(catalog.listTables(DB)).thenReturn(Arrays.asList(FIRST, SECOND));
        }

        Fixture(String flavor, String enabled, Catalog catalog) {
            this.catalog = catalog;
            Map<String, String> raw = properties(flavor, enabled);
            IcebergExternalCatalog doris = Mockito.mock(IcebergExternalCatalog.class);
            Mockito.when(doris.getProperties()).thenReturn(raw);
            Mockito.when(doris.getCatalogProperty()).thenReturn(new CatalogProperty(null, raw));
            Mockito.when(doris.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            });
            ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
            Mockito.when(db.getRemoteName()).thenReturn("db");
            Mockito.when(db.getFullName()).thenReturn("db");
            Mockito.when(doris.getDbNullable("db")).thenReturn(db);
            Mockito.doReturn(db).when(doris).getDbForCatalogOperation(Mockito.any(), Mockito.eq("db"));
            ops = new IcebergMetadataOps(doris, catalog);
            Mockito.when(doris.getMetadataOps()).thenReturn(ops);
            Mockito.when(table.getRemoteDbName()).thenReturn("db");
            Mockito.when(table.getRemoteName()).thenReturn("t1");
            Mockito.when(table.getName()).thenReturn("t1");
        }
    }
}
