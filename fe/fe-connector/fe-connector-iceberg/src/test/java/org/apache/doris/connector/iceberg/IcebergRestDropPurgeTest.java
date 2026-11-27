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

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

class IcebergRestDropPurgeTest {
    private static final String PURGE_PROPERTY = "iceberg.rest.drop-with-purge-enabled";

    @Test
    void restDropCanUnregisterWithoutPurging() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext context = new RecordingConnectorContext();
        metadata(ops, context, "rest", "false").dropTable(null, new IcebergTableHandle("db", "t"));
        Assertions.assertEquals(List.of("dropTable:db.t:purge=false"), ops.log);
        Assertions.assertFalse(ops.lastDropPurge);
        Assertions.assertTrue(context.cleanedLocations.isEmpty());
        Assertions.assertEquals(1, context.authCount);
    }

    @Test
    void restDropStillPurgesByDefaultAndWhenExplicitlyEnabled() {
        for (String enabled : Arrays.asList(null, "true")) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            metadata(ops, new RecordingConnectorContext(), "rest", enabled)
                    .dropTable(null, new IcebergTableHandle("db", "t"));
            Assertions.assertEquals(List.of("dropTable:db.t:purge=true"), ops.log);
        }
    }

    @Test
    void forceDropUnregistersEveryTableAndStillDropsViews() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = List.of("t1", "t2");
        ops.views = List.of("v");
        RecordingConnectorContext context = new RecordingConnectorContext();
        metadata(ops, context, "rest", "false").dropDatabase(null, "db", false, true);
        Assertions.assertEquals(List.of("listTableNames:db", "dropTable:db.t1:purge=false",
                "dropTable:db.t2:purge=false", "listViewNames:db", "dropView:db.v", "dropDatabase:db"), ops.log);
        Assertions.assertTrue(context.cleanedLocations.isEmpty());
        Assertions.assertEquals(1, context.authCount);
    }

    @Test
    void restOptionDoesNotChangeOtherCatalogDropSemantics() {
        for (String flavor : List.of("hms", "hadoop", "jdbc", "glue", "dlf", "s3tables")) {
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            metadata(ops, new RecordingConnectorContext(), flavor, "false")
                    .dropTable(null, new IcebergTableHandle("db", "t"));
            Assertions.assertTrue(ops.lastDropPurge, flavor);
            ops.tables = List.of("t1", "t2");
            ops.log.clear();
            metadata(ops, new RecordingConnectorContext(), flavor, "false")
                    .dropDatabase(null, "db", false, true);
            Assertions.assertTrue(ops.log.contains("dropTable:db.t1:purge=true"), flavor);
            Assertions.assertTrue(ops.log.contains("dropTable:db.t2:purge=true"), flavor);
        }
    }

    @Test
    void rejectsInvalidBooleanDuringCatalogStatementValidation() {
        for (String invalid : List.of("", "yes", "0", " false ")) {
            Map<String, String> raw = properties("rest", invalid);
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> IcebergCatalogProperties.of(raw).checkCreateTimeOnlyRules());
            Assertions.assertTrue(error.getMessage().contains(PURGE_PROPERTY), error.getMessage());
            // Binding during replay does not run newly introduced statement-time validation.
            Assertions.assertDoesNotThrow(() -> IcebergCatalogProperties.of(raw));
        }
        for (String valid : List.of("true", "false", "TRUE", "FALSE")) {
            Assertions.assertDoesNotThrow(() -> IcebergCatalogProperties.of(properties("rest", valid))
                    .checkCreateTimeOnlyRules());
        }
    }

    @Test
    void restServerThatRejectsPurgeAcceptsMetadataOnlyDrop() throws Exception {
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
        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("test", Map.of(CatalogProperties.URI,
                    "http://127.0.0.1:" + server.getAddress().getPort()));
            CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(catalog);
            RecordingConnectorContext context = new RecordingConnectorContext();
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata(ops, context, "rest", null).dropTable(null, new IcebergTableHandle("db", "t")));
            Assertions.assertTrue(error.getMessage().contains("purge disabled"), error.getMessage());
            Assertions.assertDoesNotThrow(() -> metadata(ops, context, "rest", "false")
                    .dropTable(null, new IcebergTableHandle("db", "t")));
            Assertions.assertEquals(2, queries.size());
            Assertions.assertEquals("purgeRequested=true", queries.get(0));
            Assertions.assertNull(queries.get(1), "metadata-only REST drop must omit purgeRequested");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void failedTableUnregisterStopsTheForceCascade() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = List.of("t1", "t2");
        ops.dropTableFailure = new IllegalStateException("unregister rejected");
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, new RecordingConnectorContext(), "rest", "false")
                        .dropDatabase(null, "db", false, true));
        Assertions.assertTrue(error.getMessage().contains("unregister rejected"), error.getMessage());
        Assertions.assertEquals(List.of("listTableNames:db", "dropTable:db.t1:purge=false"), ops.log);
    }

    private static Map<String, String> properties(String flavor, String enabled) {
        Map<String, String> raw = new HashMap<>();
        raw.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, flavor);
        if (enabled != null) {
            raw.put(PURGE_PROPERTY, enabled);
        }
        return raw;
    }

    private static IcebergConnectorMetadata metadata(IcebergCatalogOps ops, RecordingConnectorContext context,
            String flavor, String enabled) {
        return new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(properties(flavor, enabled)), context);
    }
}
