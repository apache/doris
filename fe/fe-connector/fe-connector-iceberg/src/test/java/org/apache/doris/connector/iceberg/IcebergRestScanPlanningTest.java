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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.iceberg.IcebergStatementScopeTest.ScopeSession;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises actual SDK RESTTable capabilities across Doris read ownership boundaries. */
public class IcebergRestScanPlanningTest {
    private static final TableIdentifier ID = TableIdentifier.of("db", "t");

    @Test
    public void directReadRejectsBeforeSnapshotConversion() throws Exception {
        for (boolean serverOverride : new boolean[] {false, true}) {
            for (boolean populated : new boolean[] {false, true}) {
                try (RestFixture fixture = new RestFixture(serverOverride, populated, true)) {
                    Table table = fixture.load();
                    Assertions.assertInstanceOf(BaseTable.class, table);
                    Assertions.assertFalse(((SupportsDistributedScanPlanning) table).allowDistributedPlanning());
                    for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE,
                            new TestStatementScope())) {
                        ConnectorSession session = new ScopeSession(1L, "q", scope);
                        reject(() -> IcebergStatementScope.sharedTable(session, "db", "t", () -> table));
                        if (scope == ConnectorStatementScope.NONE) {
                            reject(() -> IcebergStatementScope.sharedBorrowedTable(session, "db", "t", () -> {
                                throw new AssertionError("NONE must use direct loader");
                            }, () -> table));
                        }
                        scope.closeAll();
                    }
                    Assertions.assertEquals(0, fixture.io(table).reads.get());
                    Assertions.assertEquals(0, fixture.planRequests.get());
                }
            }
        }
    }

    @Test
    public void cacheRejectsBeforeSizingAndReleasesOwnership() throws Exception {
        for (boolean serverOverride : new boolean[] {false, true}) {
            for (boolean populated : new boolean[] {false, true}) {
                try (RestFixture fixture = new RestFixture(serverOverride, populated, true)) {
                    for (CacheSpec spec : List.of(CacheSpec.ofConnectorTtl(0, 10),
                            CacheSpec.ofConnectorTtl(100, 10), CacheSpec.ofWeight(true, 100, 1000, 1000000))) {
                        AtomicInteger tableCleanups = new AtomicInteger();
                        AtomicInteger catalogCleanups = new AtomicInteger();
                        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
                        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                            IcebergTableCache cache = new IcebergTableCache(owner, spec,
                                    ignored -> tableCleanups::incrementAndGet, tracker);
                            Table table = fixture.load();
                            for (int attempt = 0; attempt < 2; attempt++) {
                                reject(() -> cache.borrow(ID, () -> table));
                            }
                            Assertions.assertEquals(0, cache.size());
                            Assertions.assertEquals(2, tableCleanups.get());
                            Assertions.assertEquals(0, fixture.io(table).reads.get());
                            cache.close();
                            tracker.close(catalogCleanups::incrementAndGet);
                            Assertions.assertEquals(1, catalogCleanups.get(), "rejected loads must release leases");
                            Assertions.assertEquals(0, tracker.retainedCleanupCount());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void trackedReadsReleaseRejectedTablesButWritableTablesRemainAvailable() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE, new TestStatementScope())) {
                ConnectorSession session = new ScopeSession(1L, "q", scope);
                IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
                AtomicInteger tableCleanups = new AtomicInteger();
                AtomicInteger catalogCleanups = new AtomicInteger();
                Table table = fixture.load();
                reject(() -> IcebergStatementScope.withTrackedTable(session, "db", "t", tracker,
                        () -> table, ignored -> tableCleanups::incrementAndGet, value -> value));
                Assertions.assertEquals(1, tableCleanups.get());
                Assertions.assertSame(table,
                        IcebergStatementScope.sharedWritableTable(session, "db", "t", () -> table));
                scope.closeAll();
                tracker.close(catalogCleanups::incrementAndGet);
                Assertions.assertEquals(1, tableCleanups.get(), "failed scope load must not retain a close callback");
                Assertions.assertEquals(1, catalogCleanups.get());
                Assertions.assertEquals(0, fixture.io(table).reads.get());
            }
        }
    }

    @Test
    public void unscopedDirectRejectionReleasesTableOwnedResources() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            Table table = fixture.load();
            AtomicInteger cleanups = new AtomicInteger();
            reject(() -> IcebergStatementScope.sharedTrackedTable(null, "db", "t",
                    new IcebergCatalogResourceTracker(), () -> table, ignored -> cleanups::incrementAndGet));
            Assertions.assertEquals(1, cleanups.get());
            Assertions.assertEquals(0, fixture.io(table).reads.get());
        }
    }

    @Test
    public void clientPlanningStillCreatesPinnedReadViews() throws Exception {
        try (RestFixture fixture = new RestFixture(false, true, false);
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Table table = fixture.load();
            Assertions.assertTrue(((SupportsDistributedScanPlanning) table).allowDistributedPlanning());
            Table direct = IcebergStatementScope.sharedTable(null, "db", "t", () -> table);
            Assertions.assertNotSame(table, direct);
            Assertions.assertEquals(table.currentSnapshot().snapshotId(), direct.currentSnapshot().snapshotId());
            IcebergTableCache cache = new IcebergTableCache(owner, 100, 10, ignored -> () -> { }, null);
            try (IcebergTableCache.TableLease lease = cache.borrow(ID, () -> table)) {
                Table cached = lease.snapshotReadTable();
                Assertions.assertNotSame(table, cached);
                Assertions.assertEquals(direct.currentSnapshot().snapshotId(), cached.currentSnapshot().snapshotId());
            }
            cache.close();
            Assertions.assertEquals(0, fixture.io(table).reads.get());
        }
    }

    @Test
    public void scanAndMetadataTableRejectThroughBothAcquisitionPaths() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            for (boolean cached : new boolean[] {false, true}) {
                for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE, new TestStatementScope())) {
                    ConnectorSession session = new ScopeSession(1L, "q", scope);
                    Table table = fixture.load();
                    RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
                    ops.table = table;
                    IcebergTableCache cache = new IcebergTableCache(100, 10);
                    try {
                        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                                IcebergCatalogProperties.of(Map.of()), ignored -> ops, null, null,
                                cached ? cache : null);
                        reject(() -> provider.planScan(session, ConnectorScanRequest.builder(
                                new IcebergTableHandle("db", "t"), List.of()).build()));
                        reject(() -> provider.planScan(session, ConnectorScanRequest.builder(
                                IcebergTableHandle.forSystemTable("db", "t", "files", -1L, null, -1L),
                                List.of()).build()));
                        Assertions.assertEquals(0, fixture.io(table).reads.get());
                    } finally {
                        scope.closeAll();
                        cache.close();
                    }
                }
            }
            Assertions.assertEquals(0, fixture.planRequests.get());
        }
    }

    private static void reject(Executable action) {
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, action);
        Assertions.assertEquals("Iceberg server-side scan planning is not supported for table db.t; "
                + "configure the REST catalog to use client-side scan planning", failure.getMessage());
    }

    /** A minimal real HTTP catalog; any attempt to read local manifests fails immediately. */
    private static final class RestFixture implements AutoCloseable {
        private final HttpServer server;
        private final RESTCatalog catalog = new RESTCatalog();
        private final AtomicInteger planRequests = new AtomicInteger();

        private RestFixture(boolean serverOverride, boolean populated, boolean serverPlanning) throws IOException {
            byte[] loadResponse;
            try (InMemoryCatalog storage = new InMemoryCatalog()) {
                storage.initialize("storage", Map.of());
                storage.createNamespace(Namespace.of("db"));
                Table table = storage.createTable(ID,
                        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
                if (populated) {
                    table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                            .withPath("memory://data.parquet").withRecordCount(4).withFileSizeInBytes(100).build())
                            .commit();
                }
                LoadTableResponse.Builder response = LoadTableResponse.builder()
                        .withTableMetadata(((BaseTable) table).operations().current());
                if (serverOverride) {
                    response.addConfig("scan-planning-mode", "server");
                }
                loadResponse = LoadTableResponseParser.toJson(response.build()).getBytes(StandardCharsets.UTF_8);
            }
            byte[] config = ConfigResponseParser.toJson(ConfigResponse.builder()
                    .withEndpoints(List.of(Endpoint.V1_LOAD_TABLE, Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN)).build())
                    .getBytes(StandardCharsets.UTF_8);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                byte[] response;
                int status = 200;
                if (path.equals("/v1/config")) {
                    response = config;
                } else if (path.equals("/v1/namespaces/db/tables/t")) {
                    response = loadResponse;
                } else {
                    planRequests.incrementAndGet();
                    response = new byte[0];
                    status = 500;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, response.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(response);
                }
            });
            server.start();
            catalog.initialize("rest", Map.of("uri", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "scan-planning-mode", serverPlanning && !serverOverride ? "server" : "client",
                    "rest.auth.type", "none", "io-impl", RecordingFileIO.class.getName()));
        }

        private Table load() {
            return catalog.loadTable(ID);
        }

        private RecordingFileIO io(Table table) {
            return (RecordingFileIO) table.io();
        }

        @Override
        public void close() throws IOException {
            try {
                catalog.close();
            } finally {
                server.stop(0);
            }
        }
    }

    public static class RecordingFileIO implements FileIO {
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public InputFile newInputFile(String path) {
            reads.incrementAndGet();
            throw new AssertionError("Unexpected local metadata read: " + path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }

}
