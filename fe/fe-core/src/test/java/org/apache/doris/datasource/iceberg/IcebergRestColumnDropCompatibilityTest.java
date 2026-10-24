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

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties.DelegatedTokenMode;

import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Keeps REST DROP on the baseline SDK table endpoint and authentication path. */
public class IcebergRestColumnDropCompatibilityTest {
    private static final TableIdentifier ID = TableIdentifier.of("db", "rest_drop_compatibility");
    private final AtomicReference<Throwable> serverFailure = new AtomicReference<>();
    private InMemoryCatalog backend;
    private RESTSessionCatalog rest;
    private HttpServer server;
    private boolean tableToken;
    private int tablePosts;
    private String tableAuth;

    @Before
    public void setUp() throws Exception {
        backend = new InMemoryCatalog();
        backend.initialize("compatibility", Collections.emptyMap());
        backend.createNamespace(ID.namespace());
        backend.createTable(ID, new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "metric", Types.IntegerType.get()),
                Types.NestedField.optional(3, "other", Types.IntegerType.get())));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "http://127.0.0.1:" + server.getAddress().getPort());
        properties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
        properties.put("rest.auth.type", "oauth2");
        properties.put(OAuth2Properties.TOKEN, "catalog-token");
        properties.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
        rest = new RESTSessionCatalog();
        rest.initialize("compatibility", properties);
    }

    @After
    public void tearDown() throws Exception {
        rest.close();
        server.stop(0);
        backend.close();
        Assert.assertNull(serverFailure.get());
    }

    private TableOperations backendOps() {
        return ((HasTableOperations) backend.loadTable(ID)).operations();
    }

    private Table drop(ColumnPath path) throws Exception {
        org.apache.iceberg.catalog.Catalog catalog = rest.asCatalog(
                org.apache.iceberg.catalog.SessionCatalog.SessionContext.createEmpty());
        Table table = catalog.loadTable(ID);
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class,
                Mockito.withSettings().extraInterfaces(IcebergUserSessionCatalog.class));
        IcebergUserSessionCatalog capability = (IcebergUserSessionCatalog) dorisCatalog;
        Mockito.when(capability.getRestSessionCatalog()).thenReturn(rest);
        Mockito.when(capability.getDelegatedTokenMode()).thenReturn(DelegatedTokenMode.ACCESS_TOKEN);
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() { };
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.doReturn(Optional.empty()).when(dorisCatalog).getDbForReplay(Mockito.anyString());
        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, catalog);
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(dorisTable.getRemoteName()).thenReturn(ID.name());
        IcebergExternalMetaCache.WritableTableLease lease =
                Mockito.mock(IcebergExternalMetaCache.WritableTableLease.class);
        Mockito.when(lease.getTable()).thenReturn(table);
        IcebergRuntimeContext runtime = Mockito.mock(IcebergRuntimeContext.class);
        Mockito.when(runtime.getMetastoreProperties()).thenReturn(Mockito.mock(IcebergRestProperties.class));
        Mockito.when(lease.getRuntimeContext()).thenReturn(runtime);
        Mockito.when(lease.getAuthenticator()).thenReturn(authenticator);
        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            utils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, ops)).thenReturn(lease);
            ops.dropColumn(dorisTable, path, 1L);
        }
        Mockito.verify(lease).close();
        return table;
    }

    @Test
    public void testUnpartitionedDropDoesNotRequireTransactionEndpoint() throws Exception {
        Table table = drop(ColumnPath.of("other"));
        Assert.assertNull(table.schema().findField("other"));
        Assert.assertNull(backendOps().refresh().schema().findField("other"));
        Assert.assertEquals(1, tablePosts);
    }

    @Test
    public void testPartitionedTableAllowsUnreferencedColumn() throws Exception {
        backend.loadTable(ID).updateSpec().addField("metric").commit();
        drop(ColumnPath.of("other"));
        Assert.assertNull(backendOps().refresh().schema().findField("other"));
        Assert.assertNotNull(backendOps().current().schema().findField("metric"));
        Assert.assertEquals(1, tablePosts);
    }

    @Test
    public void testLoadedTableCredentialsArePreserved() throws Exception {
        tableToken = true;
        drop(ColumnPath.of("other"));
        Assert.assertEquals("Bearer table-token", tableAuth);
        Assert.assertNull(backendOps().refresh().schema().findField("other"));
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/v1/config")) {
                send(exchange, ConfigResponseParser.toJson(ConfigResponse.builder().withEndpoints(
                        Arrays.asList(Endpoint.V1_LOAD_TABLE, Endpoint.V1_UPDATE_TABLE)).build()));
                return;
            }
            Assert.assertTrue(path, path.endsWith("/namespaces/db/tables/" + ID.name()));
            if (exchange.getRequestMethod().equals("POST")) {
                tablePosts++;
                tableAuth = exchange.getRequestHeaders().getFirst("Authorization");
                UpdateTableRequest request = UpdateTableRequestParser.fromJson(
                        new String(ByteStreams.toByteArray(exchange.getRequestBody()), StandardCharsets.UTF_8));
                TableOperations operations = backendOps();
                TableMetadata base = operations.refresh();
                request.requirements().forEach(requirement -> requirement.validate(base));
                TableMetadata.Builder updated = TableMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(updated));
                operations.commit(base, updated.build());
            }
            LoadTableResponse.Builder response = LoadTableResponse.builder()
                    .withTableMetadata(backendOps().refresh());
            if (tableToken) {
                response.addConfig(OAuth2Properties.TOKEN, "table-token");
                response.addConfig(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
            }
            send(exchange, LoadTableResponseParser.toJson(response.build()));
        } catch (Throwable failure) {
            serverFailure.compareAndSet(null, failure);
            exchange.sendResponseHeaders(500, -1);
        } finally {
            exchange.close();
        }
    }

    private void send(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
