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

import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Uses the real SDK REST operations and HTTP/auth client, backed by real in-memory table metadata. */
public class IcebergRestSchemaCommitTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "wire");
    private InMemoryCatalog backing;
    private RESTSessionCatalog rest;
    private HttpServer server;
    private final AtomicReference<UpdateTableRequest> lastRequest = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<Exception> serverFailure = new AtomicReference<>();
    private final AtomicBoolean concurrentPartition = new AtomicBoolean();
    private final AtomicBoolean rejectNextCommit = new AtomicBoolean();

    @Before
    public void setUp() throws Exception {
        backing = new InMemoryCatalog();
        backing.initialize("backing", Collections.emptyMap());
        backing.createNamespace(Namespace.of("db"));
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "a", Types.LongType.get()),
                Types.NestedField.optional(3, "b", Types.LongType.get()));
        backing.buildTable(IDENTIFIER, schema).withProperty(TableProperties.FORMAT_VERSION, "2").create();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        initializeRest(false);
    }

    private void initializeRest(boolean oauth) {
        rest = IcebergRestSchemaCommit.newSessionCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "http://127.0.0.1:" + server.getAddress().getPort());
        properties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
        properties.put("rest.auth.type", oauth ? "oauth2" : "none");
        if (oauth) {
            properties.put(OAuth2Properties.TOKEN, "catalog-token");
            properties.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
        }
        rest.initialize("wire", properties);
    }

    @After
    public void tearDown() throws Exception {
        rest.close();
        server.stop(0);
        backing.close();
        Assert.assertNull(serverFailure.get());
    }

    private TableOperations backingOperations() {
        return ((HasTableOperations) backing.loadTable(IDENTIFIER)).operations();
    }

    private TableOperations restOperations(SessionContext context) {
        return ((HasTableOperations) rest.asCatalog(context).loadTable(IDENTIFIER)).operations();
    }

    private TableMetadata deleting(TableMetadata base, String name) {
        List<Types.NestedField> remaining = new ArrayList<>(base.schema().columns());
        remaining.remove(base.schema().findField(name));
        return base.updateSchema(new Schema(remaining));
    }

    @Test
    public void testRealRestClientSupportsConsecutiveDropsWithoutPartitionUpdates() {
        TableOperations operations = restOperations(SessionContext.createEmpty());
        for (String name : new String[] {"b", "id"}) {
            TableMetadata base = operations.current();
            IcebergRestSchemaCommit.commit(operations, base, deleting(base, name));
            UpdateTableRequest request = lastRequest.get();
            Assert.assertTrue(request.requirements().stream()
                    .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
            Assert.assertFalse(request.updates().stream().anyMatch(MetadataUpdate.AddPartitionSpec.class::isInstance));
            Assert.assertFalse(request.updates().stream()
                    .anyMatch(MetadataUpdate.RemovePartitionSpecs.class::isInstance));
        }
        TableMetadata after = backingOperations().refresh();
        Assert.assertEquals(1, after.schema().columns().size());
        Assert.assertNotNull(after.schema().findField("a"));
        Assert.assertEquals(1, after.specs().size());
        Assert.assertEquals(0, after.defaultSpecId());
    }

    @Test
    public void testRealRestClientRejectsConcurrentPartitionReference() {
        TableOperations operations = restOperations(SessionContext.createEmpty());
        TableMetadata base = operations.current();
        concurrentPartition.set(true);
        Assert.assertThrows(CommitFailedException.class,
                () -> IcebergRestSchemaCommit.commit(operations, base, deleting(base, "b")));
        TableMetadata after = backingOperations().refresh();
        Assert.assertNotNull(after.schema().findField("b"));
        Assert.assertEquals(2, after.specs().size());
    }

    @Test
    public void testFailedDropDoesNotConstrainFollowingOrdinaryCommit() {
        TableOperations operations = restOperations(SessionContext.createEmpty());
        TableMetadata base = operations.current();
        rejectNextCommit.set(true);
        Assert.assertThrows(CommitFailedException.class,
                () -> IcebergRestSchemaCommit.commit(operations, base, deleting(base, "b")));
        new BaseTable(operations, "wire").updateProperties().set("after-failed-drop", "true").commit();
        Assert.assertFalse(lastRequest.get().requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
        Assert.assertNotNull(backingOperations().refresh().schema().findField("b"));
        Assert.assertEquals("true", backingOperations().current().properties().get("after-failed-drop"));
    }

    @Test
    public void testDelegatedAuthClientRetainsPartitionRequirement() throws Exception {
        rest.close();
        initializeRest(true);
        SessionContext context = new SessionContext("user-session", null,
                Collections.singletonMap(OAuth2Properties.TOKEN, "user-token"), Collections.emptyMap());
        TableOperations operations = restOperations(context);
        TableMetadata base = operations.current();
        IcebergRestSchemaCommit.commit(operations, base, deleting(base, "b"));
        Assert.assertEquals("Bearer user-token", authorization.get());
        Assert.assertTrue(lastRequest.get().requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
        Assert.assertNull(backingOperations().refresh().schema().findField("b"));
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (exchange.getRequestURI().getPath().equals("/v1/config")) {
                send(exchange, 200, "{\"defaults\":{},\"overrides\":{}}");
                return;
            }
            if (exchange.getRequestMethod().equals("POST")) {
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                UpdateTableRequest request = UpdateTableRequestParser.fromJson(
                        new String(ByteStreams.toByteArray(exchange.getRequestBody()), StandardCharsets.UTF_8));
                lastRequest.set(request);
                if (concurrentPartition.getAndSet(false)) {
                    backing.loadTable(IDENTIFIER).updateSpec().addField("b").addNonDefaultSpec().commit();
                }
                if (rejectNextCommit.getAndSet(false)) {
                    throw new CommitFailedException("Injected commit conflict");
                }
                TableOperations operations = backingOperations();
                TableMetadata base = operations.refresh();
                request.requirements().forEach(requirement -> requirement.validate(base));
                TableMetadata.Builder updated = TableMetadata.buildFrom(base);
                request.updates().forEach(update -> update.applyTo(updated));
                operations.commit(base, updated.build());
            }
            send(exchange, 200, LoadTableResponseParser.toJson(
                    LoadTableResponse.builder().withTableMetadata(backingOperations().refresh()).build()));
        } catch (CommitFailedException e) {
            ErrorResponse error = ErrorResponse.builder().responseCode(409).withType("CommitFailedException")
                    .withMessage(e.getMessage()).build();
            send(exchange, 409, IcebergRestSchemaCommit.newMapper().writeValueAsString(error));
        } catch (Exception e) {
            serverFailure.set(e);
            send(exchange, 500, "{}");
        } finally {
            exchange.close();
        }
    }

    private void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
