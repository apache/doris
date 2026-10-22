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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties.DelegatedTokenMode;

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
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Exercises public DROP through real SDK transaction HTTP/auth and committed in-memory metadata. */
public class IcebergRestSchemaCommitTest {
    private static final TableIdentifier ID = TableIdentifier.of("db", "transaction_probe");
    private final SessionContext empty = SessionContext.empty();
    private final org.apache.iceberg.catalog.SessionCatalog.SessionContext defaultContext =
            org.apache.iceberg.catalog.SessionCatalog.SessionContext.createEmpty();
    private IcebergMetadataOps ops;
    private TableCommit submitted;
    private boolean userSessionEnabled;
    private ExternalCatalog dorisCatalog;
    private IcebergUserSessionCatalog capability;
    private Runnable afterAcquire = () -> { };
    private boolean failDorisRefresh;
    private org.apache.iceberg.catalog.SessionCatalog.SessionContext invalidatedContext;
    private final AtomicBoolean authenticated = new AtomicBoolean();
    private final AtomicBoolean leaseClosed = new AtomicBoolean();
    private final ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            Assert.assertFalse(authenticated.getAndSet(true));
            try {
                return task.call();
            } finally {
                authenticated.set(false);
            }
        }
    };
    private InMemoryCatalog backend;
    private RESTSessionCatalog rest;
    private HttpServer server;
    private boolean advertiseTransaction = true;
    private boolean tableToken;
    private boolean requireTableToken;
    private boolean concurrentPartition;
    private int transactionStatus = 204;
    private int transactionPosts;
    private int ordinaryPosts;
    private int loads;
    private String transactionAuth;
    private String ordinaryAuth;
    private String conditionalLoad;
    private UpdateTableRequest received;
    private final AtomicReference<Throwable> serverFailure = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        backend = new InMemoryCatalog();
        backend.initialize("probe", Collections.emptyMap());
        backend.createNamespace(ID.namespace());
        backend.buildTable(ID, new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "a", Types.LongType.get()),
                Types.NestedField.optional(3, "b", Types.LongType.get())))
                .withProperty(TableProperties.FORMAT_VERSION, "2").create();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        initialize(false);
    }

    private void initialize(boolean oauth) throws IOException {
        if (rest != null) {
            rest.close();
        }
        rest = Mockito.spy(new RESTSessionCatalog());
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(authenticated.get());
            Assert.assertFalse(leaseClosed.get());
            java.util.List<TableCommit> commits = invocation.getArgument(1);
            Assert.assertEquals(1, commits.size());
            submitted = commits.get(0);
            return invocation.callRealMethod();
        }).when(rest).commitTransaction(Mockito.any(), Mockito.anyList());
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(authenticated.get());
            Assert.assertFalse(leaseClosed.get());
            invalidatedContext = invocation.getArgument(0);
            return invocation.callRealMethod();
        }).when(rest).invalidateTable(Mockito.any(), Mockito.any());
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "http://127.0.0.1:" + server.getAddress().getPort());
        properties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
        properties.put("rest.auth.type", oauth ? "oauth2" : "none");
        if (oauth) {
            properties.put(OAuth2Properties.TOKEN, "catalog-token");
            properties.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
        }
        rest.initialize("transaction-probe", properties);
        dorisCatalog = Mockito.mock(ExternalCatalog.class,
                Mockito.withSettings().extraInterfaces(IcebergUserSessionCatalog.class));
        capability = (IcebergUserSessionCatalog) dorisCatalog;
        Mockito.when(capability.getRestSessionCatalog()).thenReturn(rest);
        Mockito.when(capability.getDelegatedTokenMode()).thenReturn(DelegatedTokenMode.ACCESS_TOKEN);
        Mockito.when(capability.useSessionCatalog(Mockito.any())).thenAnswer(invocation ->
                userSessionEnabled && ((SessionContext) invocation.getArgument(0)).hasDelegatedCredential());
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.doReturn(Optional.empty()).when(dorisCatalog).getDbForReplay(Mockito.anyString());
        ops = new IcebergMetadataOps(dorisCatalog, rest.asCatalog(defaultContext));
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

    private TableOperations load(SessionContext context) {
        org.apache.iceberg.catalog.SessionCatalog.SessionContext sdkContext =
                userSessionEnabled && context.hasDelegatedCredential()
                        ? IcebergSessionCatalogAdapter.toIcebergSessionContext(context) : defaultContext;
        return ((HasTableOperations) rest.loadTable(sdkContext, ID)).operations();
    }

    private void drop(TableOperations operations, SessionContext context, String column) throws UserException {
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(dorisTable.getRemoteName()).thenReturn(ID.name());
        IcebergExternalMetaCache.WritableTableLease lease =
                Mockito.mock(IcebergExternalMetaCache.WritableTableLease.class);
        IcebergRuntimeContext runtime = Mockito.mock(IcebergRuntimeContext.class);
        IcebergRestProperties retainedProperties = Mockito.mock(IcebergRestProperties.class);
        Mockito.when(retainedProperties.isIcebergRestUserSessionEnabled()).thenReturn(userSessionEnabled);
        Mockito.when(runtime.getMetastoreProperties()).thenReturn(retainedProperties);
        Mockito.when(lease.getRuntimeContext()).thenReturn(runtime);
        Mockito.when(lease.getTable()).thenReturn(new BaseTable(operations, ID.toString()));
        Mockito.when(lease.getAuthenticator()).thenReturn(authenticator);
        leaseClosed.set(false);
        Mockito.doAnswer(invocation -> {
            leaseClosed.set(true);
            return null;
        }).when(lease).close();
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(dorisCatalog).when(db).getCatalog();
        Mockito.doReturn(Optional.of(dorisTable)).when(db).getTableForReplay(Mockito.anyString());
        Mockito.doReturn(Optional.of(db)).when(dorisCatalog).getDbForReplay(Mockito.anyString());
        ExternalMetaCacheMgr cache = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(leaseClosed.get());
            Assert.assertFalse(authenticated.get());
            if (failDorisRefresh) {
                throw new IllegalStateException("injected Doris cache failure");
            }
            return null;
        }).when(cache).invalidateTableCache(dorisTable);
        Env environment = Mockito.mock(Env.class);
        Mockito.when(environment.getExtMetaCacheMgr()).thenReturn(cache);
        Mockito.when(environment.getRefreshManager()).thenReturn(new RefreshManager());
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<SessionContext> sessions = Mockito.mockStatic(
                        SessionContext.class, Mockito.CALLS_REAL_METHODS)) {
            env.when(Env::getCurrentEnv).thenReturn(environment);
            sessions.when(SessionContext::current).thenReturn(context);
            utils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, ops)).thenAnswer(invocation -> {
                afterAcquire.run();
                return lease;
            });
            try {
                ops.dropColumn(dorisTable, column, 1L);
                Mockito.verify(dorisTable).unsetObjectCreated();
                Mockito.verify(dorisTable).setIsValidRelatedTableCached(false);
                Mockito.verify(cache).invalidateRowCountCache(dorisTable);
                Mockito.verify(cache).invalidateTableCache(dorisTable);
            } finally {
                Mockito.verify(lease).close();
                Assert.assertFalse(authenticated.get());
            }
        }
    }

    @Test
    public void testExplicitObjectAndWireRequirementsAndCompleteSdkChanges() throws Exception {
        TableOperations operations = load(empty);
        drop(operations, empty, "b");
        TableCommit commit = submitted;
        Assert.assertTrue(commit.requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
        Assert.assertEquals(ID, received.identifier());
        Assert.assertEquals(commit.requirements().size(), received.requirements().size());
        Assert.assertEquals(UpdateTableRequestParser.toJson(UpdateTableRequest.create(
                ID, commit.requirements(), commit.updates())), UpdateTableRequestParser.toJson(received));
        Assert.assertTrue(received.updates().stream().anyMatch(MetadataUpdate.SetCurrentSchema.class::isInstance));
        Assert.assertFalse(received.updates().stream().anyMatch(MetadataUpdate.AddPartitionSpec.class::isInstance));
        Assert.assertNull(backendOps().refresh().schema().findField("b"));
        Assert.assertEquals(1, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
    }

    @Test
    public void testConcurrentPartitionAdditionUsesOriginalCounterAndRejectsDrop() throws Exception {
        TableOperations operations = load(empty);
        TableMetadata base = operations.current();
        concurrentPartition = true;
        Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        UpdateRequirement.AssertLastAssignedPartitionId counter = received.requirements().stream()
                .filter(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance)
                .map(UpdateRequirement.AssertLastAssignedPartitionId.class::cast).findFirst().get();
        Assert.assertEquals(base.lastAssignedPartitionId(), counter.lastAssignedPartitionId());
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
        Assert.assertEquals(2, backendOps().current().specs().size());
        Assert.assertEquals(0, ordinaryPosts);
    }

    @Test
    public void testConsecutiveDropsReloadAndDiscardStaleHandle() throws Exception {
        TableOperations stale = load(empty);
        TableMetadata base = stale.current();
        drop(stale, empty, "b");
        Assert.assertSame(base, stale.current());
        Assert.assertNotNull(stale.current().schema().findField("b"));
        int beforeLoad = loads;
        TableOperations next = load(empty);
        Assert.assertTrue(loads > beforeLoad);
        Assert.assertNull(conditionalLoad);
        Assert.assertNull(next.current().schema().findField("b"));
        drop(next, empty, "id");
        Assert.assertNull(load(empty).current().schema().findField("id"));
        Assert.assertEquals(1, backendOps().refresh().schema().columns().size());
        Assert.assertEquals(1, backendOps().current().specs().size());
    }

    @Test
    public void testCatalogOAuthAuthentication() throws Exception {
        initialize(true);
        TableOperations operations = load(empty);
        drop(operations, empty, "b");
        Assert.assertEquals("Bearer catalog-token", transactionAuth);
    }

    private SessionContext delegated() {
        return SessionContext.of("probe-user",
                new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN, "user-token"));
    }

    @Test
    public void testDelegatedUserOAuthAuthenticationUsesDorisConversion() throws Exception {
        userSessionEnabled = true;
        initialize(true);
        SessionContext context = delegated();
        TableOperations operations = load(context);
        drop(operations, context, "b");
        Assert.assertEquals("Bearer user-token", transactionAuth);
    }

    @Test
    public void testTableSpecificTokenWorksForOrdinaryCommitButTransactionFails() throws Exception {
        tableToken = true;
        requireTableToken = true;
        initialize(true);
        TableOperations operations = load(empty);
        new BaseTable(operations, "ordinary").updateProperties().set("ordinary-auth", "ok").commit();
        Assert.assertEquals("Bearer table-token", ordinaryAuth);
        UserException error = Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("transaction commit permission"));
        Assert.assertEquals("Bearer catalog-token", transactionAuth);
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
        Assert.assertEquals(1, ordinaryPosts);
        Assert.assertEquals(1, transactionPosts);
    }

    @Test
    public void testDelegatedTransactionAlsoDoesNotInheritTableToken() throws Exception {
        userSessionEnabled = true;
        tableToken = true;
        requireTableToken = true;
        initialize(true);
        SessionContext context = delegated();
        TableOperations operations = load(context);
        new BaseTable(operations, "ordinary").updateProperties().set("delegated-table-auth", "ok").commit();
        Assert.assertEquals("Bearer table-token", ordinaryAuth);
        Assert.assertThrows(UserException.class, () -> drop(operations, context, "b"));
        Assert.assertEquals("Bearer user-token", transactionAuth);
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
    }

    @Test
    public void testMissingAdvertisedEndpointFailsWithoutOrdinaryFallback() throws Exception {
        advertiseTransaction = false;
        initialize(false);
        TableOperations operations = load(empty);
        UserException error = Assert.assertThrows(UserException.class,
                () -> drop(operations, empty, "b"));
        Assert.assertTrue(error.getMessage().contains("transactions/commit"));
        Assert.assertEquals(0, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
    }

    @Test
    public void testHttp501UnsupportedEndpointDoesNotFallback() throws Exception {
        transactionStatus = 501;
        TableOperations operations = load(empty);
        Assert.assertThrows(UserException.class,
                () -> drop(operations, empty, "b"));
        Assert.assertEquals(1, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
    }

    @Test
    public void testHttp404UnadvertisedLegacyServerDoesNotFallback() throws Exception {
        transactionStatus = 404;
        TableOperations operations = load(empty);
        UserException error = Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("transaction endpoint or table not found"));
        Assert.assertEquals(1, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
    }

    @Test
    public void testOrdinaryAddKeepsConcurrentPartitionBehavior() throws Exception {
        TableOperations operations = load(empty);
        concurrentPartition = true;
        new BaseTable(operations, "ordinary").updateSchema().addColumn("added", Types.LongType.get()).commit();
        Assert.assertNotNull(backendOps().refresh().schema().findField("added"));
        Assert.assertEquals(0, transactionPosts);
        Assert.assertFalse(received.requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
    }

    @Test
    public void testOrdinaryRenameKeepsConcurrentPartitionBehavior() throws Exception {
        TableOperations operations = load(empty);
        concurrentPartition = true;
        new BaseTable(operations, "ordinary").updateSchema().renameColumn("b", "renamed").commit();
        Assert.assertNotNull(backendOps().refresh().schema().findField("renamed"));
        Assert.assertEquals(0, transactionPosts);
        Assert.assertFalse(received.requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
    }

    @Test
    public void testDefaultModeDoesNotUseDelegatedCredential() throws Exception {
        userSessionEnabled = false;
        initialize(true);
        SessionContext context = delegated();
        drop(load(context), context, "b");
        Assert.assertEquals("Bearer catalog-token", transactionAuth);
    }

    @Test
    public void testCacheInvalidationFailureReportsAlreadyCommitted() throws Exception {
        TableOperations operations = load(empty);
        Mockito.doThrow(new IllegalStateException("injected cache failure"))
                .when(rest).invalidateTable(Mockito.any(), Mockito.any());
        UserException error = Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("DROP committed"));
        Assert.assertNull(backendOps().refresh().schema().findField("b"));
        Assert.assertEquals(1, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
    }

    @Test
    public void testFailedDropDoesNotConstrainFollowingOrdinaryCommit() throws Exception {
        TableOperations operations = load(empty);
        transactionStatus = 409;
        Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        new BaseTable(operations, "ordinary").updateProperties().set("after-failed-drop", "ok").commit();
        Assert.assertNotNull(backendOps().refresh().schema().findField("b"));
        Assert.assertFalse(received.requirements().stream()
                .anyMatch(UpdateRequirement.AssertLastAssignedPartitionId.class::isInstance));
    }

    @Test
    public void testRetainedDelegatedModeSurvivesCatalogReconfiguration() throws Exception {
        userSessionEnabled = true;
        initialize(true);
        SessionContext context = delegated();
        TableOperations operations = load(context);
        RESTSessionCatalog replacement = Mockito.mock(RESTSessionCatalog.class);
        afterAcquire = () -> {
            userSessionEnabled = false;
            Mockito.when(capability.getRestSessionCatalog()).thenReturn(replacement);
        };
        drop(operations, context, "b");
        Assert.assertEquals("Bearer user-token", transactionAuth);
        Assert.assertEquals(context.getSessionId(), invalidatedContext.sessionId());
        Mockito.verifyNoInteractions(replacement);
    }

    @Test
    public void testRetainedDefaultModeSurvivesCatalogReconfiguration() throws Exception {
        initialize(true);
        SessionContext context = delegated();
        TableOperations operations = load(context);
        afterAcquire = () -> userSessionEnabled = true;
        drop(operations, context, "b");
        Assert.assertEquals("Bearer catalog-token", transactionAuth);
        Assert.assertEquals(defaultContext.sessionId(), invalidatedContext.sessionId());
    }

    @Test
    public void testDorisRefreshFailureReportsAlreadyCommitted() throws Exception {
        failDorisRefresh = true;
        TableOperations operations = load(empty);
        UserException error = Assert.assertThrows(UserException.class, () -> drop(operations, empty, "b"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("DROP committed"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Doris metadata refresh failed"));
        Assert.assertNull(backendOps().refresh().schema().findField("b"));
        Assert.assertEquals(1, transactionPosts);
        Assert.assertEquals(0, ordinaryPosts);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/v1/config")) {
                ConfigResponse.Builder config = ConfigResponse.builder();
                if (!advertiseTransaction) {
                    config.withEndpoints(Arrays.asList(Endpoint.V1_LOAD_TABLE, Endpoint.V1_UPDATE_TABLE));
                }
                send(exchange, 200, ConfigResponseParser.toJson(config.build()));
                return;
            }
            if (exchange.getRequestMethod().equals("GET")) {
                loads++;
                conditionalLoad = exchange.getRequestHeaders().getFirst("If-None-Match");
                sendMetadata(exchange);
                return;
            }
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (path.equals("/v1/transactions/commit")) {
                transactionPosts++;
                Assert.assertTrue(authenticated.get());
                Assert.assertFalse(leaseClosed.get());
                transactionAuth = auth;
                if (transactionStatus != 204) {
                    sendError(exchange, transactionStatus, "UnsupportedOperationException", "transaction unsupported");
                    return;
                }
                CommitTransactionRequest transaction = CommitTransactionRequestParser.fromJson(
                        new String(ByteStreams.toByteArray(exchange.getRequestBody()), StandardCharsets.UTF_8));
                Assert.assertEquals(1, transaction.tableChanges().size());
                received = transaction.tableChanges().get(0);
                if (requireTableToken && !"Bearer table-token".equals(auth)) {
                    sendError(exchange, 403, "ForbiddenException", "table-scoped credential required");
                    return;
                }
                apply(received);
                exchange.sendResponseHeaders(204, -1);
            } else {
                ordinaryPosts++;
                ordinaryAuth = auth;
                if (requireTableToken && !"Bearer table-token".equals(auth)) {
                    sendError(exchange, 403, "ForbiddenException", "table-scoped credential required");
                    return;
                }
                received = UpdateTableRequestParser.fromJson(
                        new String(ByteStreams.toByteArray(exchange.getRequestBody()), StandardCharsets.UTF_8));
                apply(received);
                sendMetadata(exchange);
            }
        } catch (CommitFailedException e) {
            sendError(exchange, 409, "CommitFailedException", e.getMessage());
        } catch (Throwable e) {
            serverFailure.set(e);
            sendError(exchange, 500, "RuntimeException", e.toString());
        } finally {
            exchange.close();
        }
    }

    private void apply(UpdateTableRequest request) {
        if (concurrentPartition) {
            concurrentPartition = false;
            backend.loadTable(ID).updateSpec().addField("b").addNonDefaultSpec().commit();
        }
        TableOperations operations = backendOps();
        TableMetadata base = operations.refresh();
        request.requirements().forEach(requirement -> requirement.validate(base));
        TableMetadata.Builder updated = TableMetadata.buildFrom(base);
        request.updates().forEach(update -> update.applyTo(updated));
        operations.commit(base, updated.build());
    }

    private void sendMetadata(HttpExchange exchange) throws IOException {
        LoadTableResponse.Builder response = LoadTableResponse.builder().withTableMetadata(backendOps().refresh());
        if (tableToken) {
            response.addConfig(OAuth2Properties.TOKEN, "table-token");
            response.addConfig(OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
        }
        exchange.getResponseHeaders().set("ETag", "\"schema-" + backendOps().current().currentSchemaId() + "\"");
        send(exchange, 200, LoadTableResponseParser.toJson(response.build()));
    }

    private void sendError(HttpExchange exchange, int status, String type, String message) throws IOException {
        send(exchange, status, ErrorResponseParser.toJson(ErrorResponse.builder()
                .responseCode(status).withType(type).withMessage(message).build()));
    }

    private void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
