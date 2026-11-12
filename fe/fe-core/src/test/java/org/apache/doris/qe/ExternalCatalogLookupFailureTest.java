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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.rest.StmtExecutionAction;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.mysql.DummyMysqlChannel;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.utframe.TestWithFeService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ExternalCatalogLookupFailureTest extends TestWithFeService {
    private static final String CATALOG = "lookup_failure_catalog";
    private static final String DATABASE = "example_db";
    private static final String FAILURE = "Metastore authentication failed";
    private TestExternalCatalog catalog;
    private boolean initializationUnavailable;
    private boolean failAfterLookup;

    @Override
    protected void runBeforeAll() throws Exception {
        executeSql("CREATE USER 'lookup_unprivileged_user'");
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("CREATE TABLE healthy_table (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1')");
    }

    @BeforeEach
    public void prepareCatalog() {
        FailingProvider.unavailable = false;
        FailingProvider.lookupCount = 0;
        initializationUnavailable = false;
        failAfterLookup = false;
        catalog = new TestExternalCatalog(Env.getCurrentEnv().getNextId(), CATALOG, "",
                new HashMap<>(Map.of("catalog_provider.class", FailingProvider.class.getName())), "") {
            @Override
            public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
                ExternalDatabase<? extends ExternalTable> db = super.getDbNullable(dbName);
                if (failAfterLookup) {
                    // Invalidate after the first successful probe, before forwarding can publish its default database.
                    failAfterLookup = false;
                    resetToUninitialized(false);
                    initializationUnavailable = true;
                }
                return db;
            }

            @Override
            protected void initLocalObjectsImpl() {
                if (initializationUnavailable) {
                    throw new IllegalStateException(FAILURE);
                }
            }
        };
        Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "addCatalog", catalog);
    }

    @AfterEach
    public void removeCatalog() {
        FailingProvider.unavailable = false;
        Deencapsulation.invoke(Env.getCurrentEnv().getCatalogMgr(), "removeCatalog", catalog.getId());
        catalog.onClose();
        Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(catalog.getId());
        connectContext.setThreadLocalInfo();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFieldListReturnsErrorAndKeepsSessionUsable(boolean failInitialization) throws Exception {
        RecordingChannel channel = new RecordingChannel();
        ConnectContext context = new ConnectContext(new MysqlProtocolAdapter(channel));
        context.setEnv(Env.getCurrentEnv());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.changeDefaultCatalog(CATALOG);
        context.setDatabase(DATABASE);
        context.setThreadLocalInfo();
        catalog.resetToUninitialized(false);
        initializationUnavailable = failInitialization;
        FailingProvider.unavailable = true;
        channel.request = ByteBuffer.wrap(new byte[] {4, 't', 0, 0});
        MysqlConnectProcessor processor = new MysqlConnectProcessor(context);

        Assertions.assertDoesNotThrow(processor::processOnce);
        Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
        Assertions.assertEquals(0xFF, Byte.toUnsignedInt(channel.response.get(0)));
        Assertions.assertTrue(StandardCharsets.UTF_8.decode(channel.response.duplicate()).toString().contains(FAILURE));
        Assertions.assertFalse(context.isKilled());

        channel.request = ByteBuffer.wrap(new byte[] {14});
        processor.processOnce();
        Assertions.assertEquals(0, channel.response.get(0));
        Assertions.assertFalse(context.isKilled());
    }

    @Test
    public void testForwardedRefreshSurvivesUnavailableDefaultDatabase() throws Exception {
        FailingProvider.unavailable = true;
        TMasterOpResult result = forward("REFRESH CATALOG " + CATALOG);
        Assertions.assertEquals(MysqlStateType.OK.name(), result.getStatus(), result.getErrMessage());
    }

    @Test
    public void testForwardedAlterSurvivesUnavailableDefaultDatabase() throws Exception {
        FailingProvider.unavailable = true;
        TMasterOpResult result = forward("ALTER CATALOG " + CATALOG
                + " SET PROPERTIES ('metadata_refresh_interval_sec' = '123')");
        Assertions.assertEquals(MysqlStateType.OK.name(), result.getStatus(), result.getErrMessage());
        Assertions.assertEquals("123", catalog.getProperties().get("metadata_refresh_interval_sec"));
    }

    @Test
    public void testForwardedDatabaseAccessStillReportsFailure() throws Exception {
        FailingProvider.unavailable = true;
        TMasterOpResult result = forward("SHOW TABLES");
        Assertions.assertEquals(MysqlStateType.ERR.name(), result.getStatus());
        Assertions.assertTrue(result.getErrMessage().contains(FAILURE), result.getErrMessage());
        Assertions.assertTrue(FailingProvider.lookupCount > 1, "The statement must attempt database access");
    }

    @Test
    public void testForwardedDatabaseAccessRestoresHealthyDefault() throws Exception {
        TMasterOpResult result = forward("SHOW TABLES");
        Assertions.assertNotEquals(MysqlStateType.ERR.name(), result.getStatus(), result.getErrMessage());
    }

    @Test
    public void testForwardedAlterStillChecksCatalogPrivilege() throws Exception {
        FailingProvider.unavailable = true;
        TMasterOpResult result = forward("ALTER CATALOG " + CATALOG
                + " SET PROPERTIES ('metadata_refresh_interval_sec' = '123')",
                UserIdentity.createAnalyzedUserIdentWithIp("lookup_unprivileged_user", "%"));
        Assertions.assertEquals(MysqlStateType.ERR.name(), result.getStatus());
        Assertions.assertTrue(result.getErrMessage().contains("Access denied for user"), result.getErrMessage());
        Assertions.assertNull(catalog.getProperties().get("metadata_refresh_interval_sec"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testForwardedRepairSurvivesResetAfterSuccessfulLookup(boolean alter) throws Exception {
        failAfterLookup = true;
        String sql = alter ? "ALTER CATALOG " + CATALOG + " SET PROPERTIES ('metadata_refresh_interval_sec' = '123')"
                : "REFRESH CATALOG " + CATALOG;
        TMasterOpResult result = forward(sql);
        Assertions.assertTrue(initializationUnavailable, "The successful lookup must trigger the outage");
        Assertions.assertEquals(MysqlStateType.OK.name(), result.getStatus(), result.getErrMessage());
        if (alter) {
            Assertions.assertEquals("123", catalog.getProperties().get("metadata_refresh_interval_sec"));
        }
    }

    @Test
    public void testForwardedActualAccessFailsAfterSuccessfulLookup() throws Exception {
        failAfterLookup = true;
        TMasterOpResult result = forward("SHOW TABLES");
        Assertions.assertTrue(initializationUnavailable);
        Assertions.assertEquals(MysqlStateType.ERR.name(), result.getStatus());
        Assertions.assertTrue(result.getErrMessage().contains(FAILURE), result.getErrMessage());
    }

    @Test
    public void testForwardedHealthyDefaultStillChecksDatabasePrivilege() throws Exception {
        TMasterOpResult result = forward("SELECT 1",
                UserIdentity.createAnalyzedUserIdentWithIp("lookup_unprivileged_user", "%"));
        Assertions.assertEquals(MysqlStateType.ERR.name(), result.getStatus());
        Assertions.assertTrue(result.getErrMessage().contains("Access denied"), result.getErrMessage());
    }

    @Test
    public void testHttpSchemaIgnoresUnavailableDefaultDatabase() {
        FailingProvider.unavailable = true;
        Object schema = Assertions.assertDoesNotThrow(() -> new StmtExecutionAction().querySchema(CATALOG, DATABASE,
                httpRequest("root"), Mockito.mock(HttpServletResponse.class),
                "SELECT * FROM internal." + DATABASE + ".healthy_table"));
        Assertions.assertTrue(schema.toString().contains("CREATE TABLE"), schema.toString());
        Assertions.assertTrue(schema.toString().contains("healthy_table"), schema.toString());
        Assertions.assertEquals(CATALOG, ConnectContext.get().getDefaultCatalog());
        Assertions.assertEquals(DATABASE, ConnectContext.get().getDatabase());
    }

    @Test
    public void testHttpSchemaActualDatabaseAccessStillFails() {
        FailingProvider.unavailable = true;
        RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                () -> new StmtExecutionAction().querySchema(CATALOG, DATABASE,
                        httpRequest("root"), Mockito.mock(HttpServletResponse.class), "SELECT * FROM t"));
        Assertions.assertTrue(error.toString().contains(FAILURE), error.toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHttpEndpointsRejectUnauthenticatedRequests(boolean schema) {
        FailingProvider.unavailable = true;
        StmtExecutionAction action = new StmtExecutionAction();
        HttpServletRequest request = httpRequest("missing_http_user");
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Assertions.assertThrows(UnauthorizedException.class, () -> {
            if (schema) {
                action.querySchema(CATALOG, DATABASE, request, response,
                        "SELECT * FROM internal." + DATABASE + ".healthy_table");
            } else {
                action.executeSQL(CATALOG, DATABASE, request, response,
                        "{\"stmt\":\"REFRESH CATALOG " + CATALOG + "\"}");
            }
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHttpRepairReachesSubmissionWithUnavailableDefaultDatabase(boolean alter) throws Exception {
        FailingProvider.unavailable = true;
        StatementSubmitter original = Deencapsulation.getField(StmtExecutionAction.class, StatementSubmitter.class);
        StatementSubmitter submitter = Mockito.mock(StatementSubmitter.class);
        Mockito.when(submitter.submit(Mockito.any())).thenReturn(
                CompletableFuture.completedFuture(new ExecutionResultSet(Map.of())));
        Deencapsulation.setField(StmtExecutionAction.class, "stmtSubmitter", submitter);
        String sql = alter ? "ALTER CATALOG " + CATALOG + " SET PROPERTIES ('metadata_refresh_interval_sec' = '123')"
                : "REFRESH CATALOG " + CATALOG;
        try {
            ResponseEntity<?> response = (ResponseEntity<?>) Assertions.assertDoesNotThrow(
                    () -> new StmtExecutionAction().executeSQL(CATALOG, DATABASE, httpRequest("root"),
                            Mockito.mock(HttpServletResponse.class), "{\"stmt\":\"" + sql + "\"}"));
            Assertions.assertEquals(0, ((ResponseBody<?>) response.getBody()).getCode());
            ArgumentCaptor<StatementSubmitter.StmtContext> submitted =
                    ArgumentCaptor.forClass(StatementSubmitter.StmtContext.class);
            Mockito.verify(submitter).submit(submitted.capture());
            Assertions.assertEquals(sql, submitted.getValue().stmt);
            Assertions.assertEquals("root", submitted.getValue().user);
            Assertions.assertEquals(CATALOG, ConnectContext.get().getDefaultCatalog());
            Assertions.assertEquals(DATABASE, ConnectContext.get().getDatabase());
        } finally {
            Deencapsulation.setField(StmtExecutionAction.class, "stmtSubmitter", original);
        }
    }

    private HttpServletRequest httpRequest(String user) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getScheme()).thenReturn("https");
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        Mockito.when(request.getHeader("Authorization")).thenReturn("Basic "
                + Base64.getEncoder().encodeToString((user + ":").getBytes(StandardCharsets.UTF_8)));
        Mockito.when(request.getHeader("X-Doris-Stream")).thenReturn("false");
        return request;
    }

    private TMasterOpResult forward(String sql) throws Exception {
        return forward(sql, UserIdentity.ROOT);
    }

    private TMasterOpResult forward(String sql, UserIdentity user) throws Exception {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setDb(DATABASE);
        request.setDefaultCatalog(CATALOG);
        request.setDefaultDatabase(DATABASE);
        request.setUser(user.getQualifiedUser());
        request.setCurrentUserIdent(user.toThrift());
        request.setSql(sql);
        return new MysqlConnectProcessor(ConnectContext.forMysqlProxy("lookup-failure-session")).proxyExecute(request);
    }

    public static class FailingProvider implements TestExternalCatalog.TestCatalogProvider {
        private static boolean unavailable;
        private static int lookupCount;

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            lookupCount++;
            if (unavailable) {
                throw new IllegalStateException(FAILURE);
            }
            return Map.of(DATABASE, Map.of("t", List.of(new Column("id", Type.INT))));
        }
    }

    private static class RecordingChannel extends DummyMysqlChannel {
        private ByteBuffer request;
        private ByteBuffer response;

        @Override
        public ByteBuffer fetchOnePacket() {
            return request;
        }

        @Override
        public void sendAndFlush(ByteBuffer packet) {
            response = packet.duplicate();
        }
    }
}
