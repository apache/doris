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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link PluginDrivenExternalCatalog}'s DDL overrides (createDb / dropDb /
 * dropTable) added by P4-T06c, and the cache-invalidation fix to the existing
 * createTable override.
 *
 * <p><b>Why these tests matter:</b> after the MaxCompute SPI cutover (T06b), a
 * {@code max_compute} catalog is a {@link PluginDrivenExternalCatalog} whose
 * {@code metadataOps} is always {@code null}. Without these overrides every DDL
 * would hit the base class and throw "… is not supported for catalog". These tests
 * lock in that DDL is routed to the connector SPI instead, that connector failures
 * are surfaced as {@link DdlException} (caller contract), that the SPI's missing
 * {@code ifNotExists}/{@code ifExists} semantics are enforced FE-side, and that the
 * FE metadata cache is invalidated after each op so the change is visible on the
 * same FE — exactly what the legacy {@code MaxComputeMetadataOps.afterX()} hooks did.</p>
 */
public class PluginDrivenExternalCatalogDdlRoutingTest {

    private MockedStatic<Env> mockedEnv;
    private EditLog mockEditLog;
    private Connector connector;
    private ConnectorMetadata metadata;
    private ConnectorSession session;
    private TestablePluginCatalog catalog;

    @BeforeEach
    public void setUp() {
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        session = Mockito.mock(ConnectorSession.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);

        // Construct with the real Env singleton (the constructor is Env-safe), then
        // activate the static Env mock so the DDL overrides' edit-log writes are no-ops.
        catalog = new TestablePluginCatalog(connector);
        catalog.sessionMock = session;

        Env mockEnv = Mockito.mock(Env.class);
        mockEditLog = Mockito.mock(EditLog.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getEditLog()).thenReturn(mockEditLog);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    // ==================== CREATE DATABASE ====================

    @Test
    public void testCreateDbRoutesToConnectorAndInvalidatesCache() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");

        catalog.createDb("db1", false, props);

        Mockito.verify(metadata).createDatabase(session, "db1", props);
        Mockito.verify(mockEditLog).logCreateDb(Mockito.any());
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount,
                "createDb must invalidate the catalog db-name cache (legacy afterCreateDb parity)");
    }

    @Test
    public void testCreateDbIfNotExistsShortCircuitsWhenDbExists() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.createDb("db1", true, new HashMap<>());

        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Assertions.assertEquals(0, catalog.resetMetaCacheNamesCount);
    }

    @Test
    public void testCreateDbWrapsConnectorException() {
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.createDb("db1", false, new HashMap<>()));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    // ==================== DROP DATABASE ====================

    @Test
    public void testDropDbRoutesToConnectorAndUnregisters() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.dropDb("db1", false, false);

        Mockito.verify(metadata).dropDatabase(session, "db1", false);
        Mockito.verify(mockEditLog).logDropDb(Mockito.any());
        Assertions.assertEquals("db1", catalog.unregisteredDb,
                "dropDb must remove the db from the cache (legacy afterDropDb parity)");
    }

    @Test
    public void testDropDbIfExistsWhenMissingIsNoop() throws Exception {
        catalog.dbNullableResult = null; // db not present

        catalog.dropDb("missing", true, false);

        Mockito.verify(metadata, Mockito.never()).dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        Assertions.assertNull(catalog.unregisteredDb);
    }

    @Test
    public void testDropDbMissingWithoutIfExistsThrows() {
        catalog.dbNullableResult = null;

        Assertions.assertThrows(DdlException.class, () -> catalog.dropDb("missing", false, false));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testDropDbWrapsConnectorException() {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.dropDb("db1", false, false));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    // ==================== DROP TABLE ====================

    @Test
    public void testDropTableResolvesHandleRoutesAndUnregisters() throws Exception {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "db1", "t1")).thenReturn(Optional.of(handle));
        ExternalDatabase<? extends ExternalTable> mockDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(mockDb);

        catalog.dropTable("db1", "t1", false, false, false, false, false, false);

        Mockito.verify(metadata).dropTable(session, handle);
        Mockito.verify(mockEditLog).logDropTable(Mockito.any());
        Mockito.verify(mockDb).unregisterTable("t1");
    }

    @Test
    public void testDropTableIfExistsWhenMissingIsNoop() throws Exception {
        Mockito.when(metadata.getTableHandle(session, "db1", "missing")).thenReturn(Optional.empty());

        catalog.dropTable("db1", "missing", false, false, false, true, false, false);

        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logDropTable(Mockito.any());
    }

    @Test
    public void testDropTableMissingWithoutIfExistsThrows() {
        Mockito.when(metadata.getTableHandle(session, "db1", "missing")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "missing", false, false, false, false, false, false));
        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
    }

    @Test
    public void testDropTableWrapsConnectorException() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "db1", "t1")).thenReturn(Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropTable(session, handle);

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "t1", false, false, false, false, false, false));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    // ==================== CREATE TABLE (cache-invalidation fix) ====================

    @Test
    public void testCreateTableInvalidatesDbCache() throws UserException {
        ExternalDatabase<? extends ExternalTable> mockDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(mockDb);

        try (MockedStatic<CreateTableInfoToConnectorRequestConverter> conv =
                Mockito.mockStatic(CreateTableInfoToConnectorRequestConverter.class)) {
            ConnectorCreateTableRequest req = Mockito.mock(ConnectorCreateTableRequest.class);
            conv.when(() -> CreateTableInfoToConnectorRequestConverter.convert(Mockito.any(), Mockito.any()))
                    .thenReturn(req);
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            Mockito.when(info.getDbName()).thenReturn("db1");
            Mockito.when(info.getTableName()).thenReturn("t1");

            catalog.createTable(info);

            Mockito.verify(metadata).createTable(session, req);
            Mockito.verify(mockDb).resetMetaCacheNames();
        }
    }

    // ==================== helpers ====================

    @SuppressWarnings("unchecked")
    private ExternalDatabase<? extends ExternalTable> mockExternalDatabase() {
        return (ExternalDatabase<? extends ExternalTable>) Mockito.mock(ExternalDatabase.class);
    }

    /**
     * Testable subclass: injects a mock connector, neutralizes init machinery, and
     * makes the FE-cache hooks observable so DDL routing + cache invalidation can be
     * asserted without a full Doris environment.
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        ConnectorSession sessionMock;
        ExternalDatabase<? extends ExternalTable> dbNullableResult;
        Optional<ExternalDatabase<? extends ExternalTable>> dbForReplayResult = Optional.empty();
        int resetMetaCacheNamesCount;
        String unregisteredDb;

        TestablePluginCatalog(Connector initial) {
            super(1L, "test-catalog", null, testProps(), "", initial);
            this.initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // no-op: connector is injected via constructor; skip txn-manager/auth setup.
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return sessionMock;
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
            return dbNullableResult;
        }

        @Override
        public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
            return dbForReplayResult;
        }

        @Override
        public void resetMetaCacheNames() {
            resetMetaCacheNamesCount++;
        }

        @Override
        public void unregisterDatabase(String dbName) {
            unregisteredDb = dbName;
        }

        private static Map<String, String> testProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "test");
            return props;
        }
    }
}
