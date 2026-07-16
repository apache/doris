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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests that {@link PluginDrivenExternalTable} returns the correct legacy engine
 * names and table type names for migrated JDBC/ES catalogs, preserving
 * user-visible compatibility across metadata surfaces (SHOW TABLE STATUS,
 * information_schema.tables, REST API, etc.).
 */
public class PluginDrivenExternalTableEngineTest {

    @Test
    public void testJdbcCatalogReturnsJdbcEngineName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc");
        Assertions.assertEquals("jdbc", table.getEngine(),
                "JDBC catalog tables should report engine='jdbc'");
    }

    @Test
    public void testEsCatalogReturnsEsEngineName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("es");
        Assertions.assertEquals("es", table.getEngine(),
                "ES catalog tables should report engine='es'");
    }

    @Test
    public void testUnknownCatalogReturnsPluginEngineName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("custom_type");
        Assertions.assertEquals("Plugin", table.getEngine(),
                "Unknown catalog types should report engine='Plugin'");
    }

    @Test
    public void testJdbcCatalogReturnsJdbcEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc");
        Assertions.assertEquals(TableType.JDBC_EXTERNAL_TABLE.name(),
                table.getEngineTableTypeName(),
                "JDBC catalog tables should report JDBC_EXTERNAL_TABLE type name");
    }

    @Test
    public void testEsCatalogReturnsEsEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("es");
        Assertions.assertEquals(TableType.ES_EXTERNAL_TABLE.name(),
                table.getEngineTableTypeName(),
                "ES catalog tables should report ES_EXTERNAL_TABLE type name");
    }

    @Test
    public void testUnknownCatalogReturnsPluginEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("custom_type");
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE.name(),
                table.getEngineTableTypeName(),
                "Unknown catalog types should report PLUGIN_EXTERNAL_TABLE type name");
    }

    @Test
    public void testTableTypeIsAlwaysPluginExternalTable() {
        PluginDrivenExternalTable jdbcTable = createTableWithCatalogType("jdbc");
        PluginDrivenExternalTable esTable = createTableWithCatalogType("es");
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE, jdbcTable.getType(),
                "Internal table type should always be PLUGIN_EXTERNAL_TABLE");
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE, esTable.getType(),
                "Internal table type should always be PLUGIN_EXTERNAL_TABLE");
    }

    @Test
    public void testGsonPostProcessRestoresMissingRemoteName() throws Exception {
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc");
        table.setRemoteName(null);

        table.gsonPostProcess();

        Assertions.assertEquals("test_table", table.remoteName,
                "Deserialized external tables should materialize the effective remote table name");
    }

    @Test
    public void testInitSchemaReturnsEmptyWhenTableHandleMissing() {
        Connector connector = createMockConnector(false, false);
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc", connector);

        // Return an empty schema result when the connector cannot resolve the table handle.
        Assertions.assertFalse(table.initSchema().isPresent(),
                "Missing connector table handles should produce an empty schema result");
    }

    @Test
    public void testInitSchemaAppliesRemoteColumnNameMapping() {
        Connector connector = createMockConnector(true, true);
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc", connector);

        // Verify that plugin-driven schema loading preserves mapped column names from the connector.
        Optional<SchemaCacheValue> schema = table.initSchema();
        Assertions.assertTrue(schema.isPresent(), "Schema should be present when a table handle exists");
        Assertions.assertEquals("mapped_id", schema.get().getSchema().get(0).getName(),
                "Mapped remote column names should be reflected in Doris schema metadata");
    }

    // -------- Helpers --------

    private PluginDrivenExternalTable createTableWithCatalogType(String catalogType) {
        return createTableWithCatalogType(catalogType, createMockConnector(true, false));
    }

    private PluginDrivenExternalTable createTableWithCatalogType(String catalogType, Connector connector) {
        TestablePluginCatalog catalog = new TestablePluginCatalog(catalogType, connector);
        ExternalDatabase<PluginDrivenExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getFullName()).thenReturn("test_db");
        Mockito.when(db.getRemoteName()).thenReturn("test_db");

        PluginDrivenExternalTable table = new PluginDrivenExternalTable(
                1L, "test_table", "test_table", catalog, db);
        return table;
    }

    private Connector createMockConnector(boolean tableExists, boolean renameColumn) {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableSchema schema = new ConnectorTableSchema("test_table",
                Collections.singletonList(new ConnectorColumn(
                        "id", ConnectorType.of("INT", -1, -1), "", true, null, true)),
                null, Collections.emptyMap());
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(Mockito.any(ConnectorSession.class), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(tableExists ? Optional.of(handle) : Optional.empty());
        Mockito.when(metadata.getTableSchema(Mockito.any(ConnectorSession.class), Mockito.eq(handle))).thenReturn(schema);
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(ConnectorSession.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> {
                    String remoteName = invocation.getArgument(3);
                    return renameColumn ? "mapped_" + remoteName : remoteName;
                });
        return connector;
    }

    @SuppressWarnings("unchecked")
    private ExternalDatabase<PluginDrivenExternalTable> mockExternalDatabase() {
        return Mockito.mock(ExternalDatabase.class);
    }

    /**
     * Minimal testable PluginDrivenExternalCatalog that returns a configurable type
     * without requiring full Doris environment initialization.
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final String catalogType;

        TestablePluginCatalog(String catalogType, Connector connector) {
            super(1L, "test-catalog", null, makeProps(catalogType), "", connector);
            this.catalogType = catalogType;
        }

        @Override
        public String getType() {
            return catalogType;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        private static Map<String, String> makeProps(String type) {
            Map<String, String> props = new HashMap<>();
            props.put("type", type);
            return props;
        }
    }
}
