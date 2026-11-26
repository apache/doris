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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * What a plugin-driven table answers when asked for its engine name — the {@code ENGINE} column of
 * {@code SHOW TABLE STATUS} and {@code information_schema.tables}, and the string {@code SHOW CREATE TABLE}
 * prints after {@code ENGINE=}.
 *
 * <p>The engine holds no mapping from data source to displayed name: the name comes from the connector's
 * provider, which defaults it to the catalog type. These tests pin that — a catalog type the engine has never
 * heard of gets its own name, and a connector that spells its name differently from its type is obeyed.
 */
public class PluginDrivenExternalTableEngineTest {

    @BeforeEach
    void setUp() {
        // The plugin manager is a static singleton shared with every other test in the fork: start from a known
        // empty one rather than inheriting whatever the previous test class registered.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @AfterEach
    void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

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
    public void testMaxComputeCatalogReturnsTheNameItsConnectorDeclares() {
        // The one connector whose displayed name is not its catalog type: the type a user writes is
        // "max_compute", the product is spelled "maxcompute". Only the provider knows that, which is the whole
        // point — the engine cannot special-case it. MUTATION: dropping the provider's displayEngineName()
        // override -> "max_compute" -> red.
        registerProvider("max_compute", "maxcompute");
        PluginDrivenExternalTable table = createTableWithCatalogType("max_compute");
        Assertions.assertEquals("maxcompute", table.getEngine(),
                "MaxCompute catalog tables should report the name its connector declares");
    }

    @Test
    public void testUnknownCatalogReturnsItsCatalogType() {
        // No provider registered for this type at all, which is also what a catalog whose plugin was uninstalled
        // looks like: the name falls back to the catalog type rather than becoming a generic placeholder or
        // throwing. It must NOT be a fixed string — that would mean the engine still owns the mapping.
        PluginDrivenExternalTable table = createTableWithCatalogType("custom_type");
        Assertions.assertEquals("custom_type", table.getEngine(),
                "A catalog type with no registered provider should report its own type as the engine");
    }

    @Test
    public void testJdbcCatalogReturnsJdbcEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("jdbc");
        Assertions.assertEquals("jdbc", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on a JDBC catalog table should print ENGINE=jdbc");
    }

    @Test
    public void testEsCatalogReturnsEsEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("es");
        Assertions.assertEquals("es", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on an ES catalog table should print ENGINE=es");
    }

    @Test
    public void testMaxComputeCatalogReturnsMaxComputeEngineTableTypeName() {
        registerProvider("max_compute", "maxcompute");
        PluginDrivenExternalTable table = createTableWithCatalogType("max_compute");
        Assertions.assertEquals("maxcompute", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on a MaxCompute catalog table should print ENGINE=maxcompute");
    }

    @Test
    public void testUnknownCatalogReturnsItsCatalogTypeAsEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("custom_type");
        Assertions.assertEquals("custom_type", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on a catalog with no provider should print its catalog type");
    }

    @Test
    public void testIcebergCatalogReturnsIcebergEngineName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("iceberg");
        Assertions.assertEquals("iceberg", table.getEngine(),
                "Iceberg catalog tables should report engine='iceberg'");
    }

    @Test
    public void testIcebergCatalogReturnsIcebergEngineTableTypeName() {
        PluginDrivenExternalTable table = createTableWithCatalogType("iceberg");
        Assertions.assertEquals("iceberg", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on an iceberg catalog table should print ENGINE=iceberg");
    }

    @Test
    public void testHmsCatalogReturnsHmsEngineName() {
        // An HMS catalog displays "hms", NOT the "hive" its connector accepts in CREATE TABLE ... ENGINE=.
        // Displaying and accepting are answered by two different provider methods precisely because a
        // connector may need them to differ; this is the one that does.
        registerProvider("hms", "hms");
        PluginDrivenExternalTable table = createTableWithCatalogType("hms");
        Assertions.assertEquals("hms", table.getEngine(),
                "Hms catalog tables should report engine='hms', not the accepted CREATE TABLE engine 'hive'");
    }

    @Test
    public void testHmsCatalogReturnsHmsEngineTableTypeName() {
        registerProvider("hms", "hms");
        PluginDrivenExternalTable table = createTableWithCatalogType("hms");
        Assertions.assertEquals("hms", table.getEngineTableTypeName(),
                "SHOW CREATE TABLE on an HMS catalog table should print ENGINE=hms");
    }

    @Test
    public void testEngineNameComesFromTheConnectorNotFromAnyEngineSideTable() {
        // The regression guard for the whole change: a catalog type fe-core has never heard of, whose connector
        // spells its displayed name differently again. Nothing in the engine could produce this answer from a
        // hardcoded list. MUTATION: resolving the name from the catalog type instead of the provider -> red.
        registerProvider("acme-lake", "AcmeLake");
        PluginDrivenExternalTable table = createTableWithCatalogType("acme-lake");
        Assertions.assertEquals("AcmeLake", table.getEngine(),
                "the displayed engine name must be whatever the connector's provider says it is");
    }

    @Test
    public void testBothEngineNamesAlwaysAgree() {
        // One connector, one engine name, however the user reaches it: SHOW TABLE STATUS and SHOW CREATE TABLE
        // must never disagree. MUTATION: giving getEngineTableTypeName() its own derivation -> red.
        registerProvider("max_compute", "maxcompute");
        for (String catalogType : new String[] {"jdbc", "iceberg", "max_compute", "custom_type"}) {
            PluginDrivenExternalTable table = createTableWithCatalogType(catalogType);
            Assertions.assertEquals(table.getEngine(), table.getEngineTableTypeName(),
                    "the ENGINE column and the ENGINE= clause must show the same name for " + catalogType);
        }
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

    /**
     * Verifies the fe-core call site of {@link PluginDrivenExternalTable#toThrift()}: it must pass
     * the REMOTE db/table names and the schema column count into
     * {@code ConnectorMetadata.buildTableDescriptor(...)}.
     *
     * <p>WHY this matters: after the max_compute cutover, BE static_casts the descriptor to
     * {@code MaxComputeTableDescriptor} and reads {@code project}/{@code table} (built by
     * {@code MaxComputeConnectorMetadata.buildTableDescriptor} from these two args) as the JNI
     * read-session addressing contract, which uses REMOTE names. If the call site passed the LOCAL
     * names (or a wrong numCols), the descriptor would address the wrong ODPS project/table and the
     * column count would be inconsistent with the schema, breaking reads. The connector-module UT
     * ({@code MaxComputeBuildTableDescriptorTest}) only covers the override's own output; this test
     * is the only automated guard on the cross-module WIRING.
     *
     * <p>It FAILS if the call site is changed to pass {@code db.getFullName()}/{@code getName()}
     * (local names) or any column count other than {@code schema.size()}.
     */
    @Test
    public void testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor() {
        ConnectorMetadata meta = Mockito.mock(ConnectorMetadata.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("max_compute", meta);

        // Local names differ from remote names, so a regression that passes local names is caught.
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("mydb");
        Mockito.when(db.getRemoteName()).thenReturn("REMOTE_DB");

        // Schema with a known, non-trivial column count so numCols regressions are caught.
        final int expectedNumCols = 3;
        final List<Column> schema = new ArrayList<>();
        for (int i = 0; i < expectedNumCols; i++) {
            schema.add(new Column("c" + i, PrimitiveType.INT));
        }

        // Subclass stubs ONLY the two Env-backed methods toThrift() traverses (catalog/db init and
        // schema-cache lookup), isolating the call-site wiring without standing up Env/CatalogMgr.
        PluginDrivenExternalTable table = new PluginDrivenExternalTable(
                1L, "mytbl", "REMOTE_TBL", catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip real catalog/db initialization (Env-backed)
            }

            @Override
            public List<Column> getFullSchema() {
                return schema;
            }
        };

        TTableDescriptor stub = new TTableDescriptor(1L, TTableType.MAX_COMPUTE_TABLE,
                expectedNumCols, 0, "mytbl", "REMOTE_DB");
        Mockito.when(meta.buildTableDescriptor(
                        Mockito.any(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.anyString(), Mockito.anyInt(), Mockito.anyLong()))
                .thenReturn(stub);

        table.toThrift();

        ArgumentCaptor<String> dbNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> remoteNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> numColsCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.verify(meta).buildTableDescriptor(
                Mockito.any(ConnectorSession.class), Mockito.anyLong(), Mockito.anyString(),
                dbNameCaptor.capture(), remoteNameCaptor.capture(),
                numColsCaptor.capture(), Mockito.anyLong());

        Assertions.assertEquals("REMOTE_DB", dbNameCaptor.getValue(),
                "toThrift() must pass db.getRemoteName() as dbName, not the local db name");
        Assertions.assertEquals("REMOTE_TBL", remoteNameCaptor.getValue(),
                "toThrift() must pass table.getRemoteName() as remoteName, not the local table name");
        Assertions.assertEquals(expectedNumCols, numColsCaptor.getValue().intValue(),
                "toThrift() must pass schema.size() as numCols");
    }

    // -------- Helpers --------

    /** Registers a single fake provider claiming {@code type} and displaying {@code displayName}. */
    private static void registerProvider(String type, String displayName) {
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public String displayEngineName() {
                return displayName;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                throw new UnsupportedOperationException(
                        "the displayed engine name must be answered without building a connector");
            }
        });
        ConnectorFactory.initPluginManager(manager);
    }

    private PluginDrivenExternalTable createTableWithCatalogType(String catalogType) {
        return createTableWithCatalogType(catalogType, createMockConnector(true, false));
    }

    private PluginDrivenExternalTable createTableWithCatalogType(String catalogType, Connector connector) {
        TestablePluginCatalog catalog = new TestablePluginCatalog(catalogType, connector);
        ExternalDatabase<PluginDrivenExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getFullName()).thenReturn("test_db");
        Mockito.when(db.getRemoteName()).thenReturn("test_db");

        // Unit isolation: these tests exercise engine-name / initSchema() logic against mock
        // connector/db objects that are not registered in a real Env-backed catalog. Since P6.6 H-8
        // (iceberg view schema), initSchema() consults isView() -> makeSureInitialized(); the real
        // makeSureInitialized() would resolve the db against Env and throw "Unknown database 'test_db'".
        // Stub it out (mirrors testToThrift...'s inline no-op); isView() then stays false (the view-less
        // connector path these tests assert), so initSchema() takes the table-handle path unchanged.
        PluginDrivenExternalTable table = new PluginDrivenExternalTable(
                1L, "test_table", "test_table", catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip real Env-backed catalog/db initialization
            }
        };
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
        private final Connector connector;

        TestablePluginCatalog(String catalogType) {
            this(catalogType, mockConnector(Mockito.mock(ConnectorMetadata.class)));
        }

        TestablePluginCatalog(String catalogType, ConnectorMetadata meta) {
            this(catalogType, mockConnector(meta));
        }

        private TestablePluginCatalog(String catalogType, Connector connector) {
            super(1L, "test-catalog", null, makeProps(catalogType), "", connector);
            this.catalogType = catalogType;
            this.connector = connector;
        }

        @Override
        public String getType() {
            return catalogType;
        }

        @Override
        public Connector getConnector() {
            // Bypass the parent's makeSureInitialized() (Env-backed catalog init) so the call-site
            // wiring test can reach toThrift() without standing up Env/CatalogMgr.
            return connector;
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

        private static Connector mockConnector(ConnectorMetadata meta) {
            Connector c = Mockito.mock(Connector.class);
            Mockito.when(c.getMetadata(Mockito.any())).thenReturn(meta);
            return c;
        }
    }
}
