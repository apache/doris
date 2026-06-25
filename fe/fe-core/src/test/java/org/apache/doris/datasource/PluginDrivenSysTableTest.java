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
import org.apache.doris.datasource.systable.PluginDrivenSysTable;
import org.apache.doris.datasource.systable.SysTable;

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for the generic plugin-driven system-table machinery (T18): {@link PluginDrivenSysTable},
 * {@link PluginDrivenSysExternalTable}, and {@link PluginDrivenExternalTable#getSupportedSysTables()}.
 *
 * <p><b>Why this matters:</b> plugin-driven external tables must expose connector system tables
 * (e.g. {@code cat.db.tbl$snapshots}) by REUSING the live fe-core system-table machinery
 * ({@code TableIf.findSysTable} + {@code NativeSysTable.createSysExternalTable} +
 * {@code SysTableResolver}), delegating the connector-specific bits (which sys tables exist, how to
 * obtain a sys handle) to the SPI. The discovery must be GENERIC (driven by the connector SPI, not
 * hardcoded per connector) and a system-table query must read the SYSTEM table, not the base table.</p>
 */
public class PluginDrivenSysTableTest {

    // ==================== getSupportedSysTables() delegates to the connector SPI ====================

    @Test
    public void testGetSupportedSysTablesDelegatesToConnector() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.listSupportedSysTables(session, baseHandle))
                .thenReturn(Arrays.asList("snapshots", "binlog"));

        PluginDrivenExternalTable table = bareTable(catalog, db, "REMOTE_TBL");
        Map<String, SysTable> sysTables = table.getSupportedSysTables();

        // WHY: discovery must come from the connector SPI (listSupportedSysTables), keyed by the
        // bare name so the inherited findSysTable exact-match resolves. MUTATION: returning
        // Collections.emptyMap() (ignoring the SPI) makes both keys absent -> red.
        Assertions.assertEquals(2, sysTables.size());
        Assertions.assertTrue(sysTables.containsKey("snapshots"), "must expose 'snapshots' from the SPI");
        Assertions.assertTrue(sysTables.containsKey("binlog"), "must expose 'binlog' from the SPI");
        Assertions.assertTrue(sysTables.get("snapshots") instanceof PluginDrivenSysTable,
                "each value must be a generic PluginDrivenSysTable, not a connector-specific subtype");
        Mockito.verify(metadata).listSupportedSysTables(session, baseHandle);
    }

    @Test
    public void testGetSupportedSysTablesEmptyWhenNoBaseHandle() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.empty());

        PluginDrivenExternalTable table = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        Assertions.assertTrue(table.getSupportedSysTables().isEmpty(),
                "with no base handle there is nothing to query for sys tables");
        Mockito.verify(metadata, Mockito.never())
                .listSupportedSysTables(Mockito.any(), Mockito.any());
    }

    // ==================== findSysTable (inherited TableIf default) ====================

    @Test
    public void testFindSysTableResolvesBySuffix() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.listSupportedSysTables(session, baseHandle))
                .thenReturn(Arrays.asList("snapshots", "binlog"));

        PluginDrivenExternalTable table = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");

        Optional<SysTable> hit = table.findSysTable("t$snapshots");
        Assertions.assertTrue(hit.isPresent(), "t$snapshots must resolve to the 'snapshots' SysTable");
        Assertions.assertEquals("snapshots", hit.get().getSysTableName());
        // WHY: findSysTable does an exact, case-sensitive map.get of the suffix; an unknown suffix
        // must miss. MUTATION: returning the whole map regardless of suffix would make 'nope' present.
        Assertions.assertFalse(table.findSysTable("t$nope").isPresent(),
                "an unknown system-table suffix must not resolve");
    }

    // ==================== createSysExternalTable: type + name + sibling delegation ====================

    @Test
    public void testCreateSysExternalTableReportsPluginTypeAndName() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.listSupportedSysTables(session, baseHandle))
                .thenReturn(Collections.singletonList("snapshots"));

        PluginDrivenExternalTable base = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        PluginDrivenSysTable sysType = new PluginDrivenSysTable("snapshots");
        ExternalTable sysTable = sysType.createSysExternalTable(base);

        Assertions.assertTrue(sysTable instanceof PluginDrivenSysExternalTable);
        // WHY (explicit guard "勿报 PAIMON_EXTERNAL_TABLE"): the generic sys table must inherit the
        // PLUGIN_EXTERNAL_TABLE type and MUST NOT report any connector-specific type. MUTATION: a ctor
        // that passes (e.g.) PAIMON_EXTERNAL_TABLE to super makes getType() != PLUGIN_EXTERNAL_TABLE -> red.
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE, sysTable.getType(),
                "sys table must report PLUGIN_EXTERNAL_TABLE, not a connector-specific type");
        Assertions.assertEquals("tbl$snapshots", sysTable.getName(),
                "sys table name must be base name + '$' + sysName (planner-visible name)");
        Assertions.assertEquals("REMOTE_TBL$snapshots", sysTable.getRemoteName(),
                "sys table remote name must be base remote name + '$' + sysName");
        // getSupportedSysTables delegates to the source so DESCRIBE/SHOW on a sys table lists siblings.
        Assertions.assertTrue(sysTable.getSupportedSysTables().containsKey("snapshots"),
                "sys table getSupportedSysTables must delegate to the source table (sibling listing)");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sysType.createSysExternalTable(Mockito.mock(ExternalTable.class)),
                "createSysExternalTable must reject non-PluginDrivenExternalTable sources");
    }

    // ==================== handle threading: sys query reads the SYS table, not the base ===========

    @Test
    public void testSysTableThreadsSysHandleNotBaseHandle() {
        // Mock getTableHandle -> a BASE handle; getSysTableHandle -> a DISTINCT sys handle.
        // Driving initSchema on the sys table must read schema via the SYS handle, proving the sys
        // query reads the system table, not the base. This is the whole point of T18.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle sysHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");

        // Base handle resolved from the SOURCE remote name (not the "$"-suffixed sys remote name).
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.getSysTableHandle(session, baseHandle, "snapshots"))
                .thenReturn(Optional.of(sysHandle));
        ConnectorTableSchema sysSchema = new ConnectorTableSchema(
                "REMOTE_TBL$snapshots",
                Collections.singletonList(new ConnectorColumn("snapshot_id", ConnectorType.of("BIGINT"), "", true, null)),
                "paimon",
                Collections.emptyMap());
        Mockito.when(metadata.getTableSchema(session, sysHandle)).thenReturn(sysSchema);
        Mockito.when(metadata.fromRemoteColumnName(Mockito.eq(session), Mockito.anyString(),
                        Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(inv -> inv.getArgument(3));

        PluginDrivenExternalTable base = bareTable(catalog, db, "REMOTE_TBL");
        PluginDrivenSysExternalTable sysTable = new PluginDrivenSysExternalTable(base, "snapshots") {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }
        };

        Optional<SchemaCacheValue> result = sysTable.initSchema();

        Assertions.assertTrue(result.isPresent());
        // WHY: the sys handle (NOT the base handle) must be what flows into getTableSchema, so a sys
        // query reads the system table's schema. MUTATION: an override that returned the base handle
        // (skipping getSysTableHandle) would call getTableSchema(session, baseHandle) -> these verify
        // assertions go red.
        Mockito.verify(metadata).getSysTableHandle(session, baseHandle, "snapshots");
        Mockito.verify(metadata).getTableSchema(session, sysHandle);
        Mockito.verify(metadata, Mockito.never()).getTableSchema(session, baseHandle);
    }

    @Test
    public void testSysTableEmptyWhenBaseHandleMissing() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("paimon", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.empty());

        PluginDrivenExternalTable base = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        PluginDrivenSysExternalTable sysTable = new PluginDrivenSysExternalTable(base, "snapshots") {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op
            }
        };

        Assertions.assertFalse(sysTable.initSchema().isPresent(),
                "no base handle -> no sys handle -> empty schema (no spurious getSysTableHandle)");
        Mockito.verify(metadata, Mockito.never())
                .getSysTableHandle(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    // ==================== iceberg sys table: user-visible type/engine parity (T07 gap-fill) =========

    @Test
    public void sysExternalTableReportsBaseTableMysqlTypeMatchingLegacyIceberg() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("iceberg", metadata, session);
        PluginDrivenExternalTable base = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(base, "snapshots");

        // WHY: information_schema.tables.TABLE_TYPE for an iceberg sys table (e.g. tbl$snapshots) must read
        // "BASE TABLE", byte-identical to a legacy ICEBERG_EXTERNAL_TABLE. The sys table inherits
        // PLUGIN_EXTERNAL_TABLE and routes getMysqlType -> TableType.toMysqlType; the same-test pin of
        // ICEBERG_EXTERNAL_TABLE.toMysqlType proves new == legacy with no Env. MUTATION: deleting the
        // PLUGIN_EXTERNAL_TABLE case in TableIf.TableType.toMysqlType -> sys getMysqlType returns null -> red.
        Assertions.assertEquals("BASE TABLE", sys.getMysqlType());
        Assertions.assertEquals("BASE TABLE", TableType.ICEBERG_EXTERNAL_TABLE.toMysqlType(),
                "the new plugin sys path must match the legacy iceberg TABLE_TYPE");
    }

    @Test
    public void sysExternalTableReportsIcebergEngineAndEngineTableTypeName() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("iceberg", metadata, session);
        PluginDrivenExternalTable base = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        PluginDrivenSysExternalTable sys = new PluginDrivenSysExternalTable(base, "snapshots");

        // WHY: SHOW TABLE STATUS / information_schema.tables.ENGINE for an iceberg sys table must read
        // "iceberg" (not the generic "Plugin"), and getEngineTableTypeName must read "ICEBERG_EXTERNAL_TABLE".
        // The sys table inherits both from PluginDrivenExternalTable, which switches on the catalog type;
        // T06-F1 pinned the BASE table, this pins the inherited SYS path. MUTATION: deleting the "iceberg"
        // case in PluginDrivenExternalTable.getEngine / getEngineTableTypeName -> "Plugin" /
        // "PLUGIN_EXTERNAL_TABLE" -> red. assertAll so each pin (two independent T06 behaviors) is caught
        // by its own mutation rather than masked by the other's short-circuit.
        Assertions.assertAll(
                () -> Assertions.assertEquals("iceberg", sys.getEngine()),
                () -> Assertions.assertEquals("ICEBERG_EXTERNAL_TABLE", sys.getEngineTableTypeName()));
    }

    // ==================== generic not-found path (no legacy position_deletes marker) ================

    @Test
    public void positionDeletesAbsentWhenConnectorDoesNotListIt() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("iceberg", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        // An iceberg-style supported list (MetadataTableType.values() lower-cased) but WITHOUT
        // position_deletes — exactly what IcebergConnectorMetadata.listSupportedSysTables returns.
        Mockito.when(metadata.listSupportedSysTables(session, baseHandle))
                .thenReturn(Arrays.asList("snapshots", "history", "files", "manifests", "partitions"));

        PluginDrivenExternalTable table = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        Map<String, SysTable> sysTables = table.getSupportedSysTables();

        // Positive control: a listed name resolves, proving the SPI-delegated machinery works (so the
        // negatives below are not trivially green because discovery happens to be empty).
        Assertions.assertTrue(sysTables.containsKey("snapshots"));
        Assertions.assertTrue(table.findSysTable("t$snapshots").isPresent());
        // WHY: position_deletes is the one metadata table iceberg does not expose; legacy modeled it as a
        // special UNSUPPORTED_POSITION_DELETES_TABLE that threw "not supported yet". The generic plugin path
        // has no such marker — an unlisted sys name simply does not resolve via the ordinary not-found path.
        // MUTATION: injecting "position_deletes" into getSupportedSysTables regardless of the SPI list ->
        // containsKey true / findSysTable present -> red.
        Assertions.assertFalse(sysTables.containsKey("position_deletes"),
                "position_deletes must not be exposed when the connector does not list it");
        Assertions.assertFalse(table.findSysTable("t$position_deletes").isPresent(),
                "t$position_deletes must take the generic not-found path, not a legacy 'unsupported' marker");
    }

    // ==================== sys tables are transient: not registered, not edit-log serialized ========

    @Test
    public void sysExternalTableIsTransientNeitherRegisteredNorSerialized() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle baseHandle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("iceberg", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(baseHandle));
        Mockito.when(metadata.listSupportedSysTables(session, baseHandle))
                .thenReturn(Arrays.asList("snapshots", "files"));

        PluginDrivenExternalTable base = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        PluginDrivenSysTable sysType = new PluginDrivenSysTable("snapshots");
        PluginDrivenSysExternalTable sys = (PluginDrivenSysExternalTable) sysType.createSysExternalTable(base);

        // The planner-visible sys NAME carries the "$" suffix (so a query against tbl$snapshots routes here)...
        Assertions.assertEquals("REMOTE_TBL$snapshots", sys.getRemoteName());
        // ...but the discovery map (the basis for SHOW TABLES sys-listing) is keyed by BARE names only: a
        // "$"-suffixed key must never appear, or a sys table would leak into SHOW TABLES as a real table.
        // MUTATION: keying getSupportedSysTables by "$" + name -> a "$"-key appears -> red.
        for (String key : base.getSupportedSysTables().keySet()) {
            Assertions.assertFalse(key.contains("$"),
                    "sys discovery keys must be bare names, never '$'-suffixed: " + key);
        }
        // And the transient sys ExternalTable must never be GSON-serialized into the edit log: none of its
        // OWN declared fields may carry @SerializedName (it is rebuilt per query, never persisted/replayed).
        // MUTATION: annotating any PluginDrivenSysExternalTable field with @SerializedName -> red.
        Field[] declared = PluginDrivenSysExternalTable.class.getDeclaredFields();
        Assertions.assertTrue(declared.length > 0, "guard has teeth: the sys class does declare fields");
        for (Field f : declared) {
            Assertions.assertFalse(f.isAnnotationPresent(SerializedName.class),
                    "transient sys table must not serialize field: " + f.getName());
        }
    }

    // ==================== helpers (mirror PluginDrivenExternalTablePartitionTest) ====================

    /** Table that drives the real getSupportedSysTables()/initSchema(); does not stub the schema cache. */
    private static PluginDrivenExternalTable bareTable(PluginDrivenExternalCatalog catalog,
            ExternalDatabase<PluginDrivenExternalTable> db, String remoteName) {
        return new PluginDrivenExternalTable(1L, "tbl", remoteName, catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<PluginDrivenExternalTable> mockDb(String remoteName) {
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn(remoteName);
        return db;
    }

    /**
     * Minimal PluginDrivenExternalCatalog that returns a fixed connector/session without standing up
     * the Doris environment (mirrors PluginDrivenExternalTablePartitionTest.TestablePluginCatalog).
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestablePluginCatalog(String catalogType, ConnectorMetadata metadata, ConnectorSession session) {
            this(catalogType, mockConnector(metadata, session), session);
        }

        private TestablePluginCatalog(String catalogType, Connector connector, ConnectorSession session) {
            super(1L, "test-catalog", null, makeProps(catalogType), "", connector);
            this.connector = connector;
            this.session = session;
        }

        private static Connector mockConnector(ConnectorMetadata metadata, ConnectorSession session) {
            Connector c = Mockito.mock(Connector.class);
            Mockito.when(c.getMetadata(session)).thenReturn(metadata);
            return c;
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return session;
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
