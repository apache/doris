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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pins {@link PluginDrivenExternalTable#getFullSchema()} request-scoped synthetic-write-column injection
 * (③ C3b-core). Post-flip, the iceberg DML row-id hidden column that legacy
 * {@code IcebergExternalTable.getFullSchema} appended is gone (a {@link PluginDrivenExternalTable} carries
 * no iceberg knowledge); the generic table must instead append whatever the connector declares through
 * {@link ConnectorWritePlanProvider#getSyntheticWriteColumns}, gated request-side by show-hidden / the
 * synthetic-write-column ctx flag. The injection is connector-agnostic (iron-law: no iceberg branch here),
 * so these tests use a generic invisible synthetic column.
 *
 * <p>Mockito {@code CALLS_REAL_METHODS} runs the real getFullSchema/needInternalHiddenColumns/fetch+append
 * over stubbed seams (schema cache, the connector chain), mirroring {@code PhysicalIcebergMergeSinkTest}.</p>
 */
public class PluginDrivenExternalTableTest {

    private static final List<Column> BASE_SCHEMA = ImmutableList.of(
            new Column("id", ScalarType.INT, true, null, true, null, ""),
            new Column("name", ScalarType.createStringType(), false, null, true, null, ""));

    /** A generic, connector-declared invisible synthetic write column (stands in for iceberg's row-id STRUCT). */
    private static final ConnectorColumn SYNTHETIC =
            new ConnectorColumn("__syn_write_col__", ConnectorType.of("BIGINT"), "", false, null, false).invisible();

    @AfterEach
    public void clearCtx() {
        ConnectContext.remove();
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable wired to a stubbed connector chain whose
     * write-plan provider returns {@code synthetic}. {@code writeProviderPresent=false} models a read-only
     * connector; {@code handlePresent=false} models an unresolvable handle.
     */
    private static PluginDrivenExternalTable pluginTable(List<ConnectorColumn> synthetic,
            boolean writeProviderPresent, boolean handlePresent) {
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Mockito.when(provider.getSyntheticWriteColumns(Mockito.any(), Mockito.any())).thenReturn(synthetic);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(handlePresent ? Optional.of(handle) : Optional.empty());
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(writeProviderPresent ? provider : null);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);

        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        SchemaCacheValue scv = Mockito.mock(SchemaCacheValue.class);
        Mockito.when(scv.getSchema()).thenReturn(BASE_SCHEMA);
        Mockito.doReturn(Optional.of(scv)).when(table).getSchemaCacheValue();
        return table;
    }

    @Test
    public void getFullSchemaAppendsConvertedSyntheticColumnsWhenGated() {
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), true, true);
        // Gate open: a DML over this table is in flight (the ctx flag drives needInternalHiddenColumns()).
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();

        List<Column> schema = table.getFullSchema();

        Assertions.assertEquals(BASE_SCHEMA.size() + 1, schema.size(),
                "the connector's synthetic write column is appended after the base schema");
        Assertions.assertEquals("id", schema.get(0).getName());
        Assertions.assertEquals("name", schema.get(1).getName());
        Column appended = schema.get(2);
        Assertions.assertEquals("__syn_write_col__", appended.getName(),
                "the appended column is the converted connector-declared synthetic write column");
        Assertions.assertFalse(appended.isVisible(),
                "the synthetic write column's invisible marker survives the SPI conversion");
    }

    @Test
    public void getFullSchemaReturnsBaseScheamWhenNotGated() {
        // MUTATION: dropping the show-hidden/ctx gate (always append) makes this red — an ordinary query
        // (no DML, no show-hidden) must see exactly the base schema, never the synthetic write column.
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), true, true);
        Mockito.doReturn(false).when(table).needInternalHiddenColumns();

        List<Column> schema = table.getFullSchema();

        Assertions.assertEquals(BASE_SCHEMA.size(), schema.size(),
                "ungated getFullSchema returns the base schema with no synthetic write column");
        Assertions.assertTrue(schema.stream().noneMatch(c -> "__syn_write_col__".equals(c.getName())));
    }

    @Test
    public void getFullSchemaReturnsBaseWhenConnectorDeclaresNoSyntheticColumns() {
        // A connector with no synthetic write columns (jdbc/es/paimon/maxcompute) keeps its byte-identical
        // full schema even while gated.
        PluginDrivenExternalTable table = pluginTable(Collections.emptyList(), true, true);
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();

        Assertions.assertEquals(BASE_SCHEMA.size(), table.getFullSchema().size());
    }

    @Test
    public void getFullSchemaDegradesWhenWriteProviderAbsent() {
        // MUTATION: dropping the null-write-provider guard throws NPE here — a read-only connector
        // (getWritePlanProvider()==null) must degrade to the base schema, never fail schema resolution.
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), false, true);
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();

        Assertions.assertEquals(BASE_SCHEMA.size(), table.getFullSchema().size());
    }

    @Test
    public void getFullSchemaDegradesWhenTableHandleAbsent() {
        // MUTATION: dropping the absent-handle guard NPEs on handleOpt.get() — an unresolvable table handle
        // must degrade to the base schema.
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), true, false);
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();

        Assertions.assertEquals(BASE_SCHEMA.size(), table.getFullSchema().size());
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable whose connector declares exactly
     * {@code capabilities}, to exercise the capability-helper methods over the real connector chain.
     */
    private static PluginDrivenExternalTable pluginTableWithCapabilities(Set<ConnectorCapability> capabilities) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    @Test
    public void supportsColumnAutoAnalyzeReflectsConnectorCapability() {
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE)).supportsColumnAutoAnalyze());
        // The two capabilities are independent: declaring auto-analyze must NOT enable lazy top-N.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE)).supportsTopNLazyMaterialize());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsColumnAutoAnalyze());
    }

    @Test
    public void supportsTopNLazyMaterializeReflectsConnectorCapability() {
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE)).supportsTopNLazyMaterialize());
        // Independent the other way too: declaring lazy top-N must NOT enable auto-analyze.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE)).supportsColumnAutoAnalyze());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsTopNLazyMaterialize());
    }

    @Test
    public void capabilityHelpersReturnFalseWhenConnectorAbsent() {
        // MUTATION: dropping the null-connector guard NPEs here — a catalog with no connector (read-only /
        // not-yet-initialized) must degrade to "capability absent", never crash planning.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Assertions.assertFalse(table.supportsColumnAutoAnalyze());
        Assertions.assertFalse(table.supportsTopNLazyMaterialize());
        Assertions.assertFalse(table.supportsShowCreateDdl());
        Assertions.assertFalse(table.supportsView());
    }

    @Test
    public void supportsShowCreateDdlReflectsConnectorCapability() {
        // The SHOW CREATE TABLE plugin arm renders LOCATION/PROPERTIES/clauses only when this is true.
        // MUTATION: dropping the capability check (or always-true) -> a credential-bearing connector (jdbc/es)
        // would render its connection props -> red here for the no-capability case.
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL)).supportsShowCreateDdl());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsShowCreateDdl());
        // Independent of the other capabilities: declaring auto-analyze must NOT enable SHOW CREATE rendering.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE)).supportsShowCreateDdl());
    }

    @Test
    public void supportsViewReflectsConnectorCapability() {
        // isView() resolution and the SHOW TABLES view-merge engage only when the connector declares
        // SUPPORTS_VIEW. MUTATION: dropping the capability check (or always-true) -> view-less connectors
        // (jdbc/es) would issue view round-trips / look like potential views -> red for the no-capability case.
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_VIEW)).supportsView());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsView());
        // Independent of the other capabilities.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL)).supportsView());
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable wired so the REAL makeSureInitialized /
     * resolveIsView / isView path runs end-to-end: the connector declares {@code caps}, its metadata reports
     * {@code viewExists} for the (db, remote-name) pair, and the table resolves to remote {@code db1.v1}. The
     * db is wired both as the {@code db} field (used by resolveIsView) and via getDbOrAnalysisException (used
     * by the base makeSureInitialized).
     */
    private static PluginDrivenExternalTable pluginViewTable(Set<ConnectorCapability> caps, boolean viewExists) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(viewExists);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(caps);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn("db1");
        Mockito.when(db.getId()).thenReturn(100L);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        try {
            Mockito.when(catalog.getDbOrAnalysisException(Mockito.anyString())).thenReturn(db);
        } catch (Exception ignore) {
            // getDbOrAnalysisException declares a checked exception; the stub never throws.
        }
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Deencapsulation.setField(table, "db", db);
        Deencapsulation.setField(table, "dbName", "db1");
        Deencapsulation.setField(table, "remoteName", "v1");
        return table;
    }

    @Test
    public void resolveIsViewConsultsConnectorWhenViewCapable() {
        // WHY: a flipped table reports view-ness by asking the connector (mirrors legacy
        // IcebergExternalTable.makeSureInitialized -> catalog.viewExists). MUTATION: returning a constant
        // instead of metadata.viewExists -> red for one of the two cases.
        Assertions.assertTrue(
                pluginViewTable(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW), true).resolveIsView());
        Assertions.assertFalse(
                pluginViewTable(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW), false).resolveIsView());
    }

    @Test
    public void resolveIsViewIsFalseWithoutCapabilityAndIssuesNoViewRoundTrip() {
        // WHY: view-less connectors (jdbc/es) must issue NO viewExists round-trip and stay isView()==false.
        // MUTATION: dropping the supportsView() gate -> resolveIsView reaches viewExists(session,"db1","v1")
        // (both args non-null so the stub returns true AND the verify(never) matches the call) -> the
        // assertion flips to true and verify(never) trips -> red.
        // NOTE: the table MUST carry a non-null remoteName + db (so the would-be viewExists call has non-null
        // string args) — otherwise getRemoteName()==null dodges anyString() in both the stub and the verify,
        // and the gate-drop mutation survives silently.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(true);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(EnumSet.noneOf(ConnectorCapability.class));
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn("db1");
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Deencapsulation.setField(table, "db", db);
        Deencapsulation.setField(table, "remoteName", "v1");

        Assertions.assertFalse(table.resolveIsView());
        Mockito.verify(metadata, Mockito.never())
                .viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void resolveIsViewIsFalseWhenConnectorAbsent() {
        // MUTATION: dropping the null-connector guard NPEs — a not-yet-initialized catalog must degrade to
        // "not a view", never crash planning.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Assertions.assertFalse(table.resolveIsView());
    }

    @Test
    public void isViewSurfacesResolvedFlagThroughRealInit() {
        // WHY: isView() must run makeSureInitialized (which resolves+caches the flag) and surface it.
        // MUTATION: hard-coding isView() to the base false, or not triggering makeSureInitialized -> the
        // resolved view flag is lost -> red.
        Assertions.assertTrue(
                pluginViewTable(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW), true).isView());
        Assertions.assertFalse(
                pluginViewTable(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW), false).isView());
    }

    @Test
    public void systemTableOverridesResolveIsViewToFalse() {
        // A system/metadata table ($snapshots etc.) overrides resolveIsView to a constant false so the base
        // never issues a viewExists round-trip on its synthetic "$"-suffixed name. Here the catalog declares
        // SUPPORTS_VIEW and viewExists==true, so the BASE resolveIsView WOULD return true; the override must
        // still yield false. MUTATION: dropping the sys override -> base consults the connector -> true -> red.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.viewExists(Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(true);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_VIEW));
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        ExternalDatabase db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn("db1");
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);

        PluginDrivenSysExternalTable sys =
                Mockito.mock(PluginDrivenSysExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(sys, "catalog", catalog);
        Deencapsulation.setField(sys, "db", db);
        Deencapsulation.setField(sys, "remoteName", "v1$snapshots");

        Assertions.assertFalse(sys.resolveIsView(), "a system table must never report itself as a view");
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable whose schema cache returns {@code rawProps} as the
     * connector-emitted raw table-property map, to exercise getTableProperties()/getShow* over the real strip
     * + render-hint logic. makeSureInitialized is stubbed to a no-op (no Env-backed init in a unit test).
     */
    private static PluginDrivenExternalTable pluginTableWithRawProperties(Map<String, String> rawProps) {
        PluginDrivenSchemaCacheValue scv = Mockito.mock(PluginDrivenSchemaCacheValue.class);
        Mockito.when(scv.getTableProperties()).thenReturn(rawProps);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(Optional.of(scv)).when(table).getSchemaCacheValue();
        return table;
    }

    @Test
    public void getTablePropertiesStripsControlAndRenderHintKeys() {
        // WHY: the rendered PROPERTIES(...) block must contain only user-facing properties — the FE-internal
        // schema-control keys (partition_columns/primary_keys) and the SHOW CREATE render-hint keys
        // (show.location/show.partition-clause/show.sort-clause, rendered as separate clauses) must be stripped.
        // MUTATION: dropping any strip clause -> that key leaks into PROPERTIES -> red.
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("write.format.default", "parquet");
        raw.put("partition_columns", "id");
        raw.put("primary_keys", "id");
        raw.put("show.location", "s3://bucket/db/t");
        raw.put("show.partition-clause", "PARTITION BY LIST (`id`) ()");
        raw.put("show.sort-clause", "ORDER BY (`id` ASC NULLS FIRST)");
        raw.put("path", "s3://bucket/db/t");

        Map<String, String> props = pluginTableWithRawProperties(raw).getTableProperties();

        Assertions.assertEquals("parquet", props.get("write.format.default"),
                "user-facing properties are preserved");
        Assertions.assertTrue(props.containsKey("path"),
                "a connector's user-facing path property (paimon) is preserved");
        Assertions.assertFalse(props.containsKey("partition_columns"));
        Assertions.assertFalse(props.containsKey("primary_keys"));
        Assertions.assertFalse(props.containsKey("show.location"));
        Assertions.assertFalse(props.containsKey("show.partition-clause"));
        Assertions.assertFalse(props.containsKey("show.sort-clause"));
    }

    @Test
    public void getShowLocationReadsHintKeyWithPathFallback() {
        // Reserved show.location hint -> rendered LOCATION.
        Map<String, String> iceberg = new HashMap<>();
        iceberg.put("show.location", "s3://bucket/db/t");
        Assertions.assertEquals("s3://bucket/db/t",
                pluginTableWithRawProperties(iceberg).getShowLocation());

        // Paimon carries its location in the user-facing "path" property (no show.location) -> path fallback.
        // MUTATION: dropping the path fallback -> paimon LOCATION renders empty -> red.
        Map<String, String> paimon = new HashMap<>();
        paimon.put("path", "s3://bucket/db/p");
        Assertions.assertEquals("s3://bucket/db/p",
                pluginTableWithRawProperties(paimon).getShowLocation());

        // show.location wins over path when both present (a connector that emits both).
        Map<String, String> both = new HashMap<>();
        both.put("show.location", "s3://hint");
        both.put("path", "s3://path");
        Assertions.assertEquals("s3://hint", pluginTableWithRawProperties(both).getShowLocation());

        // Neither present -> empty (no LOCATION clause rendered).
        Assertions.assertEquals("", pluginTableWithRawProperties(new HashMap<>()).getShowLocation());
    }

    @Test
    public void getShowPartitionAndSortClauseReadHintKeys() {
        Map<String, String> raw = new HashMap<>();
        raw.put("show.partition-clause", "PARTITION BY LIST (BUCKET(8, `id`)) ()");
        raw.put("show.sort-clause", "ORDER BY (`name` DESC NULLS LAST)");
        PluginDrivenExternalTable table = pluginTableWithRawProperties(raw);
        Assertions.assertEquals("PARTITION BY LIST (BUCKET(8, `id`)) ()", table.getShowPartitionClause());
        Assertions.assertEquals("ORDER BY (`name` DESC NULLS LAST)", table.getShowSortClause());

        // Absent -> empty (no clause rendered). MUTATION: returning null/non-empty -> red.
        PluginDrivenExternalTable none = pluginTableWithRawProperties(new HashMap<>());
        Assertions.assertEquals("", none.getShowPartitionClause());
        Assertions.assertEquals("", none.getShowSortClause());
    }

    @Test
    public void needInternalHiddenColumnsTracksSyntheticWriteCtxFlag() {
        // MUTATION: a needInternalHiddenColumns() that ignores the ctx flag (always false) makes post-flip
        // DML skip the row-id injection. This pins the neutral ctx signal (set per-table during row-level DML).
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(99L).when(table).getId();

        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        ctx.setSyntheticWriteColTargetTableId(99L);
        Assertions.assertTrue(table.needInternalHiddenColumns(),
                "the ctx synthetic-write flag for this table id opens the hidden-column gate");

        ctx.setSyntheticWriteColTargetTableId(101L);
        Assertions.assertFalse(table.needInternalHiddenColumns(),
                "the flag set for a different table id does not open the gate for this table");

        ctx.setSyntheticWriteColTargetTableId(-1L);
        Assertions.assertFalse(table.needInternalHiddenColumns(),
                "the cleared (-1) flag closes the gate");
    }
}
