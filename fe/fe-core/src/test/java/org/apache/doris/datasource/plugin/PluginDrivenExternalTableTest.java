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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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

    // ==================== §4.4 W3: per-handle write-admission capability probes ====================

    /**
     * A CALLS_REAL_METHODS table whose connector answers the write capabilities PER-HANDLE (the overloads a
     * heterogeneous gateway diverts). {@code handlePresent=false} models an unresolvable handle.
     */
    private static PluginDrivenExternalTable capabilityTable(boolean handlePresent,
            Set<WriteOperation> ops, boolean branch) {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(handlePresent ? Optional.of(handle) : Optional.empty());
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(connector.supportedWriteOperations(Mockito.any())).thenReturn(ops);
        Mockito.when(connector.supportsWriteBranch(Mockito.any())).thenReturn(branch);
        Mockito.when(connector.requiresPartitionHashWrite(Mockito.any())).thenReturn(true);
        Mockito.when(connector.requiresMaterializeStaticPartitionValues(Mockito.any())).thenReturn(true);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(Mockito.mock(ConnectorSession.class));
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    @Test
    public void connectorWriteCapabilitiesResolvePerHandle() {
        Set<WriteOperation> ops = EnumSet.of(WriteOperation.INSERT, WriteOperation.DELETE, WriteOperation.MERGE);
        PluginDrivenExternalTable table = capabilityTable(true, ops, true);
        Assertions.assertEquals(ops, table.connectorSupportedWriteOperations(),
                "the write ops must come from the connector's per-handle overload (resolved via the handle)");
        Assertions.assertTrue(table.connectorSupportsWriteBranch(),
                "the branch capability must come from the connector's per-handle overload");
        Assertions.assertTrue(table.requirePartitionHashOnWrite(),
                "partition-hash-write must come from the connector's per-handle overload");
        Assertions.assertTrue(table.materializeStaticPartitionValues(),
                "materialize-static-partition must come from the connector's per-handle overload");
    }

    @Test
    public void connectorWriteCapabilitiesDegradeWhenHandleUnresolvable() {
        // An unresolvable handle (dropped table / catalog) must degrade to "no writes" — empty op set + false —
        // rather than misrouting or NPE-ing, even though the connector WOULD report the capabilities for a handle.
        PluginDrivenExternalTable table = capabilityTable(false, EnumSet.of(WriteOperation.DELETE), true);
        Assertions.assertTrue(table.connectorSupportedWriteOperations().isEmpty(),
                "an unresolvable handle degrades write ops to the empty set");
        Assertions.assertFalse(table.connectorSupportsWriteBranch(),
                "an unresolvable handle degrades branch support to false");
        Assertions.assertFalse(table.requirePartitionHashOnWrite(),
                "an unresolvable handle degrades partition-hash-write to false");
        Assertions.assertFalse(table.materializeStaticPartitionValues(),
                "an unresolvable handle degrades materialize-static-partition to false");
    }

    @Test
    public void connectorWriteCapabilitiesDegradeWhenConnectorNull() {
        // A catalog dropped mid-planning nulls its transient connector; the probes must degrade (empty / false)
        // rather than NPE — this is the null-connector guard the row-id / DML gates rely on.
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Assertions.assertTrue(table.connectorSupportedWriteOperations().isEmpty(),
                "a null connector degrades write ops to the empty set");
        Assertions.assertFalse(table.connectorSupportsWriteBranch(), "a null connector degrades branch to false");
    }

    // ==================== §4.4 W4: per-handle transaction write-target handle resolution ====================

    // A CALLS_REAL_METHODS table whose connector resolves the write-target handle to `resolved` (null => empty).
    private static PluginDrivenExternalTable writeTargetTable(ConnectorTableHandle resolved) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.ofNullable(resolved));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(Mockito.mock(ConnectorSession.class));
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    @Test
    public void resolveWriteTargetHandleReturnsTheConnectorResolvedHandle() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Assertions.assertSame(handle, writeTargetTable(handle).resolveWriteTargetHandle(),
                "the insert executor threads THIS handle into beginTransaction(session, handle) so a heterogeneous "
                        + "gateway opens the sibling's transaction for a foreign table");
    }

    @Test
    public void resolveWriteTargetHandleFailsLoudWhenUnresolvable() {
        // FAILS LOUD rather than returning null: a null handle is not an instanceof the gateway's own handle type
        // and would misroute a plain write to the sibling. A downgrade to orElse(null) must break this test.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> writeTargetTable(null).resolveWriteTargetHandle(),
                "an unresolvable write-target handle must fail loud, not return null");
        Assertions.assertTrue(e.getMessage().startsWith("Cannot resolve the connector table handle for write target"),
                "the fail-loud message must name the unresolved write target");
    }

    // ============= HD-C3 INC-4: synthetic scan predicate (connector residual predicate) plumbing =============

    // A CALLS_REAL_METHODS table whose connector resolves the handle to `resolved` (null => empty) and returns
    // `predicates` from getSyntheticScanPredicates.
    private static PluginDrivenExternalTable syntheticPredicateTable(ConnectorTableHandle resolved,
            List<ConnectorExpression> predicates) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.ofNullable(resolved));
        Mockito.when(metadata.getSyntheticScanPredicates(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(predicates);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(Mockito.mock(ConnectorSession.class));
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    private static PluginDrivenMvccSnapshot pluginSnapshot() {
        return new PluginDrivenMvccSnapshot(ConnectorMvccSnapshot.builder().build(),
                Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void getSyntheticScanPredicatesDelegatesToTheConnectorForAPluginSnapshot() {
        // The analysis rule retrieves the resolved snapshot and asks the connector for its residual predicate
        // (the hudi @incr _hoodie_commit_time window); the table threads it verbatim from the SPI.
        List<ConnectorExpression> sentinel = Collections.singletonList(
                new ConnectorColumnRef("_hoodie_commit_time", ConnectorType.of("STRING")));
        PluginDrivenExternalTable table =
                syntheticPredicateTable(Mockito.mock(ConnectorTableHandle.class), sentinel);
        Assertions.assertSame(sentinel, table.getSyntheticScanPredicates(pluginSnapshot()),
                "the residual predicate must be threaded verbatim from the connector SPI");
    }

    @Test
    public void getSyntheticScanPredicatesEmptyForNonPluginSnapshot() {
        // A non-plugin MvccSnapshot carries no ConnectorMvccSnapshot to hand the SPI -> empty (and the guard
        // avoids a ClassCastException). The rule then adds no filter.
        MvccSnapshot foreign = Mockito.mock(MvccSnapshot.class);
        Assertions.assertTrue(syntheticPredicateTable(Mockito.mock(ConnectorTableHandle.class),
                        Collections.emptyList()).getSyntheticScanPredicates(foreign).isEmpty(),
                "a non-plugin snapshot must yield no synthetic predicates");
    }

    @Test
    public void getSyntheticScanPredicatesEmptyWhenHandleUnresolvable() {
        // An unresolvable handle (concurrent DROP / transient metadata error) degrades to empty rather than
        // handing the gateway a null handle -> the rule simply adds no filter.
        Assertions.assertTrue(syntheticPredicateTable(null, Collections.emptyList())
                        .getSyntheticScanPredicates(pluginSnapshot()).isEmpty(),
                "an unresolvable handle must degrade to no synthetic predicates");
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
        // Production now selects the write provider per-handle; a plain Mockito mock does not run the interface
        // default, so stub the per-handle overload to the same provider (mirrors the scan seam).
        Mockito.when(connector.getWritePlanProvider(Mockito.any()))
                .thenReturn(writeProviderPresent ? provider : null);
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
    public void getFullSchemaWithSnapshotAppendsSyntheticColumnsWhenGated() {
        // The PLAN path (LogicalFileScan.computePluginDrivenOutput) calls the 1-arg overload, NOT the 0-arg
        // one. It must append the synthetic write columns exactly like the 0-arg form -- when it did not,
        // iceberg's row-id STRUCT vanished from the scan output: every row-level DML died with "Unknown
        // column '__DORIS_ICEBERG_ROWID_COL__'" and SELECT * under show-hidden came back one column short
        // (CI 997422, 9 suites).
        // MUTATION: deleting the 1-arg override in PluginDrivenExternalTable -> red (ExternalTable's
        // non-appending overload takes over and this returns BASE_SCHEMA.size()).
        // NOTE: this MUST run on a CALLS_REAL_METHODS instance. Stubbing getFullSchema(Optional) on a mock
        // intercepts before the real body, so it can never detect a missing override -- that blind spot is
        // exactly why the regression shipped green.
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), true, true);
        Mockito.doReturn(true).when(table).needInternalHiddenColumns();

        List<Column> schema = table.getFullSchema(Optional.empty());

        Assertions.assertEquals(BASE_SCHEMA.size() + 1, schema.size(),
                "the 1-arg (plan-path) form appends the connector's synthetic write column");
        Assertions.assertEquals("__syn_write_col__", schema.get(2).getName());
        Assertions.assertEquals(table.getFullSchema(), schema,
                "0-arg and 1-arg must agree: the synthetic write columns are request-scoped, not "
                        + "version-scoped, so only the BASE schema read differs between the two forms");
    }

    @Test
    public void getFullSchemaWithSnapshotReturnsBaseWhenNotGated() {
        // The 1-arg form honours the same gate: an ordinary query (no DML, no show-hidden) planned through
        // LogicalFileScan must see exactly the base schema. MUTATION: routing the 1-arg override around the
        // gate (always append) -> red.
        PluginDrivenExternalTable table = pluginTable(Collections.singletonList(SYNTHETIC), true, true);
        Mockito.doReturn(false).when(table).needInternalHiddenColumns();

        List<Column> schema = table.getFullSchema(Optional.empty());

        Assertions.assertEquals(BASE_SCHEMA.size(), schema.size(),
                "ungated 1-arg getFullSchema returns the base schema with no synthetic write column");
        Assertions.assertTrue(schema.stream().noneMatch(c -> "__syn_write_col__".equals(c.getName())));
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
     * {@code capabilities} connector-wide and whose cached schema emits no per-table capability marker, to
     * exercise the capability-helper methods over the real connector chain.
     */
    private static PluginDrivenExternalTable pluginTableWithCapabilities(Set<ConnectorCapability> capabilities) {
        return pluginTableWithCapabilities(capabilities, Collections.emptyMap());
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable whose connector declares {@code capabilities}
     * connector-wide AND whose cached schema emits {@code perTableProps} as its raw table-property map (carrying
     * the {@code connector.per-table-capabilities} marker for heterogeneous connectors like hive). Exercises the
     * additive connector-wide-OR-per-table resolution in {@code hasScanCapability}. makeSureInitialized is
     * stubbed to a no-op (no Env-backed init in a unit test).
     */
    private static PluginDrivenExternalTable pluginTableWithCapabilities(
            Set<ConnectorCapability> capabilities, Map<String, String> perTableProps) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenSchemaCacheValue scv = Mockito.mock(PluginDrivenSchemaCacheValue.class);
        Mockito.when(scv.getTableProperties()).thenReturn(perTableProps);
        PluginDrivenExternalTable table =
                Mockito.mock(PluginDrivenExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(Optional.of(scv)).when(table).getSchemaCacheValue();
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
        // Auto-analyze is now resolved per-table (like Top-N / nested-prune): a heterogeneous hive catalog emits it
        // via the connector.per-table-capabilities marker for its plain-hive tables (and reflects the iceberg
        // sibling's set onto an iceberg-on-HMS table) even when the CATALOG connector-wide set lacks it. MUTATION:
        // reverting supportsColumnAutoAnalyze() to a connector-wide-only read ignores this marker, so a flipped
        // plain-hive / iceberg-on-HMS table silently drops out of auto-analyze -> red here.
        Map<String, String> autoAnalyzeMarker = Collections.singletonMap(
                ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE.name());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), autoAnalyzeMarker).supportsColumnAutoAnalyze());
        // The marker is capability-specific: an auto-analyze marker must NOT enable Top-N.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), autoAnalyzeMarker).supportsTopNLazyMaterialize());
    }

    @Test
    public void supportsMetadataTableReflectsConnectorCapability() {
        // The hudi_meta() / TIMELINE TVF's plugin arm gates on this. Hudi declares it connector-wide; the hive
        // gateway reflects it onto a hudi-on-HMS table via the per-table marker (both resolved by hasScanCapability).
        // MUTATION: hard-coding it / reading a different capability -> a flipped hudi table's hudi_meta() rejects
        // with "not a hudi table".
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_METADATA_TABLE)).supportsMetadataTable());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class),
                Collections.singletonMap(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                        ConnectorCapability.SUPPORTS_METADATA_TABLE.name())).supportsMetadataTable());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsMetadataTable());
        // Independent of the other capabilities: declaring metadata-table must NOT enable auto-analyze.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_METADATA_TABLE)).supportsColumnAutoAnalyze());
    }

    @Test
    public void supportsSampleAnalyzeReflectsConnectorCapability() {
        // AnalysisManager.canSample / AnalyzeTableCommand.isSamplingPartition / createAnalysisTask gate on this.
        // Hive emits it per-table for plain-hive only (legacy dlaType==HIVE); iceberg/hudi-on-HMS and native
        // iceberg/paimon do NOT declare it, keeping their build-time reject. MUTATION: hard-coding it / reading a
        // different capability -> sampled ANALYZE wrongly admitted or wrongly rejected.
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE)).supportsSampleAnalyze());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class),
                Collections.singletonMap(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                        ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE.name())).supportsSampleAnalyze());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsSampleAnalyze());
        // Independent: sample must NOT enable auto-analyze (iceberg-on-HMS gets auto-analyze but not sample).
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE)).supportsColumnAutoAnalyze());
    }

    @Test
    public void getDistributionColumnNamesReadsLowercasedMarker() {
        // Bucketing columns come from the connector's per-table marker (emitted RAW), lowercased HERE to mirror
        // legacy HMSExternalTable.getDistributionColumnNames. Consumed by sampled analyze's linear-vs-DUJ1 NDV
        // estimator choice. MUTATION: not lowercasing / not reading the marker -> the estimator choice regresses
        // for a flipped bucketed hive table.
        PluginDrivenExternalTable table = pluginTableWithCapabilities(EnumSet.noneOf(ConnectorCapability.class),
                Collections.singletonMap(ConnectorTableSchema.DISTRIBUTION_COLUMNS_KEY, "Id,Region"));
        Assertions.assertEquals(new HashSet<>(Arrays.asList("id", "region")), table.getDistributionColumnNames());
        // No marker -> empty (paimon/iceberg unchanged, TableIf default).
        Assertions.assertTrue(pluginTableWithCapabilities(EnumSet.noneOf(ConnectorCapability.class))
                .getDistributionColumnNames().isEmpty());
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
    public void supportsNestedColumnPruneReflectsConnectorCapability() {
        // WHY (H-10 L1): LogicalFileScan.supportPruneNestedColumn and the SlotTypeReplacer name->field-id
        // rewrite both gate on this for a flipped plugin table (replacing the legacy exact-class
        // IcebergExternalTable arm). MUTATION: hard-coding true/false -> the capability no longer drives it.
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE)).supportsNestedColumnPrune());
        // Independent of the other optimizer capabilities.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE)).supportsTopNLazyMaterialize());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE)).supportsNestedColumnPrune());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsNestedColumnPrune());
    }

    @Test
    public void scanCapabilityHonorsPerTableMarkerWhenConnectorWideAbsent() {
        // WHY: a heterogeneous connector (hive) cannot declare Top-N lazy / nested-column-prune connector-wide
        // because eligibility is per-table file-format gated (orc/parquet only) — blanket-declaring would
        // over-admit a text/json table (a correctness bug for nested prune). It emits the capability name only
        // for eligible tables via the connector.per-table-capabilities schema marker; the helper must honor that
        // additively even when the connector-wide set is EMPTY. MUTATION: dropping the per-table marker read ->
        // a flipped orc/parquet hive table silently loses the optimization -> red here.
        Map<String, String> topnMarker = Collections.singletonMap(
                ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE.name());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), topnMarker).supportsTopNLazyMaterialize());
        // The marker is capability-specific: a Top-N marker must NOT enable nested-column pruning.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), topnMarker).supportsNestedColumnPrune());
        // A multi-value marker enables exactly the listed capabilities.
        Map<String, String> bothMarker = Collections.singletonMap(
                ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY,
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE.name() + ","
                        + ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE.name());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), bothMarker).supportsTopNLazyMaterialize());
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), bothMarker).supportsNestedColumnPrune());
        // An empty / absent marker leaves both off (the plain-hive text-table case).
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class), Collections.emptyMap()).supportsTopNLazyMaterialize());
    }

    @Test
    public void supportsExternalMetadataPreloadReflectsConnectorCapability() {
        // F11: async metadata pre-load is gated on the connector-declared SUPPORTS_METADATA_PRELOAD capability
        // (replacing the legacy engine-name "jdbc" string). MUTATION: hard-coding true/false, or restoring the
        // engine-name gate, -> the capability no longer drives it.
        Assertions.assertTrue(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_METADATA_PRELOAD)).supportsExternalMetadataPreload());
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.noneOf(ConnectorCapability.class)).supportsExternalMetadataPreload());
        // Independent of the other capabilities.
        Assertions.assertFalse(pluginTableWithCapabilities(
                EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE)).supportsExternalMetadataPreload());
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
        Assertions.assertFalse(table.supportsNestedColumnPrune());
        Assertions.assertFalse(table.supportsExternalMetadataPreload());
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
    public void getViewTextReturnsConnectorViewSqlForVerbatimNames() {
        // WHY: BindRelation's plugin view arm (and SHOW CREATE) take the view body from getViewText(); it must
        // surface the connector's view SQL (NOT the dialect) for the table's REMOTE (db, view) pair. MUTATION:
        // returning getDialect() instead of getSql() -> body becomes "spark" -> red; passing wrong db/view
        // names -> the eq-stub misses -> null -> NPE -> red.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getViewDefinition(Mockito.any(), Mockito.eq("db1"), Mockito.eq("v1")))
                .thenReturn(new ConnectorViewDefinition("SELECT 1", "spark",
                        ImmutableList.of(
                                new ConnectorColumn("vid", ConnectorType.of("INT"), "", true, null, true),
                                new ConnectorColumn("vname", ConnectorType.of("STRING"), "", true, null, true))));
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
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

        Assertions.assertEquals("SELECT 1", table.getViewText());
        // Make the verbatim-names contract explicit (not just implicit via the eq-stub -> null -> NPE):
        // getViewText must ask the connector for THIS table's remote (db, view) pair, exactly once.
        Mockito.verify(metadata).getViewDefinition(Mockito.any(), Mockito.eq("db1"), Mockito.eq("v1"));
    }

    /**
     * Builds a CALLS_REAL_METHODS PluginDrivenExternalTable wired so the REAL initSchema runs against a stubbed
     * connector chain: metadata returns {@code viewDef} from getViewDefinition (identity column mapping) and NO
     * table handle (so a deleted isView() branch would fall through to an empty schema). isView() is stubbed
     * directly to {@code isView} (the branch under test) — mirroring how the getFullSchema tests stub
     * needInternalHiddenColumns. Remote (db, view) = (db1, v1).
     */
    private static PluginDrivenExternalTable initSchemaViewTable(ConnectorViewDefinition viewDef, boolean isView) {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getViewDefinition(Mockito.any(), Mockito.eq("db1"), Mockito.eq("v1")))
                .thenReturn(viewDef);
        // Identity column-name mapping (the 4th arg is the column name); the Mockito default would return null
        // and NPE in toSchemaCacheValue.
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyString())).thenAnswer(inv -> inv.getArgument(3));
        // No table handle: proves the view schema does NOT come from the table-handle path.
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
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
        Mockito.doReturn(isView).when(table).isView();
        return table;
    }

    @Test
    public void initSchemaBuildsViewSchemaFromViewDefinitionColumns() {
        // WHY (H8): a flipped connector VIEW has no table handle (the SDK tableExists()==false for views), so
        // initSchema must build the schema from getViewDefinition().getColumns() instead of the table-handle
        // path — otherwise DESC / SHOW COLUMNS / information_schema.columns of the view are empty. MUTATION:
        // deleting the isView() branch -> initSchema falls through to the (absent) table handle -> Optional.empty
        // -> the present-with-columns assertions go red.
        ConnectorViewDefinition viewDef = new ConnectorViewDefinition("SELECT 1", "spark",
                ImmutableList.of(
                        new ConnectorColumn("vid", ConnectorType.of("INT"), "", true, null, true),
                        new ConnectorColumn("vname", ConnectorType.of("STRING"), "", true, null, true)));
        PluginDrivenExternalTable table = initSchemaViewTable(viewDef, true);

        Optional<SchemaCacheValue> result = table.initSchema();

        Assertions.assertTrue(result.isPresent(), "a view must resolve a (non-empty) schema cache value");
        List<Column> columns = result.get().getSchema();
        Assertions.assertEquals(2, columns.size(), "the view schema columns come from the view definition");
        Assertions.assertEquals("vid", columns.get(0).getName());
        Assertions.assertEquals("vname", columns.get(1).getName());
        // A view has no partition columns (legacy IcebergUtils.loadViewSchemaCacheValue: empty partition list).
        Assertions.assertTrue(
                ((PluginDrivenSchemaCacheValue) result.get()).getPartitionColumns().isEmpty(),
                "a view has no partition columns");
    }

    @Test
    public void initSchemaUsesTableHandlePathForNonView() {
        // WHY: the isView() branch must NOT hijack ordinary tables — a non-view must still resolve its schema
        // via the table handle (getTableHandle -> getTableSchema) and must never call getViewDefinition.
        // MUTATION: gating the new branch on something always-true (or inverting isView()) -> a table is routed
        // through getViewDefinition / the handle path is skipped -> red.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.eq("db1"), Mockito.eq("t1")))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.getTableSchema(Mockito.any(), Mockito.eq(handle)))
                .thenReturn(new ConnectorTableSchema("t1",
                        ImmutableList.of(new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null, true)),
                        "ICEBERG", Collections.emptyMap()));
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyString())).thenAnswer(inv -> inv.getArgument(3));
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Connector connector = Mockito.mock(Connector.class);
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
        Deencapsulation.setField(table, "remoteName", "t1");
        Mockito.doReturn(false).when(table).isView();

        Optional<SchemaCacheValue> result = table.initSchema();

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1, result.get().getSchema().size());
        Assertions.assertEquals("id", result.get().getSchema().get(0).getName());
        Mockito.verify(metadata, Mockito.never())
                .getViewDefinition(Mockito.any(), Mockito.anyString(), Mockito.anyString());
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
    public void getTablePropertiesStripsReservedKeysAndPassesThroughColludingUserKeys() {
        // WHY: the rendered PROPERTIES(...) block must contain only user-facing properties — every FE-internal
        // reserved control key (ConnectorTableSchema.RESERVED_CONTROL_KEYS, all namespaced under __internal.:
        // the partition-columns / primary-keys markers + the SHOW CREATE render hints) must be stripped.
        // Because the reserved keys are namespaced, a source table's own BARE property (e.g. literally named
        // "partition_columns") can NEVER collide with one, so it flows through unchanged.
        // MUTATION: reverting a reserved key to a bare name -> the bare user key would be stripped (data loss)
        // or the reserved key would leak into PROPERTIES -> red.
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("write.format.default", "parquet");
        raw.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY, "id");
        raw.put(ConnectorTableSchema.PRIMARY_KEYS_KEY, "id");
        raw.put(ConnectorTableSchema.SHOW_LOCATION_KEY, "s3://bucket/db/t");
        raw.put(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY, "PARTITION BY LIST (`id`) ()");
        raw.put(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY, "ORDER BY (`id` ASC NULLS FIRST)");
        raw.put("path", "s3://bucket/db/t");
        // A user's own BARE property whose name equals the OLD un-namespaced reserved name: it must survive.
        raw.put("partition_columns", "a_user_value");

        Map<String, String> props = pluginTableWithRawProperties(raw).getTableProperties();

        Assertions.assertEquals("parquet", props.get("write.format.default"),
                "user-facing properties are preserved");
        Assertions.assertTrue(props.containsKey("path"),
                "a connector's user-facing path property (paimon) is preserved");
        Assertions.assertEquals("a_user_value", props.get("partition_columns"),
                "a user's bare partition_columns property flows through (no collision with the reserved key)");
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.PRIMARY_KEYS_KEY));
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.SHOW_LOCATION_KEY));
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY));
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY));
    }

    @Test
    public void getShowLocationReadsHintKeyWithPathFallback() {
        // Reserved show-location hint -> rendered LOCATION.
        Map<String, String> iceberg = new HashMap<>();
        iceberg.put(ConnectorTableSchema.SHOW_LOCATION_KEY, "s3://bucket/db/t");
        Assertions.assertEquals("s3://bucket/db/t",
                pluginTableWithRawProperties(iceberg).getShowLocation());

        // Paimon carries its location in the user-facing "path" property (no show.location) -> path fallback.
        // MUTATION: dropping the path fallback -> paimon LOCATION renders empty -> red.
        Map<String, String> paimon = new HashMap<>();
        paimon.put("path", "s3://bucket/db/p");
        Assertions.assertEquals("s3://bucket/db/p",
                pluginTableWithRawProperties(paimon).getShowLocation());

        // the show-location hint wins over path when both present (a connector that emits both).
        Map<String, String> both = new HashMap<>();
        both.put(ConnectorTableSchema.SHOW_LOCATION_KEY, "s3://hint");
        both.put("path", "s3://path");
        Assertions.assertEquals("s3://hint", pluginTableWithRawProperties(both).getShowLocation());

        // Neither present -> empty (no LOCATION clause rendered).
        Assertions.assertEquals("", pluginTableWithRawProperties(new HashMap<>()).getShowLocation());
    }

    @Test
    public void getShowPartitionAndSortClauseReadHintKeys() {
        Map<String, String> raw = new HashMap<>();
        raw.put(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY, "PARTITION BY LIST (BUCKET(8, `id`)) ()");
        raw.put(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY, "ORDER BY (`name` DESC NULLS LAST)");
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
