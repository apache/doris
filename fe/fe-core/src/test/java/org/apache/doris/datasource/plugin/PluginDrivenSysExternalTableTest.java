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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Pins the system-table opt-outs from Top-N lazy materialization and nested-column pruning on
 * {@link PluginDrivenSysExternalTable}.
 *
 * <p>WHY the lazy-mat opt-out matters: a system/metadata table (e.g. {@code tbl$snapshots}) is served by the
 * connector's JNI serialized-split metadata reader, which synthesizes rows and produces no file+position row-id.
 * Top-N lazy materialization injects the engine-wide row-id slot ({@code __DORIS_GLOBAL_ROWID_COL__}) and expects
 * the scan to re-fetch survivors by row-id, so admitting a sys table makes BE abort with
 * {@code __DORIS_GLOBAL_ROWID_COL__... return column size 0 not equal to expected size 1}. Legacy never lazy-
 * materialized sys tables ({@code IcebergSysExternalTable} is absent from
 * {@code MaterializeProbeVisitor.SUPPORT_RELATION_TYPES}); the base {@link PluginDrivenExternalTable} keys the
 * capability off the connector alone, so the sys table must opt out itself.
 *
 * <p>WHY the nested-prune opt-out matters: pruning would rewrite a complex column's access-path top element from
 * its NAME to a numeric iceberg field id ({@code SlotTypeReplacer}), but a system-table scan ships no field-id
 * dictionary ({@code IcebergScanPlanProvider} skips {@code SCHEMA_EVOLUTION_PROP} when {@code systemTable}), so
 * BE cannot field-id-match and rejects the scan with {@code AccessPathParser access path N does not match slot X}.
 * Legacy gated the field-id rewrite on the exact class {@code IcebergExternalTable}, which sys tables are not, so
 * it never fired for them; the migrated gate keys off the connector capability alone, so the sys table must opt
 * out itself.
 *
 * <p>Mockito {@code CALLS_REAL_METHODS} runs the real capability methods over a stubbed connector chain,
 * mirroring {@code PluginDrivenExternalTableTest}.
 */
public class PluginDrivenSysExternalTableTest {

    /**
     * A CALLS_REAL_METHODS {@link PluginDrivenSysExternalTable} whose connector declares exactly
     * {@code capabilities}, to exercise the capability-helper methods over the real connector chain. Only the
     * {@code catalog} field is set — the methods under test never touch the sys-table's source/name fields.
     */
    private static PluginDrivenSysExternalTable sysTableWithCapabilities(Set<ConnectorCapability> capabilities) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenSysExternalTable table =
                Mockito.mock(PluginDrivenSysExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    @Test
    public void systemTableNeverSupportsTopNLazyMaterializeEvenWhenConnectorDeclaresIt() {
        // The BE JNI metadata reader cannot produce the lazy-mat row-id for a synthesized sys-table row, so the
        // sys table must opt out of Top-N lazy materialization even though its connector declares the
        // capability. MUTATION: deleting the override re-inherits the connector-capability answer -> true -> red.
        Assertions.assertFalse(sysTableWithCapabilities(
                        EnumSet.of(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE)).supportsTopNLazyMaterialize(),
                "a system/metadata table must never lazy-materialize, even when the connector supports it");
    }

    /**
     * Same, plus a stubbed schema-cache value carrying {@code perTableCapabilities} — the set a system table
     * resolves its own nested-prune answer from. Mirrors {@code PluginDrivenExternalTableTest.pluginTable}.
     */
    private static PluginDrivenSysExternalTable sysTable(Set<ConnectorCapability> connectorCapabilities,
            Set<ConnectorCapability> perTableCapabilities) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(connectorCapabilities);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenSchemaCacheValue scv = Mockito.mock(PluginDrivenSchemaCacheValue.class);
        Mockito.when(scv.getTableCapabilities()).thenReturn(perTableCapabilities);
        PluginDrivenSysExternalTable table =
                Mockito.mock(PluginDrivenSysExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        Mockito.doNothing().when(table).makeSureInitialized();
        Mockito.doReturn(Optional.of(scv)).when(table).getSchemaCacheValue();
        return table;
    }

    @Test
    public void systemTableNeverSupportsNestedColumnPruneEvenWhenConnectorDeclaresIt() {
        // A metadata-table scan ships NO field-id dictionary, so the name->field-id access-path rewrite BE
        // would receive (SlotTypeReplacer) cannot be field-id-matched and BE rejects it with
        // "AccessPathParser access path N does not match slot X"; and the JNI metadata reader indexes its
        // record by the Doris child position, so a pruned type makes it return a different field's value. A
        // sys table must therefore stay out however loudly its CONNECTOR declares the data-table capability.
        Assertions.assertFalse(sysTable(EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE),
                        EnumSet.noneOf(ConnectorCapability.class)).supportsNestedColumnPrune(),
                "a system/metadata table must never nested-column-prune, even when the connector supports it");
    }

    @Test
    public void systemTableStaysOutWhenOnlyTheDataTablePruneBitReachesItsSchema() {
        // The data-table bit reaches a system table's own schema for real: HiveConnectorMetadata
        // .reflectSiblingCapabilities copies the owning sibling's connector-wide set onto EVERY schema it
        // forwards, and an iceberg-on-HMS tbl$snapshots is forwarded through exactly that path. Resolving the
        // opt-in from that bit would admit the one reader that cannot take a pruned type, so the opt-in has a
        // capability of its own — which nothing reflects.
        Assertions.assertFalse(sysTable(EnumSet.noneOf(ConnectorCapability.class),
                        EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE,
                                ConnectorCapability.SUPPORTS_FIELD_ID_ACCESS_PATH)).supportsNestedColumnPrune(),
                "a delegated metadata table inherits the data-table bits and must still stay out");
    }

    @Test
    public void systemTableOptsIntoNestedColumnPruneThroughItsOwnSchema() {
        // The opt-out is about WHICH READER serves the table, and that is a per-table question. A system table
        // served by the ordinary data readers (fluss tbl$lake through the paimon sibling, tbl$log through the
        // fluss scanner) honours a pruned type exactly like the front door does, and is as large as the front
        // door — leaving it out costs the read amplification pruning exists to avoid, and makes one query
        // answer differently through tbl than through tbl$lake. Such a table says so on its OWN schema.
        Assertions.assertTrue(sysTable(EnumSet.noneOf(ConnectorCapability.class),
                        EnumSet.of(ConnectorCapability.SUPPORTS_SYS_TABLE_NESTED_COLUMN_PRUNE))
                        .supportsNestedColumnPrune(),
                "a system table whose own schema declares the sys-table capability must nested-column-prune");
        Assertions.assertFalse(sysTable(EnumSet.noneOf(ConnectorCapability.class),
                        EnumSet.noneOf(ConnectorCapability.class)).supportsNestedColumnPrune(),
                "a system table that declares nothing must stay out");
    }
}
