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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.connector.api.scan.ConnectorColumnCategory;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.thrift.TColumnCategory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

/**
 * Guards {@link PluginDrivenScanNode#classifyColumn}, the P6.6-C2 (WS-SYNTH-READ) generic, connector-agnostic
 * port of the legacy per-connector {@code classifyColumn} overrides.
 *
 * <p>WHY this matters: once iceberg flips onto the generic {@link PluginDrivenScanNode}, the base
 * {@code FileQueryScanNode.classifyColumn} tags every column REGULAR. A REGULAR column becomes a FILE slot
 * ({@code isFileSlot = category == REGULAR || category == GENERATED}), so the BE would try to read the
 * synthesized row-id columns ({@code __DORIS_GLOBAL_ROWID_COL__*} lazy-materialization id,
 * {@code __DORIS_ICEBERG_ROWID_COL__} hidden id) from a data file where they do not exist, and would demote
 * the v3 row-lineage columns from backfill-capable GENERATED to plain REGULAR. This override keeps the
 * engine-wide GLOBAL_ROWID classification in the generic node (a Doris mechanism, mirroring
 * {@code HiveScanNode}/{@code TVFScanNode}) and delegates connector-owned special columns to the connector
 * SPI, so no connector knowledge leaks into fe-core.</p>
 *
 * <p>Driven on a Mockito {@code CALLS_REAL_METHODS} mock (no constructor — building a full
 * {@link FileQueryScanNode} needs a harness this module lacks) with the connector seam
 * {@code classifyColumnByConnector} stubbed (package-private exactly for this, mirroring
 * {@code sysTableSupportsTimeTravel}), so the real category-mapping logic runs against controlled state.</p>
 */
public class PluginDrivenScanNodeClassifyColumnTest {

    /** {@code isFileSlot} as derived in {@code FileQueryScanNode#initSchemaParams} (the read-from-file gate). */
    private static boolean isFileSlot(TColumnCategory category) {
        return category == TColumnCategory.REGULAR || category == TColumnCategory.GENERATED;
    }

    private static PluginDrivenScanNode node() {
        return Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
    }

    private static SlotDescriptor slotNamed(String name) {
        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        Column column = Mockito.mock(Column.class);
        Mockito.doReturn(name).when(column).getName();
        Mockito.doReturn(column).when(slot).getColumn();
        return slot;
    }

    @Test
    public void globalRowIdIsSynthesizedInGenericNodeWithoutConsultingConnector() {
        PluginDrivenScanNode node = node();
        // A suffixed lazy-materialization row-id (LazyMaterializeTopN appends the table/function name).
        SlotDescriptor slot = slotNamed(Column.GLOBAL_ROWID_COL + "my_tbl");

        TColumnCategory category = node.classifyColumn(slot, Collections.emptyList());

        // WHY: GLOBAL_ROWID is a generic Doris lazy-mat mechanism (also classified by Hive/TVF), so the
        // generic node owns it and must NOT delegate to the connector. MUTATION: startsWith -> equals drops
        // the suffixed name to REGULAR (a file slot) -> red.
        Assertions.assertEquals(TColumnCategory.SYNTHESIZED, category);
        Assertions.assertFalse(isFileSlot(category), "GLOBAL_ROWID must not be read from the data file");
        Mockito.verify(node, Mockito.never()).classifyColumnByConnector(Mockito.anyString());
    }

    @Test
    public void connectorSynthesizedColumnMapsToSynthesized() {
        PluginDrivenScanNode node = node();
        SlotDescriptor slot = slotNamed("__DORIS_ICEBERG_ROWID_COL__");
        Mockito.doReturn(ConnectorColumnCategory.SYNTHESIZED).when(node)
                .classifyColumnByConnector("__DORIS_ICEBERG_ROWID_COL__");

        TColumnCategory category = node.classifyColumn(slot, Collections.emptyList());

        // WHY: a connector special column reported SYNTHESIZED must NOT become a file slot (the BE
        // materializes it). MUTATION: dropping the connector delegation -> REGULAR file slot -> the BE reads
        // a non-existent file column -> red.
        Assertions.assertEquals(TColumnCategory.SYNTHESIZED, category);
        Assertions.assertFalse(isFileSlot(category), "connector SYNTHESIZED column must not be a file slot");
    }

    @Test
    public void connectorGeneratedColumnMapsToGeneratedAndStaysFileSlot() {
        PluginDrivenScanNode node = node();
        SlotDescriptor slot = slotNamed("_row_id");
        Mockito.doReturn(ConnectorColumnCategory.GENERATED).when(node).classifyColumnByConnector("_row_id");

        TColumnCategory category = node.classifyColumn(slot, Collections.emptyList());

        // WHY: v3 row-lineage is GENERATED = read from file when present, otherwise backfilled, so it MUST
        // stay a file slot. MUTATION: mapping GENERATED -> SYNTHESIZED would drop it from the file-read set
        // and lose the backfill path -> red.
        Assertions.assertEquals(TColumnCategory.GENERATED, category);
        Assertions.assertTrue(isFileSlot(category), "GENERATED row-lineage must remain a file slot for backfill");
    }

    @Test
    public void defaultColumnFallsThroughToRegular() {
        PluginDrivenScanNode node = node();
        SlotDescriptor slot = slotNamed("id");
        Mockito.doReturn(ConnectorColumnCategory.DEFAULT).when(node).classifyColumnByConnector("id");

        TColumnCategory category = node.classifyColumn(slot, Collections.emptyList());

        // WHY: a plain data column (connector says DEFAULT, not a partition key) is REGULAR. MUTATION:
        // breaking the `else super()` fall-through -> wrong category -> red.
        Assertions.assertEquals(TColumnCategory.REGULAR, category);
        Assertions.assertTrue(isFileSlot(category));
    }

    @Test
    public void defaultPartitionColumnFallsThroughToPartitionKey() {
        PluginDrivenScanNode node = node();
        SlotDescriptor slot = slotNamed("part_col");
        Mockito.doReturn(ConnectorColumnCategory.DEFAULT).when(node).classifyColumnByConnector("part_col");
        List<String> partitionKeys = Collections.singletonList("part_col");

        TColumnCategory category = node.classifyColumn(slot, partitionKeys);

        // WHY: partition keys must keep flowing through super() so partition handling is unchanged. MUTATION:
        // swallowing DEFAULT instead of calling super() would lose PARTITION_KEY -> red.
        Assertions.assertEquals(TColumnCategory.PARTITION_KEY, category);
    }

    @Test
    public void connectorRowIdConstantContractIsPinned() {
        // CONTRACT: the iceberg connector cannot import fe-core, so IcebergScanPlanProvider duplicates these
        // literals (DORIS_ICEBERG_ROWID_COL / _row_id / _last_updated_sequence_number). Pin the Doris-side
        // constant VALUES so a rename here fails loud, flagging that the connector duplicates must change too.
        Assertions.assertEquals("__DORIS_ICEBERG_ROWID_COL__", Column.ICEBERG_ROWID_COL);
        Assertions.assertEquals("_row_id", IcebergUtils.ICEBERG_ROW_ID_COL);
        Assertions.assertEquals("_last_updated_sequence_number",
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL);
    }
}
