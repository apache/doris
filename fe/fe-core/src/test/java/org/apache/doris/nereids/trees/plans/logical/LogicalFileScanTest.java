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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalFileScanTest {

    @Test
    public void testComputeOutputIncludesInvisibleRowLineageColumnsForIcebergTable() {
        // Post-cutover a native iceberg table is a PluginDrivenExternalTable, so computeOutput() flows through
        // computePluginDrivenOutput() (row-lineage columns are surfaced by the connector via getFullSchema); the
        // legacy exact-class IcebergExternalTable arm is gone. Assert the invisible v3 row-lineage columns still
        // reach the plan output.
        Column rowIdColumn = new Column(IcebergUtils.ICEBERG_ROW_ID_COL, Type.BIGINT, true);
        rowIdColumn.setIsVisible(false);
        Column lastUpdatedSequenceNumberColumn =
                new Column(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL, Type.BIGINT, true);
        lastUpdatedSequenceNumberColumn.setIsVisible(false);
        List<Column> schema = Arrays.asList(
                new Column("id", Type.INT, true),
                rowIdColumn,
                lastUpdatedSequenceNumberColumn);

        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");

        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        List<String> outputNames = scan.computeOutput().stream().map(slot -> slot.getName())
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(
                "id",
                IcebergUtils.ICEBERG_ROW_ID_COL,
                IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL), outputNames);
    }

    @Test
    public void supportPruneNestedColumnDelegatesToPluginCapability() {
        // WHY (H-10 L1): a flipped plugin-driven table (e.g. iceberg as PluginDrivenMvccExternalTable) is no
        // longer an IcebergExternalTable, so supportPruneNestedColumn must consult the connector capability via
        // PluginDrivenExternalTable.supportsNestedColumnPrune() instead of the dead exact-class arm. MUTATION:
        // reverting the plugin arm to a hard-coded `return false` -> the capable case below reds.
        PluginDrivenExternalTable capable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(capable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(capable.supportsNestedColumnPrune()).thenReturn(true);
        LogicalFileScan capableScan = new LogicalFileScan(new RelationId(2), capable,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        Assertions.assertTrue(capableScan.supportPruneNestedColumn(),
                "a plugin table whose connector declares the capability must support nested-column prune");

        PluginDrivenExternalTable incapable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(incapable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(incapable.supportsNestedColumnPrune()).thenReturn(false);
        LogicalFileScan incapableScan = new LogicalFileScan(new RelationId(3), incapable,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        Assertions.assertFalse(incapableScan.supportPruneNestedColumn(),
                "a plugin table whose connector does not declare the capability must not prune nested columns");
    }
}
