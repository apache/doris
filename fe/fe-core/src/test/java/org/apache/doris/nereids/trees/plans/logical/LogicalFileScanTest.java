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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalFileScanTest {

    @Test
    public void testComputeOutputIncludesInvisibleRowLineageColumnsForIcebergTable() {
        Column rowIdColumn = new Column(IcebergUtils.ICEBERG_ROW_ID_COL, Type.BIGINT, true);
        rowIdColumn.setIsVisible(false);
        Column lastUpdatedSequenceNumberColumn =
                new Column(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL, Type.BIGINT, true);
        lastUpdatedSequenceNumberColumn.setIsVisible(false);
        List<Column> schema = Arrays.asList(
                new Column("id", Type.INT, true),
                rowIdColumn,
                lastUpdatedSequenceNumberColumn);

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(schema);
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
    public void testPaimonOptionsBindRelationScopedSnapshotSchema() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        Map<String, String> options = Collections.singletonMap("scan.snapshot-id", "1");
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList());
        Mockito.when(table.getFullSchema(scanParams)).thenReturn(Arrays.asList(
                new Column("id", Type.INT, true),
                new Column("old_name", Type.STRING, true)));

        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(scanParams), Optional.empty());

        Assertions.assertSame(SelectedPartitions.NOT_PRUNED, scan.getSelectedPartitions());
        Assertions.assertEquals(Arrays.asList("id", "old_name"),
                scan.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Mockito.verify(table, Mockito.never()).initSelectedPartitions(Mockito.any());
    }

    @Test
    public void testPaimonSupportsNestedColumnPruning() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("paimon_tbl");
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                Collections.singletonMap("scan.snapshot-id", "1"),
                Collections.emptyList());
        Mockito.when(table.getFullSchema(scanParams)).thenReturn(Collections.emptyList());

        LogicalFileScan scan = new LogicalFileScan(new RelationId(2), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(scanParams), Optional.empty());

        Assertions.assertTrue(scan.supportPruneNestedColumn());
    }

    @Test
    public void testCapturingRelationSchemaDoesNotAllocateOutputExprIds() throws Exception {
        StatementScopeIdGenerator.clear();
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");

        new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        Assertions.assertEquals(new ExprId(10000), StatementScopeIdGenerator.newExprId());
    }

    @Test
    public void testHmsIcebergCapturesRelationSnapshotSchema() {
        Column historicalColumn = new Column("old_name", Type.INT, true);
        Column latestColumn = new Column("new_name", Type.INT, true);
        MvccSnapshot historicalSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot latestSnapshot = Mockito.mock(MvccSnapshot.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Optional.of(historicalSnapshot)))
                .thenReturn(Collections.singletonList(historicalColumn));
        Mockito.when(table.getFullSchema(Optional.of(latestSnapshot)))
                .thenReturn(Collections.singletonList(latestColumn));
        Mockito.when(table.getName()).thenReturn("hms_iceberg_tbl");

        LogicalFileScan historicalFirst = new LogicalFileScan(new RelationId(3), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(historicalSnapshot));
        LogicalFileScan latestSecond = new LogicalFileScan(new RelationId(4), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(latestSnapshot));
        LogicalFileScan latestFirst = new LogicalFileScan(new RelationId(5), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(latestSnapshot));
        LogicalFileScan historicalSecond = new LogicalFileScan(new RelationId(6), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(historicalSnapshot));

        Assertions.assertEquals(Collections.singletonList("old_name"),
                historicalFirst.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("new_name"),
                latestSecond.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("new_name"),
                latestFirst.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Assertions.assertEquals(Collections.singletonList("old_name"),
                historicalSecond.computeOutput().stream().map(slot -> slot.getName()).collect(Collectors.toList()));
        Mockito.verify(table, Mockito.times(2)).getFullSchema(Optional.of(historicalSnapshot));
        Mockito.verify(table, Mockito.times(2)).getFullSchema(Optional.of(latestSnapshot));
    }

    @Test
    public void testResolvedSnapshotParticipatesInScanEquality() {
        MvccSnapshot firstSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot secondSnapshot = Mockito.mock(MvccSnapshot.class);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("moving_branch_table");
        TableScanParams branch = new TableScanParams(TableScanParams.BRANCH,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "moving"), Collections.emptyList());

        LogicalFileScan first = new LogicalFileScan(new RelationId(7), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(branch), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(8), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.of(branch), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertFalse(first.hasSameScanState(second));
    }

    @Test
    public void testEquivalentResolvedSnapshotsKeepScanEquality() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("fixed_snapshot_table");
        MvccSnapshot firstSnapshot = icebergSnapshot(10L, 3L);
        MvccSnapshot secondSnapshot = icebergSnapshot(10L, 3L);

        LogicalFileScan first = new LogicalFileScan(new RelationId(10), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(11), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertTrue(first.hasSameScanState(second));
    }

    @Test
    public void testDifferentFrozenNameMappingsBreakScanEquality() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Mockito.any()))
                .thenReturn(Collections.singletonList(new Column("id", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("legacy_name_mapping_table");
        MvccSnapshot firstSnapshot = icebergSnapshot(10L, 3L,
                Collections.singletonMap(1, Collections.singletonList("legacy_a")));
        MvccSnapshot secondSnapshot = icebergSnapshot(10L, 3L,
                Collections.singletonMap(1, Collections.singletonList("legacy_b")));

        LogicalFileScan first = new LogicalFileScan(new RelationId(12), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(firstSnapshot));
        LogicalFileScan second = new LogicalFileScan(new RelationId(13), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(secondSnapshot));

        Assertions.assertFalse(first.hasSameScanState(second));
    }

    private static MvccSnapshot icebergSnapshot(long snapshotId, long schemaId) {
        return new IcebergMvccSnapshot(new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(snapshotId, schemaId)));
    }

    private static MvccSnapshot icebergSnapshot(long snapshotId, long schemaId,
            Map<Integer, List<String>> nameMapping) {
        return new IcebergMvccSnapshot(new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(snapshotId, schemaId), Optional.of(nameMapping)));
    }

    @Test
    public void testHudiCopiesKeepRelationSnapshotAndSchema() {
        MvccSnapshot historicalSnapshot = Mockito.mock(MvccSnapshot.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Optional.of(historicalSnapshot)))
                .thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema(Optional.of(historicalSnapshot)))
                .thenReturn(Collections.singletonList(new Column("old_name", Type.INT, true)));
        Mockito.when(table.getName()).thenReturn("hudi_table");

        LogicalHudiScan scan = new LogicalHudiScan(new RelationId(9), table,
                Collections.singletonList("db"), Collections.emptyList(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(historicalSnapshot));
        LogicalHudiScan copy = scan.withCachedOutput(scan.computeOutput());

        Assertions.assertSame(historicalSnapshot, copy.getRelationSnapshot().orElseThrow(AssertionError::new));
        Assertions.assertEquals(Collections.singletonList("old_name"),
                copy.computeOutput().stream().map(Slot::getName).collect(Collectors.toList()));
    }
}
