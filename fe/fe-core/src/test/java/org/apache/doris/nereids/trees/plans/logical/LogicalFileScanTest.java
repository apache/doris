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
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
        Column rowIdColumn = new Column("_row_id", Type.BIGINT, true);
        rowIdColumn.setIsVisible(false);
        Column lastUpdatedSequenceNumberColumn =
                new Column("_last_updated_sequence_number", Type.BIGINT, true);
        lastUpdatedSequenceNumberColumn.setIsVisible(false);
        List<Column> schema = Arrays.asList(
                new Column("id", Type.INT, true),
                rowIdColumn,
                lastUpdatedSequenceNumberColumn);

        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        // The snapshot-taking overload: computePluginDrivenOutput resolves THIS reference's version and
        // asks for the schema AS OF it (empty here — this reference carries no selector).
        Mockito.when(table.getFullSchema(Mockito.any())).thenReturn(schema);
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");

        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                Collections.singletonList("db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        List<String> outputNames = scan.computeOutput().stream().map(slot -> slot.getName())
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(
                "id",
                "_row_id",
                "_last_updated_sequence_number"), outputNames);
    }

    @Test
    public void computeOutputBindsThisReferencesOwnVersionNotLatest() {
        // The tag_branch self-join shape: ONE table referenced at TWO tags, no bare reference. Each
        // reference must bind the schema of the tag IT names. The version-BLIND lookup cannot tell the two
        // references apart, gives up, and hands BOTH of them the LATEST schema — a schema NO reference
        // asked for — which then makes the scan-time guard fire on a column the query never referenced.
        List<Column> tagT1Schema = Collections.singletonList(new Column("c1", Type.INT, true));
        List<Column> latestSchema = Arrays.asList(
                new Column("c1", Type.INT, true),
                new Column("c2", Type.INT, true));

        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("tag_branch_table");
        Mockito.when(table.getDatabase()).thenReturn((ExternalDatabase) database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn((CatalogIf) catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);

        TableScanParams tagT1 = new TableScanParams("tag", ImmutableMap.of(), ImmutableList.of("t1"));
        TableScanParams tagT2 = new TableScanParams("tag", ImmutableMap.of(), ImmutableList.of("t2"));
        MvccSnapshot pinT1 = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot pinT2 = Mockito.mock(MvccSnapshot.class);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(tagT1))).thenReturn(pinT1);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.of(tagT2))).thenReturn(pinT2);
        Mockito.when(table.getFullSchema(Optional.of(pinT1))).thenReturn(tagT1Schema);
        // What a version-blind resolution yields once two versions are pinned: no pin resolved -> latest.
        Mockito.when(table.getFullSchema(Optional.empty())).thenReturn(latestSchema);

        ConnectContext ctx = new ConnectContext();
        StatementContext stmtCtx = new StatementContext(ctx, null);
        ctx.setStatementContext(stmtCtx);
        ctx.setThreadLocalInfo();
        try {
            // Pin via loadSnapshots (not setSnapshot) so the version key is computed by the SAME function
            // the lookup uses — the test must not hand-roll a key and accidentally agree with itself.
            stmtCtx.loadSnapshots(table, Optional.empty(), Optional.of(tagT1));
            stmtCtx.loadSnapshots(table, Optional.empty(), Optional.of(tagT2));

            LogicalFileScan scan = new LogicalFileScan(new RelationId(3), table,
                    Collections.singletonList("db"), Collections.emptyList(),
                    Optional.empty(), Optional.empty(), Optional.of(tagT1), Optional.empty());

            List<String> outputNames = scan.computeOutput().stream().map(Slot::getName)
                    .collect(Collectors.toList());
            // MUTATION: reverting computePluginDrivenOutput to the version-blind getFullSchema() makes this
            // red with [c1, c2] — c2 being the phantom column that CI 996541's guard reported on.
            Assertions.assertEquals(Collections.singletonList("c1"), outputNames,
                    "a @tag(t1) reference must bind t1's schema even though the statement also pins t2");
        } finally {
            ConnectContext.remove();
        }
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
