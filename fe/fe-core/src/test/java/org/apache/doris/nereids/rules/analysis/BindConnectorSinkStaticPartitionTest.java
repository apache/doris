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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for {@link BindSink#selectConnectorSinkBindColumns} — the bind-time column selection for the
 * generic connector table sink (FIX-BIND-STATIC-PARTITION, P0-3).
 *
 * <p>Root cause this guards: before the fix, the no-column-list path bound the full base schema
 * (including partition columns), so {@code INSERT INTO mc PARTITION(pt='x') SELECT <non-partition cols>}
 * produced more bound columns than the query output and threw "insert into cols should be corresponding
 * to the query output" at bind. The static partition columns carry their value via the static partition
 * spec (not the query), so they must be excluded from the bound columns — mirroring legacy
 * {@code bindMaxComputeTableSink}.</p>
 */
public class BindConnectorSinkStaticPartitionTest {

    private static final Column ID = new Column("id", PrimitiveType.INT);
    private static final Column VAL = new Column("val", PrimitiveType.INT);
    private static final Column DS = new Column("ds", PrimitiveType.INT);
    private static final Column REGION = new Column("region", PrimitiveType.INT);
    // Base schema appends partition columns after the data columns (as the connector reports it).
    private static final List<Column> BASE_SCHEMA = ImmutableList.of(ID, VAL, DS, REGION);

    private static PluginDrivenExternalTable partitionedTable() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(BASE_SCHEMA);
        for (Column c : BASE_SCHEMA) {
            Mockito.when(table.getColumn(c.getName())).thenReturn(c);
        }
        return table;
    }

    /**
     * A table carrying an invisible column after the visible data columns, modelling an iceberg v3 table
     * whose row-lineage {@code _row_id} is appended {@code .invisible()} by the connector.
     */
    private static PluginDrivenExternalTable tableWithRowLineage() {
        Column rowId = new Column("_row_id", PrimitiveType.BIGINT);
        rowId.setIsVisible(false);
        List<Column> schema = ImmutableList.of(ID, VAL, rowId);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
        for (Column c : schema) {
            Mockito.when(table.getColumn(c.getName())).thenReturn(c);
        }
        return table;
    }

    private static List<String> names(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    /**
     * No column list, all-static {@code PARTITION(ds='x', region='y')}: both partition columns are
     * statically specified and must be excluded from the bound columns, leaving only the data columns
     * so the count matches the query output (the original blocker).
     */
    @Test
    public void noColumnListAllStaticExcludesPartitionColumns() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds", "region"), false);
        Assertions.assertEquals(ImmutableList.of("id", "val"), names(bound),
                "static partition columns must be excluded from the bound columns");
    }

    /**
     * No column list, partial-static {@code PARTITION(ds='x') SELECT id, val, region}: only the static
     * 'ds' is excluded; the dynamic 'region' stays (its value comes from the query).
     */
    @Test
    public void noColumnListPartialStaticExcludesOnlyStaticColumn() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds"), false);
        Assertions.assertEquals(ImmutableList.of("id", "val", "region"), names(bound),
                "only the statically-specified partition column must be excluded");
    }

    /**
     * No column list, no static partition (pure dynamic, e.g. {@code INSERT ... SELECT id,val,ds,region}):
     * nothing is excluded — the full base schema is bound, so the existing dynamic/JDBC path is
     * unchanged.
     */
    @Test
    public void noColumnListNoStaticPartitionBindsFullSchema() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), Collections.emptyList(), Collections.emptySet(), false);
        Assertions.assertEquals(ImmutableList.of("id", "val", "ds", "region"), names(bound),
                "without a static partition spec the full base schema is bound");
    }

    /**
     * Explicit column list: bound columns follow the user-specified list verbatim and are not affected
     * by the static partition spec (the user already chose which columns the query provides).
     */
    @Test
    public void explicitColumnListUsesUserColumnsVerbatim() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                partitionedTable(), ImmutableList.of("val", "id"), ImmutableSet.of("ds"), false);
        Assertions.assertEquals(ImmutableList.of("val", "id"), names(bound),
                "explicit column list is bound in user order, unaffected by static partitions");
    }

    /**
     * Explicit column list naming an unknown column fails loud with a clear message (unchanged behavior).
     */
    @Test
    public void explicitColumnListUnknownColumnThrows() {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                BindSink.selectConnectorSinkBindColumns(
                        partitionedTable(), ImmutableList.of("nope"), Collections.emptySet(), false));
        Assertions.assertTrue(ex.getMessage().contains("nope"), "error must name the missing column");
    }

    /**
     * No column list, ordinary write (not a rewrite): invisible columns (e.g. iceberg v3 row-lineage
     * {@code _row_id} / {@code _last_updated_sequence_number}) must be EXCLUDED from the default bound
     * columns — the user never supplies their values, so including them would make the bound-column
     * count exceed the query output and throw "insert into cols should be corresponding to the query
     * output". Guards the v3 row-lineage INSERT regression (test_iceberg_v2_to_v3_doris_spark_compare).
     */
    @Test
    public void noColumnListOrdinaryWriteExcludesInvisibleColumns() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                tableWithRowLineage(), Collections.emptyList(), Collections.emptySet(), false);
        Assertions.assertEquals(ImmutableList.of("id", "val"), names(bound),
                "invisible row-lineage columns must be excluded from an ordinary write target");
    }

    /**
     * No column list, rewrite (distributed {@code rewrite_data_files}): invisible columns are RETAINED so
     * the engine-managed row-lineage values read from the source rows are preserved through the rewrite,
     * mirroring the legacy {@code bindIcebergTableSink} rewrite branch.
     */
    @Test
    public void noColumnListRewriteRetainsInvisibleColumns() {
        List<Column> bound = BindSink.selectConnectorSinkBindColumns(
                tableWithRowLineage(), Collections.emptyList(), Collections.emptySet(), true);
        Assertions.assertEquals(ImmutableList.of("id", "val", "_row_id"), names(bound),
                "a rewrite must retain invisible row-lineage columns to preserve their values");
    }
}
