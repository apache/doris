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
import org.apache.doris.datasource.PluginDrivenExternalTable;
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
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds", "region"));
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
                partitionedTable(), Collections.emptyList(), ImmutableSet.of("ds"));
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
                partitionedTable(), Collections.emptyList(), Collections.emptySet());
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
                partitionedTable(), ImmutableList.of("val", "id"), ImmutableSet.of("ds"));
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
                        partitionedTable(), ImmutableList.of("nope"), Collections.emptySet()));
        Assertions.assertTrue(ex.getMessage().contains("nope"), "error must name the missing column");
    }
}
