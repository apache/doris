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

package org.apache.doris.datasource.scan;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link PluginDrivenScanNode}'s per-reference pinned-schema resolution — the seam that lets a
 * statement reading the SAME table at two versions (e.g. a self-join {@code FOR VERSION AS OF} across a
 * rename) map each scan's file columns against the schema IT actually reads, instead of the table's
 * version-blind latest schema (the {@code Column ... not found in table} failure at
 * {@link FileQueryScanNode#setColumnPositionMapping}). {@link PluginDrivenScanNode#pinnedSchemaOrNull} and
 * {@link PluginDrivenScanNode#visibleColumns} are pure, so they are exercised without constructing a scan
 * node (mirrors {@code PluginDrivenScanNodeMvccSchemaGuardTest}).
 */
public class PluginDrivenScanNodePinnedSchemaTest {

    private static Column col(String name, boolean visible) {
        return new Column(name, Type.INT, false, null, true, "", visible);
    }

    @Test
    public void pinnedSnapshotResolvesEachReferenceOwnSchema() {
        // The bug: two references to the same table at different versions collapsed to one (latest) schema.
        // With a per-reference pin, the OLD reference resolves the OLD schema (old_name) and the NEW reference
        // the NEW schema (new_name). MUTATION: ignoring the snapshot -> both return the same schema -> red.
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Optional<MvccSnapshot> oldPin = Optional.of(Mockito.mock(MvccSnapshot.class));
        Optional<MvccSnapshot> newPin = Optional.of(Mockito.mock(MvccSnapshot.class));
        List<Column> oldSchema = Collections.singletonList(col("old_name", true));
        List<Column> newSchema = Collections.singletonList(col("new_name", true));
        Mockito.when(table.getFullSchema(oldPin)).thenReturn(oldSchema);
        Mockito.when(table.getFullSchema(newPin)).thenReturn(newSchema);

        Assertions.assertSame(oldSchema, PluginDrivenScanNode.pinnedSchemaOrNull(table, oldPin));
        Assertions.assertSame(newSchema, PluginDrivenScanNode.pinnedSchemaOrNull(table, newPin));
    }

    @Test
    public void noExplicitPinFallsBackToNull() {
        // An empty snapshot means THIS reference has no pin (latest): return null so the caller keeps the
        // ambient version-blind default getFullSchema(), NOT getFullSchema(Optional.empty()).
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Assertions.assertNull(PluginDrivenScanNode.pinnedSchemaOrNull(table, Optional.empty()));
        Mockito.verify(table, Mockito.never()).getFullSchema(Mockito.any());
    }

    @Test
    public void nonPluginTableFallsBackToNull() {
        // A non-plugin table (internal / doris-external / TVF) never carries a plugin pin -> null -> the caller
        // keeps its own version-blind default. Guards the instanceof gate.
        TableIf table = Mockito.mock(TableIf.class);
        Assertions.assertNull(
                PluginDrivenScanNode.pinnedSchemaOrNull(table, Optional.of(Mockito.mock(MvccSnapshot.class))));
    }

    @Test
    public void visibleColumnsDropsHiddenParityWithGetBaseSchemaFalse() {
        // numOfColumnsFromFile counts only visible file columns (ExternalTable.getBaseSchema(false) parity):
        // a synthetic/hidden write column must be filtered out. MUTATION: dropping the filter -> the hidden
        // column inflates the count -> BE reads the wrong number of file columns.
        List<Column> withHidden = Arrays.asList(col("id", true), col("name", true), col("__hidden__", false));
        List<Column> visible = PluginDrivenScanNode.visibleColumns(withHidden);
        Assertions.assertEquals(2, visible.size());
        Assertions.assertTrue(visible.stream().allMatch(Column::isVisible));
    }
}
