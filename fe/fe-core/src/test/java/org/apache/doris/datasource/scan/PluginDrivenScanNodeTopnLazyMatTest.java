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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Column;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

/**
 * Guards {@link PluginDrivenScanNode#hasTopnLazyMaterializeSlot}, the connector-agnostic detection of the
 * engine-wide Top-N lazy-materialization row-id slot ({@code __DORIS_GLOBAL_ROWID_COL__}).
 *
 * <p><b>Why this matters (M-4):</b> under Top-N lazy materialization BE reads the sort key first, then
 * re-fetches the OTHER (non-projected) columns of the surviving rows by row-id. {@code pinTopnLazyMaterialize}
 * uses this detection to signal the connector (via {@code ConnectorMetadata.applyTopnLazyMaterialization})
 * that its column-pruned scan metadata (iceberg's field-id dictionary) must be rebuilt over the FULL schema —
 * otherwise a lazily re-fetched, schema-evolved column has no field-id entry and the native read mis-reads it.
 * The detection is the engine-wide GLOBAL_ROWID prefix test (a generic Doris mechanism, also used by
 * {@code classifyColumn} / {@code HiveScanNode} / {@code TVFScanNode}), so no connector knowledge leaks into
 * fe-core. Mirrors legacy {@code IcebergScanNode.createScanRangeLocations}'s {@code haveTopnLazyMatCol}.</p>
 */
public class PluginDrivenScanNodeTopnLazyMatTest {

    private static SlotDescriptor slotNamed(String name) {
        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        Column column = Mockito.mock(Column.class);
        Mockito.doReturn(name).when(column).getName();
        Mockito.doReturn(column).when(slot).getColumn();
        return slot;
    }

    @Test
    public void detectsSuffixedGlobalRowIdSlot() {
        // LazyMaterializeTopN appends the table/function name to the prefix, so the test must be startsWith,
        // not equals. MUTATION: startsWith -> equals drops the suffixed name -> detection false -> the dict
        // stays pruned -> a schema-evolved lazy re-fetch mis-reads -> red.
        Assertions.assertTrue(PluginDrivenScanNode.hasTopnLazyMaterializeSlot(
                Collections.singletonList(slotNamed(Column.GLOBAL_ROWID_COL + "my_tbl"))));
    }

    @Test
    public void detectsGlobalRowIdAmongRegularSlots() {
        // The row-id slot co-exists with the projected sort key(s). MUTATION: scanning only the first slot /
        // not iterating -> the row-id after "id" is missed -> red.
        Assertions.assertTrue(PluginDrivenScanNode.hasTopnLazyMaterializeSlot(
                Arrays.asList(slotNamed("id"), slotNamed(Column.GLOBAL_ROWID_COL + "t"))));
    }

    @Test
    public void noGlobalRowIdSlotIsNotTopn() {
        // WHY: a normal (non-lazy-mat) scan must NOT be flagged, or the connector would drop its column-pruning
        // optimization (full-schema dict) for every query. MUTATION: returning true unconditionally -> red.
        Assertions.assertFalse(PluginDrivenScanNode.hasTopnLazyMaterializeSlot(
                Arrays.asList(slotNamed("id"), slotNamed("name"))));
    }

    @Test
    public void emptySlotsIsNotTopn() {
        Assertions.assertFalse(PluginDrivenScanNode.hasTopnLazyMaterializeSlot(Collections.emptyList()));
    }

    @Test
    public void nullColumnSlotIsSkippedWithoutNpe() {
        // A slot with no bound column (slot.getColumn() == null) must be skipped, not throw. MUTATION:
        // dropping the null guard -> NPE here -> red.
        SlotDescriptor nullColSlot = Mockito.mock(SlotDescriptor.class);
        Mockito.doReturn(null).when(nullColSlot).getColumn();
        Assertions.assertFalse(PluginDrivenScanNode.hasTopnLazyMaterializeSlot(
                Collections.singletonList(nullColSlot)));
    }
}
