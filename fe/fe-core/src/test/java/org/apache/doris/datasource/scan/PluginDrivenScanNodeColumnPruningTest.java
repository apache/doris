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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Guards {@link PluginDrivenScanNode}'s column projection — the engine-side half of connector column
 * pruning.
 *
 * <p><b>Why this matters:</b> a connector never decides which columns to read; it renders whatever list
 * {@code ConnectorScanRequest.getColumns()} carries (e.g. the jdbc connector turns it verbatim into the
 * remote {@code SELECT} list, falling back to {@code SELECT *} when it is empty). That list is produced
 * HERE, by intersecting the connector's column handles with this scan's tuple slots — the slots Nereids'
 * {@code ColumnPruning} + {@code PhysicalPlanTranslator.updateScanSlotsMaterialization} already pruned to
 * what the query needs. So every "pruning stopped working" failure mode lands in this method, and it had
 * no direct coverage: returning all handles instead of the slot-matched ones silently turns every external
 * scan into a full-width read (for jdbc, a literal {@code SELECT *} against the remote database), which is
 * a pure performance regression that no result-comparing test can see.</p>
 *
 * <p>The projection is driven through the real private {@code buildColumnHandles} on a
 * {@code CALLS_REAL_METHODS} node with only its collaborator fields injected: with no {@code ConnectContext}
 * the MVCC lookup resolves empty and the pinned-schema branch (covered by
 * {@link PluginDrivenScanNodePinnedSchemaTest}) is not taken, leaving exactly the slot-intersection logic
 * under test.</p>
 */
public class PluginDrivenScanNodeColumnPruningTest {

    private static SlotDescriptor slotFor(String columnName) {
        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        Mockito.when(slot.getColumn()).thenReturn(new Column(columnName, PrimitiveType.INT));
        return slot;
    }

    /** A slot with no backing table column (a synthetic/derived slot), which must never be projected. */
    private static SlotDescriptor slotWithoutColumn() {
        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        Mockito.when(slot.getColumn()).thenReturn(null);
        return slot;
    }

    /**
     * A node whose connector metadata exposes {@code handles} for the table and whose tuple carries
     * {@code slots}. {@code cachedMetadata} is injected directly so the per-statement metadata funnel
     * (which needs a live session scope) stays out of this test.
     */
    private static PluginDrivenScanNode nodeWith(Map<String, ConnectorColumnHandle> handles,
            SlotDescriptor... slots) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);

        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.getColumnHandles(Mockito.any(), Mockito.any())).thenReturn(handles);
        Deencapsulation.setField(node, "cachedMetadata", metadata);

        TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
        ArrayList<SlotDescriptor> slotList = new ArrayList<>();
        for (SlotDescriptor slot : slots) {
            slotList.add(slot);
        }
        Mockito.when(desc.getSlots()).thenReturn(slotList);
        // getTargetTable() reads through the tuple; a plain table keeps this off both the sys-table pin
        // and the time-travel pinned-schema branches.
        Mockito.when(desc.getTable()).thenReturn(Mockito.mock(TableIf.class));
        Deencapsulation.setField(node, "desc", desc);

        return node;
    }

    private static Map<String, ConnectorColumnHandle> handles(String... names) {
        Map<String, ConnectorColumnHandle> map = new LinkedHashMap<>();
        for (String name : names) {
            map.put(name, Mockito.mock(ConnectorColumnHandle.class, "handle:" + name));
        }
        return map;
    }

    @Test
    public void testOnlyTupleSlotColumnsAreProjected() {
        // THE pruning guarantee: a 3-column table queried for 1 column projects exactly that 1 handle.
        // MUTATION: returning allHandles.values() (or dropping the slot loop) makes this return 3 -> red,
        // and downstream would make the jdbc connector emit all three columns in its remote SELECT.
        Map<String, ConnectorColumnHandle> all = handles("c1", "c2", "c3");
        PluginDrivenScanNode node = nodeWith(all, slotFor("c2"));

        List<ConnectorColumnHandle> selected = Deencapsulation.invoke(node, "buildColumnHandles");

        Assertions.assertEquals(1, selected.size(),
                "only the queried column may be projected, got: " + selected);
        Assertions.assertSame(all.get("c2"), selected.get(0));
    }

    @Test
    public void testProjectionOrderFollowsSlotOrderNotHandleOrder() {
        // The connector renders this list positionally (the jdbc SELECT list order IS the order BE maps
        // result columns back onto the scan's slots), so the order must come from the tuple slots, not from
        // the connector's handle map. MUTATION: iterating allHandles instead of the slots yields [c1, c3].
        Map<String, ConnectorColumnHandle> all = handles("c1", "c2", "c3");
        PluginDrivenScanNode node = nodeWith(all, slotFor("c3"), slotFor("c1"));

        List<ConnectorColumnHandle> selected = Deencapsulation.invoke(node, "buildColumnHandles");

        Assertions.assertEquals(2, selected.size());
        Assertions.assertSame(all.get("c3"), selected.get(0), "slot order must win over handle-map order");
        Assertions.assertSame(all.get("c1"), selected.get(1));
    }

    @Test
    public void testSlotWithoutColumnIsSkipped() {
        // A slot with no backing column has no name to look up; projecting it would need a handle that
        // cannot exist. Pins the `slot.getColumn() != null` guard against an NPE regression.
        Map<String, ConnectorColumnHandle> all = handles("c1", "c2");
        PluginDrivenScanNode node = nodeWith(all, slotWithoutColumn(), slotFor("c1"));

        List<ConnectorColumnHandle> selected = Deencapsulation.invoke(node, "buildColumnHandles");

        Assertions.assertEquals(1, selected.size());
        Assertions.assertSame(all.get("c1"), selected.get(0));
    }

    @Test
    public void testSlotWithNoMatchingHandleIsDropped() {
        // Without a pinned time-travel schema the drop is deliberately silent (the fail-loud path is gated
        // on supportsColumnHandleSnapshotPin) — pinned here so a future change to that gate is a conscious
        // one, and so the unmatched slot can never leak a null into the projected list.
        Map<String, ConnectorColumnHandle> all = handles("c1");
        PluginDrivenScanNode node = nodeWith(all, slotFor("c1"), slotFor("gone"));

        List<ConnectorColumnHandle> selected = Deencapsulation.invoke(node, "buildColumnHandles");

        Assertions.assertEquals(1, selected.size());
        Assertions.assertSame(all.get("c1"), selected.get(0));
    }

    @Test
    public void testEmptyTupleProjectsNothing() {
        // A tuple with no slots projects nothing — the ONLY input that makes the jdbc connector fall back to
        // `SELECT *`. This is what a `count(*)` scan would look like if the engine's keep-the-smallest-column
        // fallback (ColumnPruning / PhysicalPlanTranslator.updateScanSlotsMaterialization) ever stopped
        // firing, so pinning it keeps that fallback reachable ONLY from an empty tuple, never from a
        // pruning bug that happens to drop every slot.
        PluginDrivenScanNode node = nodeWith(handles("c1", "c2", "c3"));

        List<ConnectorColumnHandle> selected = Deencapsulation.invoke(node, "buildColumnHandles");

        Assertions.assertTrue(selected.isEmpty(), "an empty tuple must project nothing, got: " + selected);
    }
}
