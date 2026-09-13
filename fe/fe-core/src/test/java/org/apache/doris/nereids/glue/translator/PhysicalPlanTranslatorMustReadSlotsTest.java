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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Guards {@code PhysicalPlanTranslator.preserveConnectorMustReadSlots} — the plugin-table branch next to
 * {@code preserveExtraStorageKeySlots}, which keeps the slots a connector says its BE-side reader must read
 * even when the query references none of them.
 *
 * <p><b>WHY this matters (Rule 9):</b> the scan's tuple is where the columns BE reads are decided
 * ({@code FileQueryScanNode.updateRequiredSlots} rebuilds the required-slot list from exactly these slots
 * after planning). A reader that suppresses or merges rows by key and does not get the key back reads the
 * key-less rows and emits duplicates — no error anywhere. Doris does the same preservation for its own
 * aggregate / merge-on-read unique-key tables; this is that mechanism for plugin connectors.</p>
 *
 * <p>These tests drive the extracted static entry point directly with a real {@link TupleDescriptor} and a
 * {@code CALLS_REAL_METHODS} node whose connector answer is stubbed — building a translator over a live
 * plugin catalog needs a harness this module does not have. What they do NOT cover, because it is decided
 * before this branch runs and by generic code: the project above the scan gets its own output tuple, so a
 * column preserved here is read and then dropped rather than returned. The fluss suites' row baselines are
 * the end-to-end guard for that.</p>
 */
public class PhysicalPlanTranslatorMustReadSlotsTest {

    private static final DescriptorTable DESC_TABLE = new DescriptorTable();

    /** A scan tuple holding one slot per named column, in order. */
    private static TupleDescriptor tupleOf(String... columnNames) {
        TupleDescriptor tuple = DESC_TABLE.createTupleDescriptor();
        for (String name : columnNames) {
            SlotDescriptor slot = DESC_TABLE.addSlotDescriptor(tuple);
            slot.setColumn(new Column(name, PrimitiveType.INT));
        }
        return tuple;
    }

    private static PluginDrivenScanNode nodeAnswering(TupleDescriptor tuple, Set<String> mustRead) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(tuple).when(node).getTupleDesc();
        Mockito.doReturn(mustRead).when(node).mustReadColumnsFromConnector();
        return node;
    }

    private static SlotId slotIdOf(TupleDescriptor tuple, String columnName) {
        for (SlotDescriptor slot : tuple.getSlots()) {
            if (slot.getColumn().getName().equals(columnName)) {
                return slot.getId();
            }
        }
        throw new IllegalStateException("no slot for " + columnName);
    }

    @Test
    public void connectorNamedColumnsSurvivePruning() {
        TupleDescriptor tuple = tupleOf("id", "name", "amount");
        PluginDrivenScanNode node = nodeAnswering(tuple, ImmutableSet.of("id"));
        // "select name": only that slot is required by the project above the scan.
        Set<SlotId> required = Sets.newHashSet(slotIdOf(tuple, "name"));

        PhysicalPlanTranslator.preserveConnectorMustReadSlots(node, required);

        // WHY: 'id' is what the connector's reader needs to suppress rows; without it in the required set
        // the removeIf below this call drops it from the tuple and BE reads key-less rows. MUTATION:
        // dropping the branch (or the add) -> 'id' absent -> red.
        Assertions.assertEquals(ImmutableSet.of(slotIdOf(tuple, "name"), slotIdOf(tuple, "id")), required);
    }

    @Test
    public void connectorThatNeedsNothingChangesNothing() {
        TupleDescriptor tuple = tupleOf("id", "name", "amount");
        PluginDrivenScanNode node = nodeAnswering(tuple, Collections.emptySet());
        Set<SlotId> required = Sets.newHashSet(slotIdOf(tuple, "name"));

        PhysicalPlanTranslator.preserveConnectorMustReadSlots(node, required);

        // WHY: this is the gate that keeps the branch inert for every connector that never opted in — and
        // for an opted-in connector on a scan it decided NOT to combine (a fluss table read from fluss
        // alone), which is exactly the "only when it is really needed" requirement. MUTATION: preserving
        // unconditionally (e.g. the whole primary key regardless of the decision) -> an extra slot -> red.
        Assertions.assertEquals(Collections.singleton(slotIdOf(tuple, "name")), required);
    }

    @Test
    public void everyNamedColumnIsPreservedNotJustTheFirst() {
        TupleDescriptor tuple = tupleOf("k1", "k2", "payload");
        PluginDrivenScanNode node = nodeAnswering(tuple, ImmutableSet.of("k1", "k2"));
        Set<SlotId> required = Sets.newHashSet(slotIdOf(tuple, "payload"));

        PhysicalPlanTranslator.preserveConnectorMustReadSlots(node, required);

        // WHY: composite keys are the normal case for the readers this exists for; keeping only one column
        // of a two-column key compares the wrong thing. MUTATION: `break` after the first match -> red.
        Assertions.assertEquals(
                ImmutableSet.of(slotIdOf(tuple, "payload"), slotIdOf(tuple, "k1"), slotIdOf(tuple, "k2")),
                required);
    }

    @Test
    public void preservedSlotSurvivesThePruneItself() {
        TupleDescriptor tuple = tupleOf("id", "name", "amount");
        PluginDrivenScanNode node = nodeAnswering(tuple, ImmutableSet.of("id"));
        Set<SlotId> required = Sets.newHashSet(slotIdOf(tuple, "name"));

        // The real prune step, driven end to end: it is what decides which slots the scan reads, and the
        // branch under test sits inside it.
        Deencapsulation.invoke(new PhysicalPlanTranslator(), "updateScanSlotsMaterialization",
                node, required, Sets.newHashSet(), new PlanTranslatorContext());

        // WHY: this is the only assertion that also pins the DISPATCH — that a plugin-driven scan reaches
        // the branch at all. MUTATION: deleting the `else if (scanNode instanceof PluginDrivenScanNode)`
        // arm -> 'id' pruned away -> red. MUTATION: preserving AFTER the removeIf -> also red.
        Assertions.assertEquals(ImmutableList.of("id", "name"),
                tuple.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList()),
                "the connector's column must be read; the unreferenced one must still be pruned");
    }

    @Test
    public void columnTheScanDoesNotHaveFailsLoud() {
        TupleDescriptor tuple = tupleOf("id", "name");
        PluginDrivenScanNode node = nodeAnswering(tuple, ImmutableSet.of("id", "ghost"));
        Set<SlotId> required = Sets.newHashSet(slotIdOf(tuple, "name"));

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class,
                () -> PhysicalPlanTranslator.preserveConnectorMustReadSlots(node, required));

        // WHY: a name matching no slot means the connector and the engine disagree about the table. Reading
        // on would hand the reader a scan without a column it said it needs — wrong rows, silently. The
        // message must name the column, because that is the only clue to which side is stale. MUTATION:
        // skipping unknown names instead of throwing -> no exception -> red.
        Assertions.assertTrue(thrown.getMessage().contains("ghost"),
                "the failure must name the column the scan does not have: " + thrown.getMessage());
    }
}
