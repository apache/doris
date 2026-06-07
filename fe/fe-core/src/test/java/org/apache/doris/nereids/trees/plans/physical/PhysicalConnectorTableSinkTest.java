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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link PhysicalConnectorTableSink#getRequirePhysicalProperties()} (FIX-WRITE-DISTRIBUTION,
 * NG-2 / NG-4). After the MaxCompute SPI cutover the generic connector sink replaces legacy
 * {@code PhysicalMaxComputeTableSink}; this pins that it reproduces the legacy 3-branch distribution,
 * gated by connector capabilities:
 *
 * <ul>
 *   <li><b>dynamic-partition write</b> (a partition column present in {@code cols}) + connector
 *       declaring {@code SINK_REQUIRE_PARTITION_LOCAL_SORT} → hash-by-partition + mandatory local sort,
 *       so the MaxCompute Storage API streaming partition writer does not hit "writer has been
 *       closed" on un-grouped rows;</li>
 *   <li><b>non-partition / all-static write</b> + {@code SUPPORTS_PARALLEL_WRITE} →
 *       {@code SINK_RANDOM_PARTITIONED} (parallel writers, NG-4 parity);</li>
 *   <li><b>capability-less connector</b> (jdbc/es-like) → {@code GATHER} (single writer).</li>
 * </ul>
 *
 * <p><b>Index by {@code cols}, not full schema:</b> the generic sink's child output is aligned to
 * {@code cols} (BindSink.bindConnectorTableSink projects to cols order), so the hash/sort keys are the
 * child slots at the partition columns' <em>cols</em> positions — the tests assert that exact slot.</p>
 */
public class PhysicalConnectorTableSinkTest {

    private static final Column DATA = new Column("data", PrimitiveType.INT);
    private static final Column PART = new Column("part", PrimitiveType.INT);

    /**
     * Dynamic-partition write: the partition column 'part' is present in cols (its value comes from
     * the query), so the sink must hash-distribute and locally sort by 'part'.
     */
    @Test
    public void dynamicPartitionWriteRequiresHashAndLocalSort() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        // cols == [data, part] (part is dynamic), child output aligned 1:1 with cols.
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(PART)),
                Arrays.asList(DATA, PART),
                ImmutableList.of(dataSlot, partSlot));

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHiveTableSinkHashPartitioned,
                "dynamic-partition write must hash-distribute by partition columns");
        DistributionSpecHiveTableSinkHashPartitioned dist =
                (DistributionSpecHiveTableSinkHashPartitioned) props.getDistributionSpec();
        // The hash key is the child slot at 'part's cols position (index 1) — NOT a full-schema index.
        Assertions.assertEquals(ImmutableList.of(partSlot.getExprId()), dist.getOutputColExprIds(),
                "hash key must be the partition-column slot taken at its cols position");
        Assertions.assertTrue(props.getOrderSpec() instanceof MustLocalSortOrderSpec,
                "dynamic-partition write must require a mandatory local sort to group partition rows");
        List<OrderKey> orderKeys = props.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(1, orderKeys.size(), "exactly one partition column to sort by");
        Assertions.assertEquals(partSlot, orderKeys.get(0).getExpr(),
                "local sort must be on the partition column");
    }

    /**
     * All-static-partition write: every partition column is statically specified and therefore
     * absent from cols, so no grouping/sort is needed — parallel writers (RANDOM), matching legacy
     * branch-2. This unit-tests the in-sink fall-through over a cols-already-stripped input. That
     * input is produced by the bind layer in two reachable forms: the explicit-column-list form
     * {@code INSERT INTO mc PARTITION(part='x') (data) SELECT data} today (colNames=[data] →
     * bindColumns=[data]); and the no-column-list form {@code INSERT INTO mc PARTITION(part='x')
     * SELECT data} once P0-3 / FIX-BIND-STATIC-PARTITION makes bindConnectorTableSink exclude
     * static-partition columns (today that no-column-list form throws at bind — NG-3 — so this
     * branch is dormant for it, never silently mis-classified as dynamic).
     */
    @Test
    public void allStaticPartitionWriteUsesRandomPartitioned() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(PART)),
                Arrays.asList(DATA),
                ImmutableList.of(dataSlot));

        Assertions.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties(),
                "an all-static-partition write needs no sort/shuffle and uses parallel writers");
    }

    /**
     * Non-partitioned write with a parallel-write connector → parallel writers (RANDOM), the NG-4
     * parity case (the bug degraded this to GATHER).
     */
    @Test
    public void nonPartitionedWriteUsesRandomWhenParallel() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of()),
                Arrays.asList(DATA),
                ImmutableList.of(dataSlot));

        Assertions.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties(),
                "a non-partitioned write on a parallel-write connector must use parallel writers, not GATHER");
    }

    /**
     * Capability-less connector (jdbc/es-like): no parallel-write, no partition-sort → GATHER. Guards
     * that the change did not broaden parallel/sort behavior to connectors that did not opt in.
     */
    @Test
    public void capabilityLessConnectorGathers() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(false, false, ImmutableList.of()),
                Arrays.asList(DATA),
                ImmutableList.of(dataSlot));

        Assertions.assertSame(PhysicalProperties.GATHER, sink.getRequirePhysicalProperties(),
                "a connector declaring neither capability must keep the single-writer GATHER default");
    }

    // ==================== helpers ====================

    private static PluginDrivenExternalTable table(boolean parallelWrite, boolean requirePartitionSort,
            List<Column> partitionColumns) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsParallelWrite()).thenReturn(parallelWrite);
        Mockito.when(table.requirePartitionLocalSortOnWrite()).thenReturn(requirePartitionSort);
        Mockito.when(table.getPartitionColumns()).thenReturn(partitionColumns);
        return table;
    }

    /**
     * Builds a {@link PhysicalConnectorTableSink} exercising only {@code getRequirePhysicalProperties()}.
     * Uses CALLS_REAL_METHODS to skip the heavyweight ctor and injects the three fields the method
     * reads ({@code targetTable}, {@code cols}, and the single child via the {@code children} field, so
     * the real {@code child()} resolves to it).
     */
    private static PhysicalConnectorTableSink<Plan> sink(PluginDrivenExternalTable table,
            List<Column> cols, List<Slot> childOutput) {
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(child.getOutput()).thenReturn(childOutput);
        @SuppressWarnings("unchecked")
        PhysicalConnectorTableSink<Plan> sink =
                Mockito.mock(PhysicalConnectorTableSink.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(sink, "targetTable", table);
        Deencapsulation.setField(sink, "cols", cols);
        Deencapsulation.setField(sink, "children", ImmutableList.of(child));
        return sink;
    }
}
