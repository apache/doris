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
 * NG-2 / NG-4; revised by FIX-BIND-STATIC-PARTITION, P0-3). After the MaxCompute SPI cutover the generic
 * connector sink replaces legacy {@code PhysicalMaxComputeTableSink}; this pins that it reproduces the
 * legacy 3-branch distribution, gated by connector capabilities:
 *
 * <ul>
 *   <li><b>dynamic-partition write</b> (a partition column present in {@code cols}) + connector's write
 *       provider returning {@code true} from {@code requiresPartitionLocalSort()} → hash-by-partition +
 *       mandatory local sort, so the MaxCompute Storage API streaming partition writer does not hit
 *       "writer has been closed" on un-grouped rows;</li>
 *   <li><b>partitioned write</b> + write provider returning {@code true} from
 *       {@code requiresPartitionHashWrite()} (Hive-style) → hash-by-partition with <em>no</em> local sort
 *       (byte-exact to legacy {@code PhysicalHiveTableSink});</li>
 *   <li><b>non-partition / all-static write</b> + write provider returning {@code true} from
 *       {@code requiresParallelWrite()} → {@code SINK_RANDOM_PARTITIONED} (parallel writers, NG-4
 *       parity);</li>
 *   <li><b>capability-less connector</b> (jdbc/es-like) → {@code GATHER} (single writer).</li>
 * </ul>
 *
 * <p><b>Index by full schema, not {@code cols}:</b> the bind layer projects the static/partial-static
 * write's child to full-schema order (static partition columns filled), so the hash/sort keys are the
 * child slots at the partition columns' <em>full-schema</em> positions. {@code cols} excludes the static
 * partition columns, so a cols-position lookup would mislocate the dynamic column in the partial-static
 * case — {@code partialStaticPartitionHashesByDynamicColumn} guards that.</p>
 */
public class PhysicalConnectorTableSinkTest {

    private static final Column DATA = new Column("data", PrimitiveType.INT);
    private static final Column PART = new Column("part", PrimitiveType.INT);

    /**
     * Dynamic-partition write: the partition column 'part' is present in cols (its value comes from
     * the query), so the sink must hash-distribute and locally sort by 'part'. cols == full schema
     * here (no static partition), so full-schema and cols positions coincide.
     */
    @Test
    public void dynamicPartitionWriteRequiresHashAndLocalSort() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        // cols == full schema == [data, part] (part is dynamic), child output aligned 1:1.
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(PART), ImmutableList.of(DATA, PART)),
                Arrays.asList(DATA, PART),
                ImmutableList.of(dataSlot, partSlot));

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHiveTableSinkHashPartitioned,
                "dynamic-partition write must hash-distribute by partition columns");
        DistributionSpecHiveTableSinkHashPartitioned dist =
                (DistributionSpecHiveTableSinkHashPartitioned) props.getDistributionSpec();
        // The hash key is the child slot at 'part's full-schema position (index 1).
        Assertions.assertEquals(ImmutableList.of(partSlot.getExprId()), dist.getOutputColExprIds(),
                "hash key must be the partition-column slot taken at its full-schema position");
        Assertions.assertTrue(props.getOrderSpec() instanceof MustLocalSortOrderSpec,
                "dynamic-partition write must require a mandatory local sort to group partition rows");
        List<OrderKey> orderKeys = props.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(1, orderKeys.size(), "exactly one partition column to sort by");
        Assertions.assertEquals(partSlot, orderKeys.get(0).getExpr(),
                "local sort must be on the partition column");
    }

    /**
     * Pure-dynamic write with a REORDERED explicit column list ({@code INSERT INTO mc (part, data)
     * SELECT vpart, vdata}, schema [data, part]): the bind layer projects the child to FULL-SCHEMA
     * order regardless of the user column order, so child output = [dataSlot, partSlot] while cols =
     * [part, data]. The partition column must be located by its full-schema position (1), not its cols
     * position (0). Guards the FIX-BIND-STATIC-PARTITION indexing revision against the pure-dynamic
     * reordered-list regression a cols-position lookup would cause (it would read child[0] = dataSlot).
     */
    @Test
    public void dynamicReorderedColumnListHashesByPartitionAtFullSchemaPosition() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(PART), ImmutableList.of(DATA, PART)),
                Arrays.asList(PART, DATA),                       // cols reordered: part first
                ImmutableList.of(dataSlot, partSlot));           // child in full-schema order [data, part]

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHiveTableSinkHashPartitioned,
                "reordered-list dynamic write must still hash-distribute by the partition column");
        DistributionSpecHiveTableSinkHashPartitioned dist =
                (DistributionSpecHiveTableSinkHashPartitioned) props.getDistributionSpec();
        // 'part' at full-schema index 1 -> child[1] = partSlot. A cols-position lookup ('part' at cols
        // index 0) would read child[0] = dataSlot and shuffle by the wrong column.
        Assertions.assertEquals(ImmutableList.of(partSlot.getExprId()), dist.getOutputColExprIds(),
                "hash key must be the partition slot at its full-schema position, not its cols position");
        Assertions.assertEquals(partSlot, props.getOrderSpec().getOrderKeys().get(0).getExpr(),
                "local sort must be on the partition column slot");
    }

    /**
     * Partial-static write ({@code PARTITION(ds='x') SELECT id, val, region} — ds static, region
     * dynamic): the bind layer projects the child to full schema with ds filled (NULL), so child
     * output = [id, val, ds, region] while cols = [id, val, region] (ds excluded). The partition
     * columns must be located by their FULL-SCHEMA positions (ds@2, region@3), not their cols
     * positions — otherwise the dynamic 'region' would be mislocated and grouping would break,
     * re-triggering "writer has been closed". This guards the FIX-BIND-STATIC-PARTITION revision of
     * the indexing (a cols-position regression yields hash keys = [ds] only).
     */
    @Test
    public void partialStaticPartitionHashesByDynamicColumn() {
        Column id = new Column("id", PrimitiveType.INT);
        Column val = new Column("val", PrimitiveType.INT);
        Column ds = new Column("ds", PrimitiveType.INT);
        Column region = new Column("region", PrimitiveType.INT);
        SlotReference idSlot = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference valSlot = new SlotReference("val", IntegerType.INSTANCE);
        SlotReference dsSlot = new SlotReference("ds", IntegerType.INSTANCE);
        SlotReference regionSlot = new SlotReference("region", IntegerType.INSTANCE);

        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(ds, region), ImmutableList.of(id, val, ds, region)),
                Arrays.asList(id, val, region),                          // cols excludes static ds
                ImmutableList.of(idSlot, valSlot, dsSlot, regionSlot));   // child == full schema

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHiveTableSinkHashPartitioned,
                "partial-static write must hash-distribute by partition columns");
        DistributionSpecHiveTableSinkHashPartitioned dist =
                (DistributionSpecHiveTableSinkHashPartitioned) props.getDistributionSpec();
        // Both partition columns located by full-schema position: child[2]=dsSlot, child[3]=regionSlot.
        // A cols-position regression (region at cols index 2) would read child[2]=dsSlot and drop
        // regionSlot, yielding [dsSlot] — caught by this exact-list assertion.
        Assertions.assertEquals(ImmutableList.of(dsSlot.getExprId(), regionSlot.getExprId()),
                dist.getOutputColExprIds(),
                "hash keys must be the partition-column slots at their full-schema positions");
        Assertions.assertTrue(props.getOrderSpec() instanceof MustLocalSortOrderSpec,
                "partial-static write must require a mandatory local sort");
        List<OrderKey> orderKeys = props.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(2, orderKeys.size(), "sort by both partition columns in full-schema order");
        Assertions.assertEquals(dsSlot, orderKeys.get(0).getExpr());
        Assertions.assertEquals(regionSlot, orderKeys.get(1).getExpr());
    }

    /**
     * All-static-partition write: every partition column is statically specified and therefore absent
     * from cols, so no grouping/sort is needed — parallel writers (RANDOM), matching legacy branch-2.
     * After FIX-BIND-STATIC-PARTITION the bind layer projects the no-column-list form's child to full
     * schema ([data, part] with part filled), but the RANDOM branch never indexes the child, so the
     * result is RANDOM regardless of the child shape.
     */
    @Test
    public void allStaticPartitionWriteUsesRandomPartitioned() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, true, ImmutableList.of(PART), ImmutableList.of(DATA, PART)),
                Arrays.asList(DATA),                              // cols excludes the static part
                ImmutableList.of(dataSlot, partSlot));            // child == full schema (part filled)

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
                table(true, true, ImmutableList.of(), ImmutableList.of(DATA)),
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
                table(false, false, ImmutableList.of(), ImmutableList.of(DATA)),
                Arrays.asList(DATA),
                ImmutableList.of(dataSlot));

        Assertions.assertSame(PhysicalProperties.GATHER, sink.getRequirePhysicalProperties(),
                "a connector declaring neither capability must keep the single-writer GATHER default");
    }

    /**
     * Rewrite (compaction) override: a {@code rewrite_data_files} INSERT-SELECT must gather to a single
     * writer to control its output file count, even on a PARTITIONED table where an ordinary write would
     * hash-distribute by the partition columns. The neutral {@code isRewrite} flag short-circuits to
     * GATHER before the partition-shuffle arm. The table/cols/child are identical to
     * {@link #dynamicPartitionWriteRequiresHashAndLocalSort} (which, with {@code isRewrite=false}, returns
     * the hash-partitioned spec), so this isolates the override as the sole behavioral delta. Mutation
     * lock: dropping the {@code if (isRewrite) return GATHER} guard makes this return
     * {@link DistributionSpecHiveTableSinkHashPartitioned} and the assertion fails.
     */
    @Test
    public void rewriteModeGathersEvenOnPartitionedTable() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sinkRewrite(
                table(true, true, ImmutableList.of(PART), ImmutableList.of(DATA, PART)),
                Arrays.asList(DATA, PART),
                ImmutableList.of(dataSlot, partSlot));

        Assertions.assertSame(PhysicalProperties.GATHER, sink.getRequirePhysicalProperties(),
                "a rewrite write must gather to a single writer even on a partitioned table, "
                        + "overriding the partition-shuffle distribution");
    }

    /**
     * Hash-write connector (Hive-style, {@code requiresPartitionHashWrite=true} but
     * {@code requiresPartitionLocalSort=false}): a partitioned write hash-distributes by the partition
     * columns with NO mandatory local sort — byte-exact to legacy {@code PhysicalHiveTableSink}, which
     * hashed by the partition columns and never attached an order spec (the hive file writer buffers a
     * per-partition writer, so grouping the rows by a sort is unnecessary). The MaxCompute arm is skipped
     * because {@code requirePartitionLocalSortOnWrite()} is false, so this reaches the new no-sort arm.
     */
    @Test
    public void partitionHashWriteHashesByPartitionWithoutLocalSort() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        SlotReference partSlot = new SlotReference("part", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, false, true, ImmutableList.of(PART), ImmutableList.of(DATA, PART)),
                Arrays.asList(DATA, PART),
                ImmutableList.of(dataSlot, partSlot));

        PhysicalProperties props = sink.getRequirePhysicalProperties();

        Assertions.assertTrue(props.getDistributionSpec() instanceof DistributionSpecHiveTableSinkHashPartitioned,
                "a hash-write connector must hash-distribute a partitioned write by its partition columns");
        DistributionSpecHiveTableSinkHashPartitioned dist =
                (DistributionSpecHiveTableSinkHashPartitioned) props.getDistributionSpec();
        Assertions.assertEquals(ImmutableList.of(partSlot.getExprId()), dist.getOutputColExprIds(),
                "hash key must be the partition-column slot taken at its full-schema position");
        Assertions.assertFalse(props.getOrderSpec() instanceof MustLocalSortOrderSpec,
                "a no-sort hash-write connector must NOT add a mandatory local sort (byte-exact to legacy "
                        + "PhysicalHiveTableSink, which hash-distributed without a sort) — else a hive write "
                        + "would pay an unnecessary sort the legacy path never had");
    }

    /**
     * Non-partitioned write on a hash-write connector: the hash arm's {@code !partitionNames.isEmpty()}
     * gate falls through to the parallel arm, matching legacy {@code PhysicalHiveTableSink}'s
     * non-partitioned {@code SINK_RANDOM_PARTITIONED}. Guards that the hash arm never fires without
     * partition columns (which would build an empty-key distribution).
     */
    @Test
    public void nonPartitionedHashWriteConnectorUsesRandomWhenParallel() {
        SlotReference dataSlot = new SlotReference("data", IntegerType.INSTANCE);
        PhysicalConnectorTableSink<Plan> sink = sink(
                table(true, false, true, ImmutableList.of(), ImmutableList.of(DATA)),
                Arrays.asList(DATA),
                ImmutableList.of(dataSlot));

        Assertions.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties(),
                "a non-partitioned hash-write connector falls through to parallel writers, not the hash arm");
    }

    // ==================== helpers ====================

    private static PluginDrivenExternalTable table(boolean parallelWrite, boolean requirePartitionSort,
            List<Column> partitionColumns, List<Column> fullSchema) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.supportsParallelWrite()).thenReturn(parallelWrite);
        Mockito.when(table.requirePartitionLocalSortOnWrite()).thenReturn(requirePartitionSort);
        Mockito.when(table.getPartitionColumns()).thenReturn(partitionColumns);
        Mockito.when(table.getFullSchema()).thenReturn(fullSchema);
        return table;
    }

    /** As {@link #table(boolean, boolean, List, List)} but also stubs the hash-write capability. */
    private static PluginDrivenExternalTable table(boolean parallelWrite, boolean requirePartitionSort,
            boolean requirePartitionHash, List<Column> partitionColumns, List<Column> fullSchema) {
        PluginDrivenExternalTable table = table(parallelWrite, requirePartitionSort, partitionColumns, fullSchema);
        Mockito.when(table.requirePartitionHashOnWrite()).thenReturn(requirePartitionHash);
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

    /**
     * Builds a {@link PhysicalConnectorTableSink} as {@link #sink} but in rewrite mode (the neutral
     * {@code isRewrite} field set true), to exercise the rewrite GATHER override.
     */
    private static PhysicalConnectorTableSink<Plan> sinkRewrite(PluginDrivenExternalTable table,
            List<Column> cols, List<Slot> childOutput) {
        PhysicalConnectorTableSink<Plan> sink = sink(table, cols, childOutput);
        Deencapsulation.setField(sink, "isRewrite", true);
        return sink;
    }
}
