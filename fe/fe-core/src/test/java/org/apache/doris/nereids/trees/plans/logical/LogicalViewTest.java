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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.ViewIf;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link LogicalView#computeOutput()}.
 *
 * <p>Focuses on the schema-drift scenario: when the underlying external table (e.g. Hive)
 * gains new columns after the view was created, {@code view.getFullSchema()} is smaller
 * than {@code child().getOutput()} and the loop used to crash with
 * {@code IndexOutOfBoundsException}.
 */
public class LogicalViewTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Slot slot(String name) {
        Slot slot = Mockito.mock(Slot.class);
        Mockito.when(slot.getName()).thenReturn(name);
        Mockito.when(slot.withQualifier(Mockito.anyList())).thenReturn(slot);
        Mockito.when(slot.withOneLevelTableAndColumnAndQualifier(
                Mockito.any(), Mockito.any(), Mockito.anyList())).thenReturn(slot);
        return slot;
    }

    private static Column col(String name) {
        return new Column(name, Type.STRING, false, AggregateType.NONE, "", "");
    }

    /**
     * Build a mock ViewIf that reports the given schema and qualifiers.
     */
    private static ViewIf mockView(List<Column> schema, List<String> qualifiers) {
        ViewIf view = Mockito.mock(ViewIf.class);
        Mockito.when(view.getFullSchema()).thenReturn(schema);
        Mockito.when(view.getFullQualifiers()).thenReturn(qualifiers);
        return view;
    }

    /**
     * Build a mock LogicalPlan child whose {@code getOutput()} returns the given slots.
     */
    private static LogicalPlan mockChild(List<Slot> slots) {
        LogicalPlan child = Mockito.mock(LogicalPlan.class);
        Mockito.when(child.getOutput()).thenReturn(slots);
        return child;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * Normal case: child output size == fullSchema size.
     * All slots should be decorated with their corresponding stored column.
     */
    @Test
    public void testComputeOutputNormalCase() {
        List<Column> schema = ImmutableList.of(col("id"), col("name"), col("age"));
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("name"), slot("age"));

        ViewIf view = mockView(schema, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        List<Slot> output = logicalView.computeOutput();

        Assertions.assertEquals(3, output.size(), "Output size should match schema size");
        Assertions.assertEquals("id", output.get(0).getName());
        Assertions.assertEquals("name", output.get(1).getName());
        Assertions.assertEquals("age", output.get(2).getName());
    }

    /**
     * Schema-drift case: child output has MORE slots than fullSchema.
     *
     * <p>This is the regression test for the IndexOutOfBoundsException bug:
     * after {@code ALTER TABLE ADD COLUMNS} + {@code REFRESH TABLE} on the base table,
     * {@code childOutput.size()} grows beyond {@code view.getFullSchema().size()}.
     *
     * <p>The fix truncates output to the view's declared schema width so that:
     * (1) no IndexOutOfBoundsException is thrown, and
     * (2) the view's output contract (as seen in {@code DESC view}) is preserved.
     */
    @Test
    public void testComputeOutputSchemaDrift_moreColumnsThanSchema() {
        // View was created with 3-column base table → fullSchema has 3 entries
        List<Column> schema = ImmutableList.of(col("id"), col("name"), col("age"));
        // After ADD COLUMN + REFRESH TABLE, the child produces 4 slots
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("name"), slot("age"), slot("score"));

        ViewIf view = mockView(schema, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        // Must NOT throw IndexOutOfBoundsException
        List<Slot> output = Assertions.assertDoesNotThrow(logicalView::computeOutput,
                "computeOutput() must not throw when fullSchema is shorter than childOutput");

        // Output is truncated to the view's declared schema width (3), not the child width (4).
        // The new 'score' column is not visible until the view itself is refreshed.
        Assertions.assertEquals(3, output.size(),
                "Output size should be truncated to fullSchema size to preserve view contract");
        Assertions.assertEquals("id", output.get(0).getName());
        Assertions.assertEquals("name", output.get(1).getName());
        Assertions.assertEquals("age", output.get(2).getName());
    }

    /**
     * Schema-drift case: child output has FEWER slots than fullSchema.
     *
     * <p>Defensive check: if a column was somehow removed from the base table
     * while the view schema is wider, we must not access a nonexistent child slot.
     * The output should simply reflect however many columns the child returns.
     */
    @Test
    public void testComputeOutputSchemaDrift_fewerColumnsThanSchema() {
        // fullSchema wider than actual child (defensive scenario)
        List<Column> schema = ImmutableList.of(col("id"), col("name"), col("age"), col("score"));
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("name"));

        ViewIf view = mockView(schema, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        List<Slot> output = logicalView.computeOutput();

        Assertions.assertEquals(2, output.size(),
                "Output size should equal child output size (loop bound)");
        Assertions.assertEquals("id", output.get(0).getName());
        Assertions.assertEquals("name", output.get(1).getName());
    }

    /**
     * Empty schema case (guard introduced by #40715): fullSchema is empty.
     * All slots must fall back to withQualifier().
     */
    @Test
    public void testComputeOutputEmptySchema() {
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("name"));

        ViewIf view = mockView(Collections.emptyList(), ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        List<Slot> output = Assertions.assertDoesNotThrow(logicalView::computeOutput,
                "computeOutput() must not throw for empty fullSchema");

        Assertions.assertEquals(2, output.size());
        Assertions.assertEquals("id", output.get(0).getName());
        Assertions.assertEquals("name", output.get(1).getName());
    }

    /**
     * Null schema case (guard introduced by #40715): getFullSchema() returns null.
     * All slots must fall back to withQualifier().
     */
    @Test
    public void testComputeOutputNullSchema() {
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("name"));

        ViewIf view = mockView(null, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        List<Slot> output = Assertions.assertDoesNotThrow(logicalView::computeOutput,
                "computeOutput() must not throw for null fullSchema");

        Assertions.assertEquals(2, output.size());
    }

    /**
     * Single new column added (minimal drift). Verifies the exact boundary condition
     * at index {@code fullSchema.size()}: output is truncated to schema width (1).
     */
    @Test
    public void testComputeOutputSchemaDrift_singleColumnAdded() {
        // 1-column schema, child returns 2 slots after schema drift
        List<Column> schema = ImmutableList.of(col("id"));
        List<Slot> childSlots = ImmutableList.of(slot("id"), slot("extra"));

        ViewIf view = mockView(schema, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(childSlots));

        List<Slot> output = Assertions.assertDoesNotThrow(logicalView::computeOutput);

        // Truncated to schema width; the new 'extra' column is not exposed.
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals("id", output.get(0).getName());
    }

    /**
     * Child returns no output (empty relation). Must return an empty list and not crash.
     */
    @Test
    public void testComputeOutputEmptyChild() {
        List<Column> schema = ImmutableList.of(col("id"), col("name"));
        ViewIf view = mockView(schema, ImmutableList.of("hive", "test", "v"));
        LogicalView<LogicalPlan> logicalView = new LogicalView<>(view, mockChild(ImmutableList.of()));

        List<Slot> output = logicalView.computeOutput();

        Assertions.assertTrue(output.isEmpty());
    }
}
