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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWriteSortColumn;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalConnectorTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelDeleteSink;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PluginDrivenTableSink;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Pins the two generic write-admission gates the neutral translator enforces over
 * {@link Connector#supportedWriteOperations()} (P6 write-capability unification, Task 6): the INSERT gate in
 * {@link PhysicalPlanTranslator#visitPhysicalConnectorTableSink} and the row-level-DML gate in the plugin arm
 * of {@link PhysicalPlanTranslator#visitPhysicalExternalRowLevelDeleteSink} — WITH DISTINCT rejection messages, so a
 * connector declaring only {@code {INSERT}} is admitted for a plain write but rejected for DELETE/MERGE, not
 * lumped into one coarse "no writes supported" gate. This is the granularity regression guard for Task 3's
 * admission rewrite: a mutation that merges the two gates (or swaps their messages) turns these red.
 */
public class PhysicalPlanTranslatorAdmissionGateTest {

    private static final Column DATA = new Column("data", PrimitiveType.INT);
    private static final Column A = new Column("a", PrimitiveType.INT);
    private static final Column B = new Column("b", PrimitiveType.INT);
    private static final Column C = new Column("c", PrimitiveType.INT);

    @Test
    public void insertGateAllowsConnectorDeclaringInsert() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        PluginDrivenExternalTable table = pluginTable(EnumSet.of(WriteOperation.INSERT));

        @SuppressWarnings("unchecked")
        PhysicalConnectorTableSink<Plan> sink = Mockito.mock(PhysicalConnectorTableSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();
        Mockito.doReturn(false).when(sink).isRewrite();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalConnectorTableSink(sink, context);

        PluginDrivenTableSink pluginSink = capturePluginSink(childFragment);
        Assertions.assertEquals(WriteOperation.INSERT, Deencapsulation.getField(pluginSink, "writeOperation"),
                "a connector declaring INSERT must reach the sink machinery with WriteOperation.INSERT, not be "
                        + "rejected by the admission gate");
    }

    @Test
    public void insertGateRejectsConnectorNotDeclaringInsert() {
        // {} mirrors the null-write-provider connector's view: the resolved provider's supportedOperations()
        // is empty whenever getWritePlanProvider() returns null. The gate must reject before ever resolving a
        // write plan provider / calling planWrite.
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        PluginDrivenExternalTable table = pluginTable(EnumSet.noneOf(WriteOperation.class));

        @SuppressWarnings("unchecked")
        PhysicalConnectorTableSink<Plan> sink = Mockito.mock(PhysicalConnectorTableSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> translator.visitPhysicalConnectorTableSink(sink, context));
        Assertions.assertTrue(ex.getMessage().contains("does not support INSERT operations"),
                "got: " + ex.getMessage());
    }

    @Test
    public void rowLevelDmlGateRejectsConnectorDeclaringOnlyInsertWithDistinctMessage() {
        // Declares INSERT (would pass the INSERT gate above) but neither DELETE nor MERGE: the row-level DML
        // helper must reject it, and with a message DISTINCT from the INSERT gate's, so logs/callers can tell
        // "this connector can't do row-level DML at all" apart from "this connector can't write at all".
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        PluginDrivenExternalTable table = pluginTable(EnumSet.of(WriteOperation.INSERT));

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelDeleteSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> translator.visitPhysicalExternalRowLevelDeleteSink(sink, context));
        Assertions.assertTrue(ex.getMessage().contains("does not support row-level DML operations"),
                "got: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains("does not support INSERT operations"),
                "the row-level DML rejection must be a message DISTINCT from the INSERT gate's, got: "
                        + ex.getMessage());
    }

    @Test
    public void partialInsertResolvesWriteSortAgainstFullOutput() {
        assertWriteSortUsesBoundOutputColumn(ImmutableList.of(C), C);
    }

    @Test
    public void reorderedInsertResolvesWriteSortAgainstFullOutput() {
        assertWriteSortUsesBoundOutputColumn(ImmutableList.of(C, A), C);
    }

    @Test
    public void staticPartitionInsertKeepsSortColumnInFullOutput() {
        assertWriteSortUsesBoundOutputColumn(ImmutableList.of(A, C), B);
    }

    @Test
    public void insertCapturesWriteMetadataIdentityBeforePhysicalShaping() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Mockito.when(provider.getWriteMetadataIdentity(Mockito.any(), Mockito.any()))
                .thenReturn("sort-3/spec-7");
        PluginDrivenExternalTable table = pluginTable(EnumSet.of(WriteOperation.INSERT), provider);

        @SuppressWarnings("unchecked")
        PhysicalConnectorTableSink<Plan> sink = Mockito.mock(PhysicalConnectorTableSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();
        Mockito.doReturn(false).when(sink).isRewrite();

        new PhysicalPlanTranslator(context, null).visitPhysicalConnectorTableSink(sink, context);

        PluginDrivenTableSink pluginSink = capturePluginSink(childFragment);
        Assertions.assertEquals("sort-3/spec-7",
                Deencapsulation.getField(pluginSink, "boundWriteMetadataIdentity"));
        // Capture before sort shaping so a generation change cannot pair an old physical sort with a new fence.
        InOrder inOrder = Mockito.inOrder(provider);
        inOrder.verify(provider).getWriteMetadataIdentity(Mockito.any(), Mockito.any());
        inOrder.verify(provider).getWriteSortColumns(Mockito.any(), Mockito.any(), Mockito.anyList());
    }

    // ==================== helpers ====================

    private static Plan mockChild(PlanFragment childFragment) {
        Plan child = Mockito.mock(Plan.class);
        Mockito.doReturn(childFragment).when(child).accept(Mockito.any(), Mockito.any());
        return child;
    }

    private static void assertWriteSortUsesBoundOutputColumn(List<Column> writeColumns, Column sortColumn) {
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Mockito.when(provider.getWriteSortColumns(Mockito.any(), Mockito.any(), Mockito.anyList()))
                .thenAnswer(invocation -> {
                    List<ConnectorColumn> columns = invocation.getArgument(2);
                    for (int i = 0; i < columns.size(); i++) {
                        if (columns.get(i).getName().equals(sortColumn.getName())) {
                            return ImmutableList.of(new ConnectorWriteSortColumn(i, true, true));
                        }
                    }
                    return ImmutableList.of();
                });
        PluginDrivenExternalTable table = pluginTable(EnumSet.of(WriteOperation.INSERT), provider);
        Mockito.when(table.requiresFullSchemaWriteOrder()).thenReturn(true);

        SlotReference aOutput = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference bOutput = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference cOutput = new SlotReference("c", IntegerType.INSTANCE);
        TupleDescriptor tuple = context.generateTupleDesc();
        SlotRef aSlot = registerLegacySlot(context, tuple, aOutput, A);
        SlotRef bSlot = registerLegacySlot(context, tuple, bOutput, B);
        SlotRef cSlot = registerLegacySlot(context, tuple, cOutput, C);

        @SuppressWarnings("unchecked")
        PhysicalConnectorTableSink<Plan> sink = Mockito.mock(PhysicalConnectorTableSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(writeColumns).when(sink).getCols();
        Mockito.doReturn(ImmutableList.of(A, B, C)).when(sink).getBoundTargetSchema();
        Mockito.doReturn(ImmutableList.of(aOutput, bOutput, cOutput)).when(sink).getOutput();
        Mockito.doReturn(false).when(sink).isRewrite();

        new PhysicalPlanTranslator(context, null).visitPhysicalConnectorTableSink(sink, context);

        TSortInfo sortInfo = Deencapsulation.getField(capturePluginSink(childFragment), "writeSortInfo");
        Assertions.assertNotNull(sortInfo);
        Assertions.assertEquals(1, sortInfo.getOrderingExprsSize());
        int actualSlotId = sortInfo.getOrderingExprs().get(0).getNodes().get(0).getSlotRef().getSlotId();
        SlotRef expected = sortColumn == A ? aSlot : sortColumn == B ? bSlot : cSlot;
        Assertions.assertEquals(expected.getDesc().getId().asInt(), actualSlotId,
                "write-sort position must use the same full-schema coordinates as the sink output");
    }

    private static SlotRef registerLegacySlot(PlanTranslatorContext context, TupleDescriptor tuple,
            SlotReference output, Column column) {
        SlotDescriptor descriptor = context.addSlotDesc(tuple);
        descriptor.setColumn(column);
        descriptor.setType(column.getType());
        descriptor.setIsNullable(true);
        SlotRef slotRef = new SlotRef(descriptor);
        context.addExprIdSlotRefPair(output.getExprId(), slotRef);
        return slotRef;
    }

    /** A plugin-driven table whose connector declares exactly the given write operations. */
    private static PluginDrivenExternalTable pluginTable(Set<WriteOperation> ops) {
        return pluginTable(ops, Mockito.mock(ConnectorWritePlanProvider.class));
    }

    private static PluginDrivenExternalTable pluginTable(Set<WriteOperation> ops,
            ConnectorWritePlanProvider provider) {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        // The write seams now resolve metadata through the per-statement funnel, which reads the session's
        // statement scope; offline tests use NONE (a fresh getMetadata per call, byte-identical to pre-funnel).
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        // Production selects the write provider per-handle; a plain mock does not run the interface default.
        Mockito.when(connector.getWritePlanProvider(Mockito.any())).thenReturn(provider);
        // The admission gate resolves the handle, fetches the per-handle provider and asks IT which
        // operations are supported -- the provider is the only place a write trait is declared.
        Mockito.when(provider.supportedOperations()).thenReturn(ops);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Optional.of(handle));
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getRemoteDbName()).thenReturn("db");
        Mockito.when(table.getRemoteName()).thenReturn("t");
        return table;
    }

    private static PluginDrivenTableSink capturePluginSink(PlanFragment childFragment) {
        ArgumentCaptor<DataSink> captor = ArgumentCaptor.forClass(DataSink.class);
        Mockito.verify(childFragment).setSink(captor.capture());
        DataSink built = captor.getValue();
        Assertions.assertTrue(built instanceof PluginDrivenTableSink,
                "must route through the generic PluginDrivenTableSink, was " + built.getClass().getSimpleName());
        return (PluginDrivenTableSink) built;
    }
}
