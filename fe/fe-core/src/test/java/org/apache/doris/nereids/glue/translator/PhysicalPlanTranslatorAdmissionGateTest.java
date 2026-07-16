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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalConnectorTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergDeleteSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PluginDrivenTableSink;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Pins the two generic write-admission gates the neutral translator enforces over
 * {@link Connector#supportedWriteOperations()} (P6 write-capability unification, Task 6): the INSERT gate in
 * {@link PhysicalPlanTranslator#visitPhysicalConnectorTableSink} and the row-level-DML gate in the plugin arm
 * of {@link PhysicalPlanTranslator#visitPhysicalIcebergDeleteSink} — WITH DISTINCT rejection messages, so a
 * connector declaring only {@code {INSERT}} is admitted for a plain write but rejected for DELETE/MERGE, not
 * lumped into one coarse "no writes supported" gate. This is the granularity regression guard for Task 3's
 * admission rewrite: a mutation that merges the two gates (or swaps their messages) turns these red.
 */
public class PhysicalPlanTranslatorAdmissionGateTest {

    private static final Column DATA = new Column("data", PrimitiveType.INT);

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
        // {} mirrors the null-write-provider connector's delegator view: Connector.supportedWriteOperations()
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
        PhysicalIcebergDeleteSink<Plan> sink = Mockito.mock(PhysicalIcebergDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> translator.visitPhysicalIcebergDeleteSink(sink, context));
        Assertions.assertTrue(ex.getMessage().contains("does not support row-level DML operations"),
                "got: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains("does not support INSERT operations"),
                "the row-level DML rejection must be a message DISTINCT from the INSERT gate's, got: "
                        + ex.getMessage());
    }

    // ==================== helpers ====================

    private static Plan mockChild(PlanFragment childFragment) {
        Plan child = Mockito.mock(Plan.class);
        Mockito.doReturn(childFragment).when(child).accept(Mockito.any(), Mockito.any());
        return child;
    }

    /** A plugin-driven table whose connector declares exactly the given write operations. */
    private static PluginDrivenExternalTable pluginTable(Set<WriteOperation> ops) {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        // Production selects the write provider per-handle; a plain mock does not run the interface default.
        Mockito.when(connector.getWritePlanProvider(Mockito.any())).thenReturn(provider);
        Mockito.when(connector.supportedWriteOperations()).thenReturn(ops);
        // The admission gate now resolves the handle first and consults the per-handle overload.
        Mockito.when(connector.supportedWriteOperations(Mockito.any())).thenReturn(ops);
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
