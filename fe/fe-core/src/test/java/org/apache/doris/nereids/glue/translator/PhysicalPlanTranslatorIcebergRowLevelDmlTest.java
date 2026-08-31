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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelDeleteSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PluginDrivenTableSink;
import org.apache.doris.thrift.TDataSink;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Unit tests for the dual-mode routing added to the iceberg row-level DML translator visitors
 * ({@link PhysicalPlanTranslator#visitPhysicalExternalRowLevelDeleteSink} /
 * {@code visitPhysicalExternalRowLevelMergeSink}) for the iceberg SPI cutover (commit-bridge S5d).
 *
 * <p>Pre-flip the target is a native {@code IcebergExternalTable} and the visitor builds the native
 * {@code IcebergDeleteSink} / {@code IcebergMergeSink} (byte-identical; its native end-to-end test
 * {@code IcebergDDLAndDMLPlanTest} was retired with the P6.6 iceberg cutover as the native arm is no longer
 * reachable, and the native sink ctor loads vended credentials + the live iceberg table, so it is not
 * exercised at the translator unit level here). Post-flip the target is a
 * {@link PluginDrivenExternalTable} and the visitor must route through the generic
 * {@link PluginDrivenTableSink} with the matching {@link WriteOperation}, so the connector's
 * {@code planWrite} emits its DELETE / MERGE BE sink dialect. These tests pin the plugin (post-flip) arm.</p>
 *
 * <p>Load-bearing invariants pinned here:
 * <ul>
 *   <li><b>routing + op</b>: the plugin arm builds a {@link PluginDrivenTableSink} carrying DELETE / MERGE;</li>
 *   <li><b>MERGE output-expr loop lift</b>: the operation/row-id materialized-name loop runs for the plugin arm
 *       (it is lifted above the native/plugin branch) and the operation + row-id slots are published in the
 *       fragment output exprs — BE's {@code viceberg_merge_sink} resolves them by output-expr name regardless
 *       of the sink dialect;</li>
 *   <li><b>DELETE has no loop</b>: the DELETE arm publishes no output exprs (BE resolves the row id by block
 *       column name, not output-expr name);</li>
 *   <li><b>Fix B pin</b>: the statement's MVCC read snapshot is threaded onto the write handle.</li>
 * </ul></p>
 */
public class PhysicalPlanTranslatorIcebergRowLevelDmlTest {

    private static final Column DATA = new Column("data", PrimitiveType.INT);

    @Test
    public void deletePluginArmRoutesToPluginSinkWithDeleteOperationAndNoOutputExprs() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelDeleteSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelDeleteSink(sink, context);

        PluginDrivenTableSink pluginSink = capturePluginSink(childFragment);
        Assertions.assertEquals(WriteOperation.DELETE, Deencapsulation.getField(pluginSink, "writeOperation"),
                "a post-flip DELETE must thread WriteOperation.DELETE so the connector emits TIcebergDeleteSink");
        Assertions.assertNull(Deencapsulation.getField(pluginSink, "writeSortInfo"),
                "a row-level DELETE has no engine write sort");
        assertConnectorColumnsFromCols(pluginSink);
        // DELETE resolves its row id by BE block-name (a real hidden column), so the visitor must NOT emit the
        // MERGE-style output-expr list — match the native delete path exactly.
        Mockito.verify(childFragment, Mockito.never()).setOutputExprs(Mockito.anyList());
    }

    @Test
    public void mergePluginArmRoutesToPluginSinkAndPublishesOperationAndRowIdOutputExprs() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        TupleDescriptor tuple = context.generateTupleDesc();
        SlotReference dataSlot = registerSlot(context, tuple, "data");
        SlotReference opSlot = registerSlot(context, tuple, MergeOperation.OPERATION_COLUMN);
        SlotReference rowidSlot = registerSlot(context, tuple, Column.ICEBERG_ROWID_COL);

        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelMergeSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelMergeSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();
        Mockito.doReturn(ImmutableList.<Slot>of(dataSlot, opSlot, rowidSlot)).when(sink).getOutput();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelMergeSink(sink, context);

        PluginDrivenTableSink pluginSink = capturePluginSink(childFragment);
        Assertions.assertEquals(WriteOperation.MERGE, Deencapsulation.getField(pluginSink, "writeOperation"),
                "a post-flip MERGE must thread WriteOperation.MERGE so the connector emits TIcebergMergeSink");
        Assertions.assertNull(Deencapsulation.getField(pluginSink, "writeSortInfo"),
                "a row-level MERGE carries its sort inside the connector's TIcebergMergeSink.sort_fields, not the"
                        + " engine write sort");
        assertConnectorColumnsFromCols(pluginSink);

        // The fragment output-exprs are load-bearing: BE's viceberg_merge_sink resolves the operation / row-id
        // columns strictly by output-expr name. Assert the list content (not just that some list was set), so a
        // regression that emptied it or dropped the operation/row-id slots is caught.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Expr>> exprsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(childFragment).setOutputExprs(exprsCaptor.capture());
        List<Expr> publishedExprs = exprsCaptor.getValue();
        Assertions.assertEquals(3, publishedExprs.size(),
                "every sink output slot must be published as a fragment output expr");
        Assertions.assertTrue(publishedExprs.contains(context.findSlotRef(opSlot.getExprId())),
                "the operation column must be published in the fragment output exprs (BE resolves it by name)");
        Assertions.assertTrue(publishedExprs.contains(context.findSlotRef(rowidSlot.getExprId())),
                "the row-id column must be published in the fragment output exprs (BE resolves it by name)");
        Assertions.assertTrue(publishedExprs.contains(context.findSlotRef(dataSlot.getExprId())),
                "the data column must be published in the fragment output exprs");
    }

    // #66112: SQL MERGE INTO must reject a target row matched by more than one source row; UPDATE shares the
    // same sink dialect and has no such rule. Only BE can see the duplicates, so the engine's job is to ship
    // the statement's requirement onto the write handle. A regression that hardcoded it (either polarity)
    // would either silently accept a duplicate-source MERGE (wrong-result write) or reject legal UPDATEs, so
    // both polarities are pinned.
    @Test
    public void mergePluginArmThreadsTheSqlMergeCardinalityRequirementOntoTheWriteHandle() {
        PluginDrivenTableSink pluginSink = translateMergeWithCardinalityRequirement(true);
        Assertions.assertEquals(true,
                Deencapsulation.getField(pluginSink, "requireMergeCardinalityCheck"),
                "a SQL MERGE must thread requireMergeCardinalityCheck=true so the connector stamps it onto"
                        + " TIcebergMergeSink and BE validates duplicate matches");
    }

    @Test
    public void updatePluginArmThreadsNoCardinalityRequirementOntoTheWriteHandle() {
        PluginDrivenTableSink pluginSink = translateMergeWithCardinalityRequirement(false);
        Assertions.assertEquals(false,
                Deencapsulation.getField(pluginSink, "requireMergeCardinalityCheck"),
                "an UPDATE reaches the same sink dialect but carries no SQL cardinality rule");
    }

    private PluginDrivenTableSink translateMergeWithCardinalityRequirement(boolean require) {
        PlanTranslatorContext context = new PlanTranslatorContext();
        TupleDescriptor tuple = context.generateTupleDesc();
        SlotReference dataSlot = registerSlot(context, tuple, "data");
        SlotReference opSlot = registerSlot(context, tuple, MergeOperation.OPERATION_COLUMN);
        SlotReference rowidSlot = registerSlot(context, tuple, Column.ICEBERG_ROWID_COL);

        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelMergeSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelMergeSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();
        Mockito.doReturn(ImmutableList.<Slot>of(dataSlot, opSlot, rowidSlot)).when(sink).getOutput();
        Mockito.doReturn(require).when(sink).isRequireMergeCardinalityCheck();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelMergeSink(sink, context);
        return capturePluginSink(childFragment);
    }

    @Test
    public void mergePluginArmRunsMaterializedNameLoopSoBeResolvesOperationColumn() {
        // The materialized-name loop is lifted above the native/plugin branch. If it were left only in the
        // native arm, the plugin MERGE here would never materialize the synthetic operation column's name, and
        // BE's viceberg_merge_sink (which matches the operation column by output-expr name) would fail to find
        // it. This asserts the plugin arm materializes the name -> the loop ran for the plugin path.
        PlanTranslatorContext context = new PlanTranslatorContext();
        TupleDescriptor tuple = context.generateTupleDesc();
        SlotReference dataSlot = registerSlot(context, tuple, "data");
        SlotReference opSlot = registerSlot(context, tuple, MergeOperation.OPERATION_COLUMN);
        SlotReference rowidSlot = registerSlot(context, tuple, Column.ICEBERG_ROWID_COL);

        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelMergeSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelMergeSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();
        Mockito.doReturn(ImmutableList.<Slot>of(dataSlot, opSlot, rowidSlot)).when(sink).getOutput();

        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelMergeSink(sink, context);

        SlotDescriptor opDesc = context.findSlotRef(opSlot.getExprId()).getDesc();
        Assertions.assertEquals(MergeOperation.OPERATION_COLUMN, opDesc.getMaterializedColumnName(),
                "the plugin MERGE arm must materialize the synthetic operation column's BE col_name");
        SlotDescriptor rowidDesc = context.findSlotRef(rowidSlot.getExprId()).getDesc();
        Assertions.assertEquals(Column.ICEBERG_ROWID_COL, rowidDesc.getMaterializedColumnName(),
                "the plugin MERGE arm must materialize the synthetic row-id column's BE col_name");
        SlotDescriptor dataDesc = context.findSlotRef(dataSlot.getExprId()).getDesc();
        Assertions.assertNull(dataDesc.getMaterializedColumnName(),
                "a regular data column must not be materialized (only operation/row-id are)");
    }

    @Test
    public void rowLevelDmlThreadsMvccReadSnapshotPinOntoTheWriteHandle() throws Exception {
        // Fix B: the write handle must carry the statement's pinned MVCC read snapshot, so a DELETE/MERGE
        // re-derives its deletes from the SAME snapshot its scan read. Binding is intentionally late because
        // that is the first point where the exact target branch is available.
        Plugin plugin = pluginTable();
        ConnectorMvccSnapshot connectorSnapshot = Mockito.mock(ConnectorMvccSnapshot.class);
        PluginDrivenMvccSnapshot pinned = new PluginDrivenMvccSnapshot(
                connectorSnapshot, Collections.emptyMap(), Collections.emptyMap());
        ConnectorTableHandle pinnedHandle = Mockito.mock(ConnectorTableHandle.class);
        // applyMvccSnapshotPin unwraps the snapshot and calls metadata.applySnapshot(...) -> the pinned handle.
        Mockito.when(plugin.metadata.applySnapshot(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(pinnedHandle);

        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelDeleteSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA)).when(sink).getCols();

        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelDeleteSink(sink, context);
        PluginDrivenTableSink pluginSink = capturePluginSink(childFragment);
        try (MockedStatic<MvccUtil> mvcc = Mockito.mockStatic(MvccUtil.class)) {
            mvcc.when(() -> MvccUtil.getSnapshotFromContext(
                    plugin.table, Optional.empty(), Optional.empty())).thenReturn(Optional.of(pinned));
            pluginSink.bindDataSink(Optional.empty());
        }

        ConnectorWritePlanProvider provider = Deencapsulation.getField(pluginSink, "writePlanProvider");
        ArgumentCaptor<ConnectorWriteHandle> handle = ArgumentCaptor.forClass(ConnectorWriteHandle.class);
        Mockito.verify(provider).planWrite(Mockito.any(), handle.capture());
        Assertions.assertSame(pinnedHandle, handle.getValue().getTableHandle(),
                "the row-level DML write handle must carry the snapshot-pinned table handle (Fix B), not the raw"
                        + " latest-read handle");
    }

    @Test
    public void rowLevelDmlConnectorColumnsCarryTheWholeTypeNotJustItsPrimitiveTag() {
        // Production ALWAYS threads the hidden __DORIS_ICEBERG_ROWID_COL__ -- a STRUCT (file_path, pos, ...)
        // -- through a row-level DML sink's cols, so this arm sees a complex type on every DELETE/UPDATE/MERGE,
        // even against a table whose data columns are all scalar. Deriving the ConnectorType from the column's
        // primitive TAG alone ("STRUCT") drops the children, and a childless complex type is rejected outright
        // by ConnectorType's shape check -> the whole iceberg DML surface failed to translate (build 1006199).
        // The other tests here use a scalar-only fixture, which is exactly why they could not catch it.
        Column rowId = new Column(Column.ICEBERG_ROWID_COL, new StructType(
                new StructField("file_path", Type.STRING),
                new StructField("pos", Type.BIGINT)));

        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelDeleteSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(DATA, rowId)).when(sink).getCols();

        PlanTranslatorContext context = new PlanTranslatorContext();
        PhysicalPlanTranslator translator = new PhysicalPlanTranslator(context, null);
        translator.visitPhysicalExternalRowLevelDeleteSink(sink, context);

        @SuppressWarnings("unchecked")
        List<ConnectorColumn> connectorColumns = (List<ConnectorColumn>) Deencapsulation.getField(
                capturePluginSink(childFragment), "connectorColumns");
        ConnectorType rowIdType = connectorColumns.get(1).getType();
        Assertions.assertEquals("STRUCT", rowIdType.getTypeName().toUpperCase(Locale.ROOT),
                "the row-id column must keep its STRUCT tag");
        Assertions.assertEquals(ImmutableList.of("file_path", "pos"), rowIdType.getFieldNames(),
                "the row-id STRUCT must carry its field names, not be flattened to a bare tag");
        Assertions.assertEquals(2, rowIdType.getChildren().size(),
                "the row-id STRUCT must carry its child types, not be flattened to a bare tag");
    }

    @Test
    public void rowLevelDmlPreservesNestedComputeVariantCarrierForConnectorSchemaBinding() {
        Column variant = new Column("variant_data",
                ArrayType.create(new ConnectorComputeVariantType(), true));
        PlanFragment childFragment = Mockito.mock(PlanFragment.class);
        Plugin plugin = pluginTable();

        @SuppressWarnings("unchecked")
        PhysicalExternalRowLevelDeleteSink<Plan> sink = Mockito.mock(PhysicalExternalRowLevelDeleteSink.class);
        Mockito.doReturn(mockChild(childFragment)).when(sink).child();
        Mockito.doReturn(plugin.table).when(sink).getTargetTable();
        Mockito.doReturn(ImmutableList.of(variant)).when(sink).getCols();

        PlanTranslatorContext context = new PlanTranslatorContext();
        new PhysicalPlanTranslator(context, null).visitPhysicalExternalRowLevelDeleteSink(sink, context);

        @SuppressWarnings("unchecked")
        List<ConnectorColumn> connectorColumns = (List<ConnectorColumn>) Deencapsulation.getField(
                capturePluginSink(childFragment), "connectorColumns");
        Assertions.assertEquals("VARIANT_COMPUTE_V2",
                connectorColumns.get(0).getType().getChildren().get(0).getTypeName(),
                "row-level writes must not report an execution-only external Variant as persisted VARIANT");
    }

    // ==================== helpers ====================

    /** A column-less slot (no backing Column, so its slot col_name is empty until the loop materializes it). */
    private static SlotReference registerSlot(PlanTranslatorContext context, TupleDescriptor tuple, String name) {
        SlotReference slot = new SlotReference(name, IntegerType.INSTANCE);
        context.createSlotDesc(tuple, slot);
        return slot;
    }

    private static Plan mockChild(PlanFragment childFragment) {
        Plan child = Mockito.mock(Plan.class);
        Mockito.doReturn(childFragment).when(child).accept(Mockito.any(), Mockito.any());
        return child;
    }

    /** The mocked plugin connector chain, exposing the pieces a test needs to stub/assert. */
    private static final class Plugin {
        private final PluginDrivenExternalTable table;
        private final ConnectorMetadata metadata;

        private Plugin(PluginDrivenExternalTable table, ConnectorMetadata metadata) {
            this.table = table;
            this.metadata = metadata;
        }
    }

    /** A plugin-driven table whose connector resolves a non-null write provider + present table handle. */
    private static Plugin pluginTable() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        // The write seams now resolve metadata through the per-statement funnel, which reads the session's
        // statement scope; offline tests use NONE (a fresh getMetadata per call, byte-identical to pre-funnel).
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        // Production selects the write provider per-handle; a plain mock does not run the interface default.
        Mockito.when(connector.getWritePlanProvider(Mockito.any())).thenReturn(provider);
        // The row-level DML gate (buildPluginRowLevelDmlSink) resolves the handle, fetches the per-handle
        // provider and admits on ITS supportedOperations containing DELETE/MERGE.
        Mockito.when(provider.supportedOperations())
                .thenReturn(EnumSet.of(WriteOperation.DELETE, WriteOperation.MERGE));
        Mockito.when(provider.planWrite(Mockito.any(), Mockito.any()))
                .thenReturn(new ConnectorSinkPlan(new TDataSink()));
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
        return new Plugin(table, metadata);
    }

    private static PluginDrivenTableSink capturePluginSink(PlanFragment childFragment) {
        ArgumentCaptor<DataSink> captor = ArgumentCaptor.forClass(DataSink.class);
        Mockito.verify(childFragment).setSink(captor.capture());
        DataSink built = captor.getValue();
        Assertions.assertTrue(built instanceof PluginDrivenTableSink,
                "a post-flip row-level DML must route through the generic PluginDrivenTableSink, was "
                        + built.getClass().getSimpleName());
        return (PluginDrivenTableSink) built;
    }

    @SuppressWarnings("unchecked")
    private static void assertConnectorColumnsFromCols(PluginDrivenTableSink pluginSink) {
        List<ConnectorColumn> connectorColumns =
                (List<ConnectorColumn>) Deencapsulation.getField(pluginSink, "connectorColumns");
        Assertions.assertEquals(1, connectorColumns.size(),
                "the connector columns must be derived from the sink's getCols()");
        Assertions.assertEquals("data", connectorColumns.get(0).getName(),
                "the connector column name must carry the sink column name");
        // This is the exact translator output consumed by the provider. Parameterless Doris scalars expose
        // internal 0/0 defaults, but the connector schema contract uses the canonical -1/-1 representation.
        Assertions.assertEquals(-1, connectorColumns.get(0).getType().getPrecision());
        Assertions.assertEquals(-1, connectorColumns.get(0).getType().getScale());
    }
}
