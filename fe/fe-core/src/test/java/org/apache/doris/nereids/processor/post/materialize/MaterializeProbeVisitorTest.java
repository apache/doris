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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class MaterializeProbeVisitorTest {

    @Test
    void testIcebergVariantTopNStaysInInitialScan() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergExternalDatabase database = Mockito.mock(IcebergExternalDatabase.class);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getRemoteName()).thenReturn("db");
        IcebergExternalTable table = new IcebergExternalTable(1, "tbl", "tbl", catalog, database);
        for (DataType type : externalVariantTypes()) {
            List<Slot> output = topNOutput(type);
            PhysicalFileScan scan = new PhysicalFileScan(new RelationId(1), table, ImmutableList.of("db"),
                    null, Optional.empty(), new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT),
                    null, Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty());
            assertVariantTopNStaysInInitialScan(scan, output);
        }
    }

    @Test
    void testFileTVFVariantTopNStaysInInitialScan() {
        for (String functionName : ImmutableList.of("s3", "hdfs", "local")) {
            for (String format : ImmutableList.of("parquet", "orc")) {
                for (DataType type : externalVariantTypes()) {
                    List<Slot> output = topNOutput(type);
                    VectorSearch function = Mockito.mock(VectorSearch.class);
                    Mockito.when(function.getName()).thenReturn(functionName);
                    Mockito.when(function.getTVFProperties())
                            .thenReturn(new Properties(ImmutableMap.of("format", format)));
                    PhysicalTVFRelation scan = new PhysicalTVFRelation(new RelationId(1), function,
                            ImmutableList.of(), new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT));
                    assertVariantTopNStaysInInitialScan(scan, output);
                }
            }
        }
    }

    @Test
    void testOlapAndLanceVariantCanStillBeDeferred() {
        SlotReference slot = (SlotReference) topNOutput(VariantType.COMPUTE_V2_INSTANCE).get(1);
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        Assertions.assertTrue(visitor.visitPhysicalOlapScan(mockBaseOlapScan(slot),
                new MaterializeProbeVisitor.ProbeContext(slot)).isPresent());
        PhysicalTVFRelation lance = mockVectorSearchRelation();
        Mockito.when(lance.getOutput()).thenReturn(ImmutableList.of(slot));
        Mockito.when(lance.getOperativeSlots()).thenReturn(ImmutableList.of());
        Assertions.assertTrue(visitor.visitPhysicalTVFRelation(lance,
                new MaterializeProbeVisitor.ProbeContext(slot)).isPresent());
    }

    private List<DataType> externalVariantTypes() {
        return ImmutableList.of(VariantType.INSTANCE, VariantType.COMPUTE_V2_INSTANCE,
                ArrayType.of(VariantType.COMPUTE_V2_INSTANCE),
                MapType.of(IntegerType.INSTANCE, VariantType.COMPUTE_V2_INSTANCE),
                new StructType(ImmutableList.of(new StructField("nested",
                        ArrayType.of(VariantType.COMPUTE_V2_INSTANCE), true, ""))));
    }

    private List<Slot> topNOutput(DataType type) {
        return ImmutableList.of(new SlotReference("id", IntegerType.INSTANCE),
                new SlotReference("v", type).withColumn(new Column("v", type.toCatalogDataType())),
                new SlotReference("payload", IntegerType.INSTANCE)
                        .withColumn(new Column("payload", org.apache.doris.catalog.Type.INT)));
    }

    private void assertVariantTopNStaysInInitialScan(Plan scan, List<Slot> output) {
        PhysicalTopN<Plan> topN = new PhysicalTopN<>(
                ImmutableList.of(new OrderKey(output.get(0), true, true)), 10, 0,
                SortPhase.GATHER_SORT, scan.getLogicalProperties(), scan);
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        // VARIANT is passive here: only id participates in TopN ordering.
        Assertions.assertFalse(topN.accept(visitor,
                new MaterializeProbeVisitor.ProbeContext((SlotReference) output.get(1))).isPresent());
        Assertions.assertTrue(topN.accept(visitor,
                new MaterializeProbeVisitor.ProbeContext((SlotReference) output.get(2))).isPresent());
        Assertions.assertFalse(topN.accept(visitor,
                new MaterializeProbeVisitor.ProbeContext((SlotReference) output.get(0))).isPresent());
    }

    @Test
    void testExternalTimestampsStayInInitialScan() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        for (DataType type : ImmutableList.of(DateTimeV2Type.SYSTEM_DEFAULT, TimeStampTzType.SYSTEM_DEFAULT,
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT),
                MapType.of(IntegerType.INSTANCE, TimeStampTzType.SYSTEM_DEFAULT),
                new StructType(ImmutableList.of(new StructField("ts", DateTimeV2Type.SYSTEM_DEFAULT, true, ""))))) {
            SlotReference slot = new SlotReference("ts", type).withColumn(new Column("ts", type.toCatalogDataType()));
            PhysicalTVFRelation relation = mockVectorSearchRelation();
            Mockito.when(relation.getFunction().getName()).thenReturn("s3");
            Mockito.when(relation.getFunction().getTVFProperties())
                    .thenReturn(new Properties(ImmutableMap.of("format", "parquet")));
            Mockito.when(relation.getOutput()).thenReturn(ImmutableList.of(slot));
            Mockito.when(relation.getOperativeSlots()).thenReturn(ImmutableList.of());
            Assertions.assertFalse(visitor.visitPhysicalTVFRelation(
                    relation, new MaterializeProbeVisitor.ProbeContext(slot)).isPresent(), type.toSql());
        }
    }

    @Test
    void testExternalIntegerCanStillBeDeferred() {
        SlotReference slot = new SlotReference("id", IntegerType.INSTANCE)
                .withColumn(new Column("id", org.apache.doris.catalog.Type.INT));
        PhysicalTVFRelation relation = mockVectorSearchRelation();
        Mockito.when(relation.getFunction().getName()).thenReturn("s3");
        Mockito.when(relation.getFunction().getTVFProperties())
                .thenReturn(new Properties(ImmutableMap.of("format", "parquet")));
        Mockito.when(relation.getOutput()).thenReturn(ImmutableList.of(slot));
        Mockito.when(relation.getOperativeSlots()).thenReturn(ImmutableList.of());
        Assertions.assertTrue(new MaterializeProbeVisitor().visitPhysicalTVFRelation(
                relation, new MaterializeProbeVisitor.ProbeContext(slot)).isPresent());
    }

    @Test
    void testVectorSearchSupportsLazyMaterialization() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();

        Assertions.assertTrue(visitor.checkTVFRelationTableSupportedType(relation));
    }

    @Test
    void testVectorSearchKeepsNestedSubColumnInSearchPhase() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();
        SlotReference nestedSlot = Mockito.mock(SlotReference.class);
        Mockito.when(nestedSlot.hasSubColPath()).thenReturn(true);

        Assertions.assertFalse(visitor.visitPhysicalTVFRelation(
                relation, new MaterializeProbeVisitor.ProbeContext(nestedSlot)).isPresent());
    }

    @Test
    void testOlapScanUsesRelationSlotWithAccessPaths() {
        SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference relationSlot = contextSlot.withAccessPaths(
                ImmutableList.of(dataPath("nested")), ImmutableList.of());
        contextSlot = (SlotReference) contextSlot.withNullable(false);
        PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

        MaterializeProbeVisitor.ProbeContext context = new MaterializeProbeVisitor.ProbeContext(contextSlot);
        Optional<MaterializeSource> source = new MaterializeProbeVisitor().visitPhysicalOlapScan(scan, context);

        Assertions.assertTrue(source.isPresent());
        Assertions.assertSame(relationSlot, source.get().baseSlot);
        Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFilterUsingIndexUsesRelationSlotWithAccessPaths() {
        ConnectContext oldContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().topNLazyMaterializationUsingIndex = true;
        context.setThreadLocalInfo();
        try {
            SlotReference contextSlot = new SlotReference("a", IntegerType.INSTANCE);
            SlotReference relationSlot = contextSlot.withAccessPaths(
                    ImmutableList.of(dataPath("nested")), ImmutableList.of());
            contextSlot = (SlotReference) contextSlot.withNullable(false);
            PhysicalOlapScan scan = mockBaseOlapScan(relationSlot);

            PhysicalFilter<PhysicalOlapScan> filter = Mockito.mock(PhysicalFilter.class);
            Mockito.when(filter.child()).thenReturn(scan);
            Mockito.when(filter.getInputSlots()).thenReturn(ImmutableSet.of(contextSlot));

            MaterializeProbeVisitor.ProbeContext probeContext = new MaterializeProbeVisitor.ProbeContext(contextSlot);
            Optional<MaterializeSource> source =
                    new MaterializeProbeVisitor().visitPhysicalFilter(filter, probeContext);

            Assertions.assertTrue(source.isPresent());
            Assertions.assertSame(relationSlot, source.get().baseSlot);
            Assertions.assertEquals(relationSlot.getAllAccessPaths(), source.get().baseSlot.getAllAccessPaths());
        } finally {
            if (oldContext == null) {
                ConnectContext.remove();
            } else {
                oldContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    void testLazyMaterializeOutputKeepsBaseSlotAccessPaths() {
        Column column = Mockito.mock(Column.class);
        Mockito.when(column.getName()).thenReturn("a");
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseColumnIdxByName("a")).thenReturn(0);
        PhysicalOlapScan relation = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(relation.getTable()).thenReturn(table);
        Mockito.when(relation.getAllChildrenTypes()).thenReturn(new BitSet());

        List<TColumnAccessPath> allPaths = ImmutableList.of(dataPath("all"));
        List<TColumnAccessPath> predicatePaths = ImmutableList.of(dataPath("predicate"));
        List<TColumnAccessPath> displayAllPaths = ImmutableList.of(dataPath("display_all"));
        List<TColumnAccessPath> displayPredicatePaths = ImmutableList.of(dataPath("display_predicate"));
        SlotReference baseSlot = new SlotReference("a", IntegerType.INSTANCE)
                .withColumn(column)
                .withAccessPaths(allPaths, predicatePaths, displayAllPaths, displayPredicatePaths);
        SlotReference lazySlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference rowId = new SlotReference("__DORIS_ROWID_COL__", IntegerType.INSTANCE);

        BiMap<Relation, SlotReference> relationToRowId = HashBiMap.create();
        relationToRowId.put(relation, rowId);
        Map<Relation, List<Slot>> relationToLazySlotMap = ImmutableMap.of(
                relation, ImmutableList.<Slot>of(lazySlot));
        Map<Slot, MaterializeSource> materializeMap = ImmutableMap.of(
                lazySlot, new MaterializeSource(relation, baseSlot));
        PhysicalLazyMaterialize<PhysicalOlapScan> materialize = new PhysicalLazyMaterialize<>(
                relation, ImmutableList.of(rowId), ImmutableList.of(), relationToLazySlotMap,
                relationToRowId, materializeMap);

        SlotReference outputSlot = (SlotReference) materialize.getOutput().get(0);
        Assertions.assertEquals(Optional.of(allPaths), outputSlot.getAllAccessPaths());
        Assertions.assertEquals(Optional.of(predicatePaths), outputSlot.getPredicateAccessPaths());
        Assertions.assertEquals(Optional.of(displayAllPaths), outputSlot.getDisplayAllAccessPaths());
        Assertions.assertEquals(Optional.of(displayPredicatePaths), outputSlot.getDisplayPredicateAccessPaths());
    }

    private TColumnAccessPath dataPath(String... path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
        accessPath.data_access_path = new TDataAccessPath(ImmutableList.copyOf(path));
        return accessPath;
    }

    private PhysicalOlapScan mockBaseOlapScan(SlotReference outputSlot) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(1L);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(outputSlot));
        Mockito.when(scan.getOperativeSlots()).thenReturn(ImmutableList.of());
        return scan;
    }

    private PhysicalTVFRelation mockVectorSearchRelation() {
        PhysicalTVFRelation relation = Mockito.mock(PhysicalTVFRelation.class);
        VectorSearch function = Mockito.mock(VectorSearch.class);
        Mockito.when(function.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        Mockito.when(relation.getFunction()).thenReturn(function);
        return relation;
    }
}
