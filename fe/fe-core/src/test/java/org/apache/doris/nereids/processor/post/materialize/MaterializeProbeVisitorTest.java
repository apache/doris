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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarBinaryType;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class MaterializeProbeVisitorTest {

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

    /** Verifies vector_search can lazily fetch a plain VARBINARY column (no longer blanket-blocked). */
    @Test
    void testVectorSearchAllowsVarbinaryColumnLazyMaterialization() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();
        SlotReference varbinarySlot = Mockito.mock(SlotReference.class);
        Mockito.when(varbinarySlot.getDataType()).thenReturn(VarBinaryType.INSTANCE);
        Mockito.when(varbinarySlot.getOriginalColumn()).thenReturn(
                Optional.of(Mockito.mock(Column.class)));
        Mockito.when(relation.getOutput()).thenReturn(Collections.singletonList(varbinarySlot));
        Mockito.when(relation.getOperativeSlots()).thenReturn(Collections.emptyList());

        Assertions.assertTrue(visitor.visitPhysicalTVFRelation(
                relation, new MaterializeProbeVisitor.ProbeContext(varbinarySlot)).isPresent());
    }

    /** Verifies vector_search can lazily fetch regular scalar columns. */
    @Test
    void testVectorSearchAllowsScalarColumnLazyMaterialization() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();
        SlotReference scalarSlot = Mockito.mock(SlotReference.class);
        Mockito.when(scalarSlot.getDataType()).thenReturn(IntegerType.INSTANCE);
        Mockito.when(scalarSlot.getOriginalColumn()).thenReturn(
                Optional.of(Mockito.mock(Column.class)));
        Mockito.when(relation.getOutput()).thenReturn(Collections.singletonList(scalarSlot));
        Mockito.when(relation.getOperativeSlots()).thenReturn(Collections.emptyList());

        Assertions.assertTrue(visitor.visitPhysicalTVFRelation(
                relation, new MaterializeProbeVisitor.ProbeContext(scalarSlot)).isPresent());
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

    /** Creates a vector_search physical relation mock. */
    private PhysicalTVFRelation mockVectorSearchRelation() {
        PhysicalTVFRelation relation = Mockito.mock(PhysicalTVFRelation.class);
        VectorSearch function = Mockito.mock(VectorSearch.class);
        Mockito.when(function.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        Mockito.when(relation.getFunction()).thenReturn(function);
        return relation;
    }
}
