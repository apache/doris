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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class SlotTypeReplacerTest {

    @Test
    void testIcebergAccessPathReplacesMixedCaseStructChildByFieldId() {
        Column column = mixedCaseStructColumn();
        LogicalFileScan scan = newIcebergScan(column);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        List<ColumnAccessPath> allPaths = ImmutableList.of(path("payload", "mixedfield"));
        List<ColumnAccessPath> predicatePaths = ImmutableList.of(path("payload", "mixedfield"));

        SlotReference replacedSlot = replaceSlot(scan, slot, column, allPaths, predicatePaths);

        List<ColumnAccessPath> expectedPaths = ImmutableList.of(path("10", "11"));
        Assertions.assertEquals(expectedPaths, replacedSlot.getAllAccessPaths().get());
        Assertions.assertEquals(expectedPaths, replacedSlot.getPredicateAccessPaths().get());
        Assertions.assertEquals(allPaths, replacedSlot.getDisplayAllAccessPaths().get());
        Assertions.assertEquals(predicatePaths, replacedSlot.getDisplayPredicateAccessPaths().get());
    }

    @Test
    void testIcebergVariantFullProjectionKeepsSubpathNamesAfterRootFieldId() {
        Column column = new Column("v", Type.VARIANT, true);
        column.setUniqueId(100);
        LogicalFileScan scan = newIcebergScan(column);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        List<ColumnAccessPath> allPaths = ImmutableList.of(path("v"));
        List<ColumnAccessPath> predicatePaths = ImmutableList.of(path("v", "Metric", "x"));

        SlotReference replacedSlot = replaceSlot(scan, slot, column, allPaths, predicatePaths);

        Assertions.assertEquals(ImmutableList.of(path("100")), replacedSlot.getAllAccessPaths().get());
        Assertions.assertEquals(ImmutableList.of(path("100", "Metric", "x")),
                replacedSlot.getPredicateAccessPaths().get());
        Assertions.assertEquals(allPaths, replacedSlot.getDisplayAllAccessPaths().get());
        Assertions.assertEquals(predicatePaths, replacedSlot.getDisplayPredicateAccessPaths().get());
    }

    @Test
    void testIcebergMapKeyAccessPathReplacesNestedKeyStructChildByFieldId() {
        Column column = mapWithStructKeyColumn();
        LogicalFileScan scan = newIcebergScan(column);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        List<ColumnAccessPath> allPaths = ImmutableList.of(path("payload", AccessPathInfo.ACCESS_MAP_KEYS,
                "keyfield"));
        List<ColumnAccessPath> predicatePaths = ImmutableList.of(path("payload", AccessPathInfo.ACCESS_MAP_KEYS,
                "keyfield"));

        SlotReference replacedSlot = replaceSlot(scan, slot, column, allPaths, predicatePaths);

        List<ColumnAccessPath> expectedPaths =
                ImmutableList.of(path("20", AccessPathInfo.ACCESS_MAP_KEYS, "22"));
        Assertions.assertEquals(expectedPaths, replacedSlot.getAllAccessPaths().get());
        Assertions.assertEquals(expectedPaths, replacedSlot.getPredicateAccessPaths().get());
        Assertions.assertEquals(allPaths, replacedSlot.getDisplayAllAccessPaths().get());
        Assertions.assertEquals(predicatePaths, replacedSlot.getDisplayPredicateAccessPaths().get());
    }

    @Test
    void testExceptUsesReplacedOutputsAndChildrenOutputs() {
        assertSetOperationUsesReplacedOutputs(true);
    }

    @Test
    void testIntersectUsesReplacedOutputsAndChildrenOutputs() {
        assertSetOperationUsesReplacedOutputs(false);
    }

    private void assertSetOperationUsesReplacedOutputs(boolean isExcept) {
        Column column = twoFieldStructColumn();
        LogicalFileScan leftScan = newIcebergScan(column);
        LogicalFileScan rightScan = newIcebergScan(column);
        SlotReference leftSlot = (SlotReference) leftScan.getOutput().get(0);
        SlotReference rightSlot = (SlotReference) rightScan.getOutput().get(0);
        DataType prunedType = new org.apache.doris.nereids.types.StructType(ImmutableList.of(
                new org.apache.doris.nereids.types.StructField(
                        "keep", org.apache.doris.nereids.types.IntegerType.INSTANCE, true, "")));
        List<ColumnAccessPath> allPaths = ImmutableList.of(path("payload", "keep"));
        Map<Integer, AccessPathInfo> accessPaths = new LinkedHashMap<>();
        accessPaths.put(leftSlot.getExprId().asInt(), new AccessPathInfo(prunedType, allPaths, allPaths));
        accessPaths.put(rightSlot.getExprId().asInt(), new AccessPathInfo(prunedType, allPaths, allPaths));

        Plan setOperation = isExcept
                ? new LogicalExcept(SetOperation.Qualifier.DISTINCT, ImmutableList.of(leftSlot),
                        ImmutableList.of(ImmutableList.of(leftSlot), ImmutableList.of(rightSlot)),
                        ImmutableList.of(leftScan, rightScan))
                : new LogicalIntersect(SetOperation.Qualifier.DISTINCT, ImmutableList.of(leftSlot),
                        ImmutableList.of(ImmutableList.of(leftSlot), ImmutableList.of(rightSlot)),
                        ImmutableList.of(leftScan, rightScan));

        Plan replacedPlan = new SlotTypeReplacer(accessPaths, setOperation).replace();
        List<SlotReference> firstChildOutputs = isExcept
                ? ((LogicalExcept) replacedPlan).getRegularChildOutput(0)
                : ((LogicalIntersect) replacedPlan).getRegularChildOutput(0);
        List<SlotReference> secondChildOutputs = isExcept
                ? ((LogicalExcept) replacedPlan).getRegularChildOutput(1)
                : ((LogicalIntersect) replacedPlan).getRegularChildOutput(1);
        List<? extends Slot> outputs = replacedPlan.getOutput();

        Assertions.assertEquals("STRUCT<keep:INT>", outputs.get(0).getDataType().toSql());
        Assertions.assertEquals("STRUCT<keep:INT>", firstChildOutputs.get(0).getDataType().toSql());
        Assertions.assertEquals("STRUCT<keep:INT>", secondChildOutputs.get(0).getDataType().toSql());
    }

    private SlotReference replaceSlot(LogicalFileScan scan, SlotReference slot, Column column,
            List<ColumnAccessPath> allPaths, List<ColumnAccessPath> predicatePaths) {
        Map<Integer, AccessPathInfo> accessPaths = new LinkedHashMap<>();
        accessPaths.put(slot.getExprId().asInt(),
                new AccessPathInfo(DataType.fromCatalogType(column.getType()), allPaths, predicatePaths));

        Plan replacedPlan = new SlotTypeReplacer(accessPaths, scan).replace();
        Slot replacedSlot = ((LogicalFileScan) replacedPlan).getOutput().get(0);
        return (SlotReference) replacedSlot;
    }

    private LogicalFileScan newIcebergScan(Column column) {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getFullSchema()).thenReturn(Collections.singletonList(column));
        Mockito.when(table.getName()).thenReturn("iceberg_tbl");
        return new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("iceberg_catalog", "iceberg_db"), Collections.emptyList(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private Column mixedCaseStructColumn() {
        StructType type = new StructType(new StructField("MixedField", Type.INT));
        Column column = new Column("payload", type, true);
        column.setUniqueId(10);
        column.getChildren().get(0).setName("MixedField");
        column.getChildren().get(0).setUniqueId(11);
        return column;
    }

    private Column twoFieldStructColumn() {
        StructType type = new StructType(
                new StructField("keep", Type.INT),
                new StructField("drop", Type.INT));
        Column column = new Column("payload", type, true);
        column.setUniqueId(10);
        column.getChildren().get(0).setName("keep");
        column.getChildren().get(0).setUniqueId(11);
        column.getChildren().get(1).setName("drop");
        column.getChildren().get(1).setUniqueId(12);
        return column;
    }

    private Column mapWithStructKeyColumn() {
        StructType keyType = new StructType(new StructField("KeyField", Type.INT));
        Column column = new Column("payload", new MapType(keyType, Type.INT), true);
        column.setUniqueId(20);
        Column keyColumn = column.getChildren().get(0);
        keyColumn.setUniqueId(21);
        keyColumn.getChildren().get(0).setName("KeyField");
        keyColumn.getChildren().get(0).setUniqueId(22);
        column.getChildren().get(1).setUniqueId(23);
        return column;
    }

    private ColumnAccessPath path(String... parts) {
        return ColumnAccessPath.data(ImmutableList.copyOf(parts));
    }
}
