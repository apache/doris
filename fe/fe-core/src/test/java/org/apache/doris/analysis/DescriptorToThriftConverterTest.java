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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TSlotDescriptor;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTupleDescriptor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class DescriptorToThriftConverterTest {

    // ==================== SlotDescriptor tests ====================

    @Test
    public void testSlotDescriptorBasic() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(1), new TupleId(0));
        slotDesc.setType(Type.INT);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertEquals(1, result.getId());
        Assertions.assertEquals(0, result.getParent());
        Assertions.assertEquals(Type.INT.toThrift(), result.getSlotType());
        Assertions.assertEquals(0, result.getNullIndicatorBit());
        Assertions.assertEquals("", result.getColName());
    }

    @Test
    public void testSlotDescriptorWithColumn() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(2), new TupleId(1));
        Column column = new Column("col_name", Type.INT);
        slotDesc.setColumn(column);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertTrue(result.isSetColUniqueId());
        Assertions.assertTrue(result.isSetPrimitiveType());
        Assertions.assertTrue(result.isSetIsKey());
        Assertions.assertEquals(column.getUniqueId(), result.getColUniqueId());
        Assertions.assertEquals(column.getDataType().toThrift(), result.getPrimitiveType());
        Assertions.assertEquals(column.isKey(), result.isIsKey());
    }

    @Test
    public void testSlotDescriptorWithMaterializedColumnName() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(3), new TupleId(0));
        Column column = new Column("original_name", Type.INT);
        slotDesc.setColumn(column);
        slotDesc.setMaterializedColumnName("materialized_name");

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertEquals("materialized_name", result.getColName());
    }

    @Test
    public void testSlotDescriptorNonNullable() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(4), new TupleId(0));
        slotDesc.setType(Type.INT);
        slotDesc.setIsNullable(false);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertEquals(-1, result.getNullIndicatorBit());
    }

    @Test
    public void testSlotDescriptorWithAutoIncrement() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(5), new TupleId(0));
        slotDesc.setType(Type.INT);
        slotDesc.setAutoInc(true);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertTrue(result.isIsAutoIncrement());
    }

    @Test
    public void testSlotDescriptorWithSubColPath() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(6), new TupleId(0));
        slotDesc.setType(Type.INT);
        List<String> subColPath = Arrays.asList("a", "b", "c");
        slotDesc.setSubColLables(subColPath);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertTrue(result.isSetColumnPaths());
        Assertions.assertEquals(subColPath, result.getColumnPaths());
    }

    @Test
    public void testSlotDescriptorWithAccessPaths() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(7), new TupleId(0));
        slotDesc.setType(Type.INT);
        List<ColumnAccessPath> allPaths = Arrays.asList(
                ColumnAccessPath.data(Arrays.asList("a", "b")),
                ColumnAccessPath.meta(Arrays.asList("c")));
        List<ColumnAccessPath> predPaths = Arrays.asList(
                ColumnAccessPath.data(Arrays.asList("x")));
        slotDesc.setAllAccessPaths(allPaths);
        slotDesc.setPredicateAccessPaths(predPaths);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertTrue(result.isSetAllAccessPaths());
        Assertions.assertEquals(2, result.getAllAccessPaths().size());
        Assertions.assertTrue(result.isSetPredicateAccessPaths());
        Assertions.assertEquals(1, result.getPredicateAccessPaths().size());
    }

    @Test
    public void testSlotDescriptorWithDefaultValue() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(8), new TupleId(0));
        Column column = new Column("col_with_default", Type.INT, true, null, "42", "");
        slotDesc.setColumn(column);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertTrue(result.isSetColDefaultValue());
        Assertions.assertEquals("42", result.getColDefaultValue());
    }

    @Test
    public void testSlotDescriptorWithAllFields() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(10), new TupleId(2));
        Column column = new Column("test_col", Type.INT);
        slotDesc.setColumn(column);
        slotDesc.setAutoInc(true);
        slotDesc.setMaterializedColumnName("mat_col");
        slotDesc.setSubColLables(Arrays.asList("x", "y"));
        List<ColumnAccessPath> allPaths = Arrays.asList(
                ColumnAccessPath.data(Arrays.asList("a")));
        List<ColumnAccessPath> predPaths = Arrays.asList(
                ColumnAccessPath.meta(Arrays.asList("b")));
        slotDesc.setAllAccessPaths(allPaths);
        slotDesc.setPredicateAccessPaths(predPaths);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertEquals(10, result.getId());
        Assertions.assertEquals(2, result.getParent());
        Assertions.assertEquals("mat_col", result.getColName());
        Assertions.assertTrue(result.isIsAutoIncrement());
        Assertions.assertEquals(Arrays.asList("x", "y"), result.getColumnPaths());
        Assertions.assertEquals(1, result.getAllAccessPaths().size());
        Assertions.assertEquals(1, result.getPredicateAccessPaths().size());
    }

    // ==================== TupleDescriptor tests ====================

    @Test
    public void testTupleDescriptorBasic() {
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(5));

        TTupleDescriptor result = DescriptorToThriftConverter.toThrift(tupleDesc);

        Assertions.assertEquals(5, result.getId());
        Assertions.assertEquals(0, result.getByteSize());
        Assertions.assertEquals(0, result.getNumNullBytes());
        Assertions.assertFalse(result.isSetTableId());
    }

    @Test
    public void testTupleDescriptorWithTable() {
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(6));
        TableIf mockTable = Mockito.mock(TableIf.class);
        Mockito.when(mockTable.getId()).thenReturn(12345L);
        tupleDesc.setTable(mockTable);

        TTupleDescriptor result = DescriptorToThriftConverter.toThrift(tupleDesc);

        Assertions.assertTrue(result.isSetTableId());
        Assertions.assertEquals(12345, result.getTableId());
    }

    @Test
    public void testTupleDescriptorWithNegativeTableId() {
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(7));
        TableIf mockTable = Mockito.mock(TableIf.class);
        Mockito.when(mockTable.getId()).thenReturn(-1L);
        tupleDesc.setTable(mockTable);

        TTupleDescriptor result = DescriptorToThriftConverter.toThrift(tupleDesc);

        Assertions.assertFalse(result.isSetTableId());
    }

    // ==================== DescriptorTable tests ====================

    @Test
    public void testDescriptorTableEmpty() {
        DescriptorTable descTable = new DescriptorTable();

        TDescriptorTable result = DescriptorToThriftConverter.toThrift(descTable);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getTupleDescriptors() == null || result.getTupleDescriptors().isEmpty());
        Assertions.assertTrue(result.getSlotDescriptors() == null || result.getSlotDescriptors().isEmpty());
    }

    @Test
    public void testDescriptorTableWithTuplesAndSlots() {
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple1 = descTable.createTupleDescriptor();
        SlotDescriptor slot1 = descTable.addSlotDescriptor(tuple1);
        slot1.setType(Type.INT);
        SlotDescriptor slot2 = descTable.addSlotDescriptor(tuple1);
        slot2.setType(Type.STRING);

        TupleDescriptor tuple2 = descTable.createTupleDescriptor();
        SlotDescriptor slot3 = descTable.addSlotDescriptor(tuple2);
        slot3.setType(Type.DOUBLE);

        TDescriptorTable result = DescriptorToThriftConverter.toThrift(descTable);

        Assertions.assertEquals(2, result.getTupleDescriptors().size());
        Assertions.assertEquals(3, result.getSlotDescriptors().size());
    }

    @Test
    public void testDescriptorTableReturnsNewInstanceEachCall() {
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor();
        SlotDescriptor slot = descTable.addSlotDescriptor(tuple);
        slot.setType(Type.INT);

        TDescriptorTable first = DescriptorToThriftConverter.toThrift(descTable);
        TDescriptorTable second = DescriptorToThriftConverter.toThrift(descTable);

        Assertions.assertNotSame(first, second);
        Assertions.assertEquals(first, second);
    }

    @Test
    public void testDescriptorTableWithReferencedTable() {
        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor();
        SlotDescriptor slot = descTable.addSlotDescriptor(tuple);
        slot.setType(Type.INT);

        TableIf mockTable = Mockito.mock(TableIf.class);
        Mockito.when(mockTable.getId()).thenReturn(100L);
        TTableDescriptor tTableDesc = new TTableDescriptor();
        Mockito.when(mockTable.toThrift()).thenReturn(tTableDesc);
        tuple.setTable(mockTable);

        TDescriptorTable result = DescriptorToThriftConverter.toThrift(descTable);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getTupleDescriptors().size());
        Assertions.assertEquals(100, result.getTupleDescriptors().get(0).getTableId());
        Assertions.assertNotNull(result.getTableDescriptors());
        Assertions.assertEquals(1, result.getTableDescriptors().size());
        Mockito.verify(mockTable).toThrift();
    }

    // ==================== ColumnAccessPath conversion tests ====================

    @Test
    public void testColumnAccessPathDataToThrift() {
        ColumnAccessPath accessPath = ColumnAccessPath.data(Arrays.asList("col1", "field1"));

        TColumnAccessPath result = DescriptorToThriftConverter.toThrift(accessPath);

        Assertions.assertEquals(TAccessPathType.DATA, result.getType());
        Assertions.assertTrue(result.isSetDataAccessPath());
        Assertions.assertFalse(result.isSetMetaAccessPath());
        Assertions.assertEquals(Arrays.asList("col1", "field1"), result.getDataAccessPath().getPath());
    }

    @Test
    public void testColumnAccessPathMetaToThrift() {
        ColumnAccessPath accessPath = ColumnAccessPath.meta(Arrays.asList("col2", "field2"));

        TColumnAccessPath result = DescriptorToThriftConverter.toThrift(accessPath);

        Assertions.assertEquals(TAccessPathType.META, result.getType());
        Assertions.assertFalse(result.isSetDataAccessPath());
        Assertions.assertTrue(result.isSetMetaAccessPath());
        Assertions.assertEquals(Arrays.asList("col2", "field2"), result.getMetaAccessPath().getPath());
    }

    @Test
    public void testColumnAccessPathListToThrift() {
        List<ColumnAccessPath> paths = Arrays.asList(
                ColumnAccessPath.data(Arrays.asList("a", "b")),
                ColumnAccessPath.meta(Arrays.asList("c")),
                ColumnAccessPath.data(Arrays.asList("d", "e", "f")));

        List<TColumnAccessPath> results = DescriptorToThriftConverter.toThrift(paths);

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals(TAccessPathType.DATA, results.get(0).getType());
        Assertions.assertEquals(Arrays.asList("a", "b"), results.get(0).getDataAccessPath().getPath());
        Assertions.assertEquals(TAccessPathType.META, results.get(1).getType());
        Assertions.assertEquals(Arrays.asList("c"), results.get(1).getMetaAccessPath().getPath());
        Assertions.assertEquals(TAccessPathType.DATA, results.get(2).getType());
        Assertions.assertEquals(Arrays.asList("d", "e", "f"), results.get(2).getDataAccessPath().getPath());
    }

    @Test
    public void testColumnAccessPathEmptyPathToThrift() {
        ColumnAccessPath accessPath = ColumnAccessPath.data(Arrays.asList());

        TColumnAccessPath result = DescriptorToThriftConverter.toThrift(accessPath);

        Assertions.assertEquals(TAccessPathType.DATA, result.getType());
        Assertions.assertTrue(result.isSetDataAccessPath());
        Assertions.assertEquals(Arrays.asList(), result.getDataAccessPath().getPath());
    }

    @Test
    public void testSlotDescriptorAccessPathsRoundTrip() {
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(20), new TupleId(0));
        slotDesc.setType(Type.INT);
        List<ColumnAccessPath> allPaths = Arrays.asList(
                ColumnAccessPath.data(Arrays.asList("x", "y")),
                ColumnAccessPath.meta(Arrays.asList("z")));
        slotDesc.setAllAccessPaths(allPaths);

        TSlotDescriptor result = DescriptorToThriftConverter.toThrift(slotDesc);

        Assertions.assertEquals(2, result.getAllAccessPaths().size());
        TColumnAccessPath first = result.getAllAccessPaths().get(0);
        Assertions.assertEquals(TAccessPathType.DATA, first.getType());
        Assertions.assertEquals(Arrays.asList("x", "y"), first.getDataAccessPath().getPath());
        TColumnAccessPath second = result.getAllAccessPaths().get(1);
        Assertions.assertEquals(TAccessPathType.META, second.getType());
        Assertions.assertEquals(Arrays.asList("z"), second.getMetaAccessPath().getPath());
    }
}
