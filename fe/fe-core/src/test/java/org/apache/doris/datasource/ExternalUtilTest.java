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

package org.apache.doris.datasource;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalUtilTest {

    @Test
    public void testInitSchemaInfoForPrunedColumnBasicAndNameMapping() {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Long schemaId = 100L;

        SlotDescriptor normalSlot = new SlotDescriptor(new SlotId(1), null);
        Column normalColumn = new Column("col1", Type.INT, true);
        normalColumn.setUniqueId(1);
        normalSlot.setType(Type.INT);
        normalSlot.setColumn(normalColumn);

        SlotDescriptor globalRowIdSlot = new SlotDescriptor(new SlotId(2), null);
        Column globalRowIdColumn = new Column(Column.GLOBAL_ROWID_COL + "_suffix", Type.BIGINT, true);
        globalRowIdColumn.setUniqueId(2);
        globalRowIdSlot.setType(Type.BIGINT);
        globalRowIdSlot.setColumn(globalRowIdColumn);

        List<SlotDescriptor> slots = Arrays.asList(normalSlot, globalRowIdSlot);

        Map<Integer, List<String>> nameMapping = new HashMap<>();
        List<String> mappedNames = Arrays.asList("mapped_col1");
        nameMapping.put(normalColumn.getUniqueId(), mappedNames);

        ExternalUtil.initSchemaInfoForPrunedColumn(params, schemaId, slots, nameMapping);

        Assert.assertEquals(schemaId.longValue(), params.getCurrentSchemaId());

        List<TSchema> history = params.getHistorySchemaInfo();
        Assert.assertEquals(1, history.size());

        TSchema tSchema = history.get(0);
        Assert.assertEquals(schemaId.longValue(), tSchema.getSchemaId());

        TStructField rootField = tSchema.getRootField();
        Assert.assertNotNull(rootField);
        // GLOBAL_ROWID_COL should be skipped
        Assert.assertEquals(1, rootField.getFieldsSize());

        TField field = rootField.getFields().get(0).getFieldPtr();
        Assert.assertEquals(normalColumn.getName(), field.getName());
        Assert.assertEquals(normalColumn.getUniqueId(), field.getId());
        Assert.assertEquals(normalColumn.isAllowNull(), field.isIsOptional());
        Assert.assertEquals(normalColumn.getType().toColumnTypeThrift(), field.getType());
        Assert.assertEquals(mappedNames, field.getNameMapping());
    }

    @Test
    public void testInitSchemaInfoForPrunedColumnWithArrayNestedType() {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Long schemaId = 200L;

        ArrayType arrayType = ArrayType.create(Type.INT, true);
        Column arrayColumn = new Column("arr_col", arrayType, true);
        arrayColumn.setUniqueId(10);
        arrayColumn.createChildrenColumn(arrayType, arrayColumn);

        SlotDescriptor arraySlot = new SlotDescriptor(new SlotId(3), null);
        arraySlot.setType(arrayType);
        arraySlot.setColumn(arrayColumn);

        List<SlotDescriptor> slots = Collections.singletonList(arraySlot);

        ExternalUtil.initSchemaInfoForPrunedColumn(params, schemaId, slots, null);

        List<TSchema> history = params.getHistorySchemaInfo();
        Assert.assertEquals(1, history.size());

        TSchema tSchema = history.get(0);
        Assert.assertEquals(schemaId.longValue(), tSchema.getSchemaId());

        TStructField rootField = tSchema.getRootField();
        Assert.assertNotNull(rootField);
        Assert.assertEquals(1, rootField.getFieldsSize());

        TField rootArrayField = rootField.getFields().get(0).getFieldPtr();
        Assert.assertTrue(rootArrayField.isSetNestedField());

        TNestedField nestedField = rootArrayField.getNestedField();
        Assert.assertTrue(nestedField.isSetArrayField());

        TArrayField tArrayField = nestedField.getArrayField();
        TField itemField = tArrayField.getItemField().getFieldPtr();
        Assert.assertEquals(arrayColumn.getChildren().get(0).getType().toColumnTypeThrift(), itemField.getType());
    }

    @Test
    public void testInitSchemaInfoForPrunedColumnWithStructNestedType() {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Long schemaId = 300L;

        StructType structType = new StructType(
                new StructField("f_int", Type.INT),
                new StructField("f_str", Type.VARCHAR));
        Column structColumn = new Column("struct_col", structType, true);
        structColumn.setUniqueId(20);
        structColumn.createChildrenColumn(structType, structColumn);

        SlotDescriptor structSlot = new SlotDescriptor(new SlotId(4), null);
        structSlot.setType(structType);
        structSlot.setColumn(structColumn);

        List<SlotDescriptor> slots = Collections.singletonList(structSlot);

        ExternalUtil.initSchemaInfoForPrunedColumn(params, schemaId, slots, null);

        List<TSchema> history = params.getHistorySchemaInfo();
        Assert.assertEquals(1, history.size());

        TSchema tSchema = history.get(0);
        Assert.assertEquals(schemaId.longValue(), tSchema.getSchemaId());

        TStructField rootField = tSchema.getRootField();
        Assert.assertNotNull(rootField);
        Assert.assertEquals(1, rootField.getFieldsSize());

        TField rootStructField = rootField.getFields().get(0).getFieldPtr();
        Assert.assertTrue(rootStructField.isSetNestedField());

        TNestedField nestedField = rootStructField.getNestedField();
        Assert.assertTrue(nestedField.isSetStructField());
        TStructField tStructField = nestedField.getStructField();

        Assert.assertEquals(2, tStructField.getFieldsSize());
        // 保证字段顺序和类型与 StructType 一致
        TField firstField = tStructField.getFields().get(0).getFieldPtr();
        TField secondField = tStructField.getFields().get(1).getFieldPtr();
        Assert.assertEquals(structColumn.getChildren().get(0).getType().toColumnTypeThrift(), firstField.getType());
        Assert.assertEquals(structColumn.getChildren().get(1).getType().toColumnTypeThrift(), secondField.getType());
    }

    @Test
    public void testInitSchemaInfoForPrunedColumnWithMapNestedType() {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Long schemaId = 400L;

        MapType mapType = new MapType(Type.VARCHAR, Type.INT);
        Column mapColumn = new Column("map_col", mapType, true);
        mapColumn.setUniqueId(30);
        mapColumn.createChildrenColumn(mapType, mapColumn);

        SlotDescriptor mapSlot = new SlotDescriptor(new SlotId(5), null);
        mapSlot.setType(mapType);
        mapSlot.setColumn(mapColumn);

        List<SlotDescriptor> slots = Collections.singletonList(mapSlot);

        ExternalUtil.initSchemaInfoForPrunedColumn(params, schemaId, slots, null);

        List<TSchema> history = params.getHistorySchemaInfo();
        Assert.assertEquals(1, history.size());

        TSchema tSchema = history.get(0);
        Assert.assertEquals(schemaId.longValue(), tSchema.getSchemaId());

        TStructField rootField = tSchema.getRootField();
        Assert.assertNotNull(rootField);
        Assert.assertEquals(1, rootField.getFieldsSize());

        TField rootMapField = rootField.getFields().get(0).getFieldPtr();
        Assert.assertTrue(rootMapField.isSetNestedField());

        TNestedField nestedField = rootMapField.getNestedField();
        Assert.assertTrue(nestedField.isSetMapField());

        // key / value 类型应与 children 中的列类型一致
        TField keyField = nestedField.getMapField().getKeyField().getFieldPtr();
        TField valueField = nestedField.getMapField().getValueField().getFieldPtr();
        Assert.assertEquals(mapColumn.getChildren().get(0).getType().toColumnTypeThrift(), keyField.getType());
        Assert.assertEquals(mapColumn.getChildren().get(1).getType().toColumnTypeThrift(), valueField.getType());
    }

    @Test
    public void testInitSchemaInfoForAllColumnMultipleColumnsAndNameMapping() {
        TFileScanRangeParams params = new TFileScanRangeParams();
        Long schemaId = 500L;

        Column col1 = new Column("c1", Type.INT, true);
        col1.setUniqueId(101);
        Column col2 = new Column("c2", Type.VARCHAR, false);
        col2.setUniqueId(102);

        List<Column> columns = Arrays.asList(col1, col2);

        Map<Integer, List<String>> nameMapping = new HashMap<>();
        nameMapping.put(col1.getUniqueId(), Arrays.asList("m_c1"));
        nameMapping.put(col2.getUniqueId(), Arrays.asList("m_c2_a", "m_c2_b"));

        ExternalUtil.initSchemaInfoForAllColumn(params, schemaId, columns, nameMapping);

        Assert.assertEquals(schemaId.longValue(), params.getCurrentSchemaId());
        List<TSchema> history = params.getHistorySchemaInfo();
        Assert.assertEquals(1, history.size());

        TSchema tSchema = history.get(0);
        Assert.assertEquals(schemaId.longValue(), tSchema.getSchemaId());

        TStructField rootField = tSchema.getRootField();
        Assert.assertNotNull(rootField);
        Assert.assertEquals(2, rootField.getFieldsSize());

        TField field1 = rootField.getFields().get(0).getFieldPtr();
        TField field2 = rootField.getFields().get(1).getFieldPtr();

        Assert.assertEquals(col1.getName(), field1.getName());
        Assert.assertEquals(col1.getUniqueId(), field1.getId());
        Assert.assertEquals(col1.isAllowNull(), field1.isIsOptional());
        Assert.assertEquals(col1.getType().toColumnTypeThrift(), field1.getType());
        Assert.assertEquals(Arrays.asList("m_c1"), field1.getNameMapping());

        Assert.assertEquals(col2.getName(), field2.getName());
        Assert.assertEquals(col2.getUniqueId(), field2.getId());
        Assert.assertEquals(col2.isAllowNull(), field2.isIsOptional());
        Assert.assertEquals(col2.getType().toColumnTypeThrift(), field2.getType());
        Assert.assertEquals(Arrays.asList("m_c2_a", "m_c2_b"), field2.getNameMapping());
    }
}

