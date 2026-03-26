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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * 测试 Iceberg 隐藏列功能
 */
public class IcebergHiddenColumnTest {

    @Test
    public void testHiddenColumnStructType() {
        // 获取隐藏列类型
        Type rowIdType = IcebergRowId.getRowIdType();
        Assert.assertTrue(rowIdType instanceof StructType);

        StructType structType = (StructType) rowIdType;
        List<StructField> fields = structType.getFields();
        Assert.assertEquals(4, fields.size());

        // 验证字段名称（不带 $ 前缀）
        Assert.assertEquals("file_path", fields.get(0).getName());
        Assert.assertEquals("row_position", fields.get(1).getName());
        Assert.assertEquals("partition_spec_id", fields.get(2).getName());
        Assert.assertEquals("partition_data", fields.get(3).getName());

        // 验证字段类型
        Assert.assertTrue(fields.get(0).getType().isStringType());
        Assert.assertTrue(fields.get(1).getType().isBigIntType());
        Assert.assertTrue(fields.get(2).getType().isScalarType(PrimitiveType.INT));
        Assert.assertTrue(fields.get(3).getType().isStringType());
    }

    @Test
    public void testIcebergRowIdColumnName() {
        // 验证常量定义
        Assert.assertEquals("__DORIS_ICEBERG_ROWID_COL__", Column.ICEBERG_ROWID_COL);

        // 验证以 __DORIS_ 开头
        Assert.assertTrue(Column.ICEBERG_ROWID_COL.startsWith(Column.HIDDEN_COLUMN_PREFIX));
    }

    @Test
    public void testStructFieldOrder() {
        // 验证 STRUCT 字段顺序
        Type rowIdType = IcebergRowId.getRowIdType();
        StructType structType = (StructType) rowIdType;
        List<StructField> fields = structType.getFields();

        // 确保字段顺序正确（与 BE 一致）
        // 顺序：file_path, row_position, partition_spec_id, partition_data
        Assert.assertEquals("file_path", fields.get(0).getName());
        Assert.assertEquals("row_position", fields.get(1).getName());
        Assert.assertEquals("partition_spec_id", fields.get(2).getName());
        Assert.assertEquals("partition_data", fields.get(3).getName());
    }
}
