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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * 测试 Iceberg 隐藏列功能
 */
public class IcebergHiddenColumnTest {

    @Test
    public void testHiddenColumnStructType() {
        // 获取隐藏列类型
        Type rowIdType = IcebergRowId.getRowIdType();
        Assertions.assertTrue(rowIdType instanceof StructType);

        StructType structType = (StructType) rowIdType;
        List<StructField> fields = structType.getFields();
        Assertions.assertEquals(4, fields.size());

        // 验证字段名称（不带 $ 前缀）
        Assertions.assertEquals("file_path", fields.get(0).getName());
        Assertions.assertEquals("row_position", fields.get(1).getName());
        Assertions.assertEquals("partition_spec_id", fields.get(2).getName());
        Assertions.assertEquals("partition_data", fields.get(3).getName());

        // 验证字段类型
        Assertions.assertTrue(fields.get(0).getType().isStringType());
        Assertions.assertTrue(fields.get(1).getType().isBigIntType());
        Assertions.assertTrue(fields.get(2).getType().isScalarType(PrimitiveType.INT));
        Assertions.assertTrue(fields.get(3).getType().isStringType());
    }

    @Test
    public void testIcebergRowIdColumnName() {
        // 验证常量定义
        Assertions.assertEquals("__DORIS_ICEBERG_ROWID_COL__", Column.ICEBERG_ROWID_COL);

        // 验证以 __DORIS_ 开头
        Assertions.assertTrue(Column.ICEBERG_ROWID_COL.startsWith(Column.HIDDEN_COLUMN_PREFIX));
    }

    @Test
    public void testStructFieldOrder() {
        // 验证 STRUCT 字段顺序
        Type rowIdType = IcebergRowId.getRowIdType();
        StructType structType = (StructType) rowIdType;
        List<StructField> fields = structType.getFields();

        // 确保字段顺序正确（与 BE 一致）
        // 顺序：file_path, row_position, partition_spec_id, partition_data
        Assertions.assertEquals("file_path", fields.get(0).getName());
        Assertions.assertEquals("row_position", fields.get(1).getName());
        Assertions.assertEquals("partition_spec_id", fields.get(2).getName());
        Assertions.assertEquals("partition_data", fields.get(3).getName());
    }
}
