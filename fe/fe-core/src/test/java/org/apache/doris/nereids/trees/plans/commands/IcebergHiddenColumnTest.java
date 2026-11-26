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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 测试 Iceberg 隐藏列常量。
 *
 * <p>隐藏列的 STRUCT 结构（字段名/顺序/类型）由连接器声明、单一来源，其形状由
 * {@code ConnectorColumnConverterTest.convertColumnReconstructsIcebergRowIdHiddenColumn}（fe-core 侧）
 * 与 {@code IcebergWritePlanProviderTest.getSyntheticWriteColumnsDeclaresRowIdStruct}（连接器侧）锁定。
 */
public class IcebergHiddenColumnTest {

    @Test
    public void testIcebergRowIdColumnName() {
        // 验证常量定义（FE↔BE 线协议列名，必须逐字节稳定）
        Assertions.assertEquals("__DORIS_ICEBERG_ROWID_COL__", Column.ICEBERG_ROWID_COL);

        // 验证以 __DORIS_ 开头
        Assertions.assertTrue(Column.ICEBERG_ROWID_COL.startsWith(Column.HIDDEN_COLUMN_PREFIX));
    }
}
