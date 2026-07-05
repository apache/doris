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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for IcebergMetadataColumn.
 */
public class IcebergMetadataColumnTest {

    @Test
    public void testFilePathColumn() {
        IcebergMetadataColumn filePath = IcebergMetadataColumn.FILE_PATH;

        Assertions.assertNotNull(filePath);
        Assertions.assertEquals("$file_path", filePath.getColumnName());
        Assertions.assertTrue(filePath.getColumnType().isStringType());
    }

    @Test
    public void testRowPositionColumn() {
        IcebergMetadataColumn rowPosition = IcebergMetadataColumn.ROW_POSITION;

        Assertions.assertNotNull(rowPosition);
        Assertions.assertEquals("$row_position", rowPosition.getColumnName());
        Assertions.assertTrue(rowPosition.getColumnType().isBigIntType());
    }

    @Test
    public void testPartitionSpecIdColumn() {
        IcebergMetadataColumn partitionSpecId = IcebergMetadataColumn.PARTITION_SPEC_ID;

        Assertions.assertNotNull(partitionSpecId);
        Assertions.assertEquals("$partition_spec_id", partitionSpecId.getColumnName());
        Assertions.assertTrue(partitionSpecId.getColumnType().isScalarType());
    }

    @Test
    public void testPartitionDataColumn() {
        IcebergMetadataColumn partitionData = IcebergMetadataColumn.PARTITION_DATA;

        Assertions.assertNotNull(partitionData);
        Assertions.assertEquals("$partition_data", partitionData.getColumnName());
        Assertions.assertTrue(partitionData.getColumnType().isStringType());
    }

    @Test
    public void testGetAllColumnNames() {
        Assertions.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$file_path"));
        Assertions.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$row_position"));
        Assertions.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$partition_spec_id"));
        Assertions.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$partition_data"));
        Assertions.assertFalse(IcebergMetadataColumn.getAllColumnNames().contains("$row_id"));
    }

    @Test
    public void testIsMetadataColumn() {
        Assertions.assertTrue(IcebergMetadataColumn.isMetadataColumn("$file_path"));
        Assertions.assertFalse(IcebergMetadataColumn.isMetadataColumn("regular_column"));
        Assertions.assertFalse(IcebergMetadataColumn.isMetadataColumn(null));
        Assertions.assertFalse(IcebergMetadataColumn.isMetadataColumn("$row_id"));
    }

    @Test
    public void testFromColumnName() {
        Assertions.assertEquals(IcebergMetadataColumn.FILE_PATH,
                IcebergMetadataColumn.fromColumnName("$file_path"));
        Assertions.assertNull(IcebergMetadataColumn.fromColumnName("not_a_metadata_column"));
        Assertions.assertNull(IcebergMetadataColumn.fromColumnName("$row_id"));
    }
}
