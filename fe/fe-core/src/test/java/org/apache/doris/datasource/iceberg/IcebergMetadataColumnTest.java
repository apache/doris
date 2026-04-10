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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for IcebergMetadataColumn.
 */
public class IcebergMetadataColumnTest {

    @Test
    public void testFilePathColumn() {
        IcebergMetadataColumn filePath = IcebergMetadataColumn.FILE_PATH;

        Assert.assertNotNull(filePath);
        Assert.assertEquals("$file_path", filePath.getColumnName());
        Assert.assertTrue(filePath.getColumnType().isStringType());
    }

    @Test
    public void testRowPositionColumn() {
        IcebergMetadataColumn rowPosition = IcebergMetadataColumn.ROW_POSITION;

        Assert.assertNotNull(rowPosition);
        Assert.assertEquals("$row_position", rowPosition.getColumnName());
        Assert.assertTrue(rowPosition.getColumnType().isBigIntType());
    }

    @Test
    public void testPartitionSpecIdColumn() {
        IcebergMetadataColumn partitionSpecId = IcebergMetadataColumn.PARTITION_SPEC_ID;

        Assert.assertNotNull(partitionSpecId);
        Assert.assertEquals("$partition_spec_id", partitionSpecId.getColumnName());
        Assert.assertTrue(partitionSpecId.getColumnType().isScalarType());
    }

    @Test
    public void testPartitionDataColumn() {
        IcebergMetadataColumn partitionData = IcebergMetadataColumn.PARTITION_DATA;

        Assert.assertNotNull(partitionData);
        Assert.assertEquals("$partition_data", partitionData.getColumnName());
        Assert.assertTrue(partitionData.getColumnType().isStringType());
    }

    @Test
    public void testGetAllColumnNames() {
        Assert.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$file_path"));
        Assert.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$row_position"));
        Assert.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$partition_spec_id"));
        Assert.assertTrue(IcebergMetadataColumn.getAllColumnNames().contains("$partition_data"));
        Assert.assertFalse(IcebergMetadataColumn.getAllColumnNames().contains("$row_id"));
    }

    @Test
    public void testIsMetadataColumn() {
        Assert.assertTrue(IcebergMetadataColumn.isMetadataColumn("$file_path"));
        Assert.assertFalse(IcebergMetadataColumn.isMetadataColumn("regular_column"));
        Assert.assertFalse(IcebergMetadataColumn.isMetadataColumn(null));
        Assert.assertFalse(IcebergMetadataColumn.isMetadataColumn("$row_id"));
    }

    @Test
    public void testFromColumnName() {
        Assert.assertEquals(IcebergMetadataColumn.FILE_PATH,
                IcebergMetadataColumn.fromColumnName("$file_path"));
        Assert.assertNull(IcebergMetadataColumn.fromColumnName("not_a_metadata_column"));
        Assert.assertNull(IcebergMetadataColumn.fromColumnName("$row_id"));
    }
}
