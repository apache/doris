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

package org.apache.doris.datasource.iceberg.helper;

import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test for IcebergWriterHelper DeleteFile conversion
 */
public class IcebergWriterHelperTest {

    private Schema schema;
    private PartitionSpec unpartitionedSpec;
    private FileFormat format;

    @BeforeEach
    public void setUp() {
        // Create a simple schema
        schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );

        // Create unpartitioned spec
        unpartitionedSpec = PartitionSpec.unpartitioned();

        // Use Parquet format
        format = FileFormat.PARQUET;

    }

    @Test
    public void testConvertToDeleteFiles_EmptyList() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();
        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, commitDataList);

        Assertions.assertTrue(deleteFiles.isEmpty());
    }

    @Test
    public void testConvertToDeleteFiles_DataFileIgnored() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();

        // Add a DATA file (should be ignored)
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/data.parquet");
        commitData.setRowCount(100);
        commitData.setFileSize(1024);
        commitData.setFileContent(TFileContent.DATA);
        commitDataList.add(commitData);

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, commitDataList);

        Assertions.assertTrue(deleteFiles.isEmpty());
    }

    @Test
    public void testConvertToDeleteFiles_PositionDelete() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/delete.parquet");
        commitData.setRowCount(10);
        commitData.setFileSize(512);
        commitData.setFileContent(TFileContent.POSITION_DELETES);
        commitData.setReferencedDataFilePath("/path/to/data.parquet");
        commitDataList.add(commitData);

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, commitDataList);

        Assertions.assertEquals(1, deleteFiles.size());
        DeleteFile deleteFile = deleteFiles.get(0);
        Assertions.assertEquals("/path/to/delete.parquet", deleteFile.path());
        Assertions.assertEquals(10, deleteFile.recordCount());
        Assertions.assertEquals(512, deleteFile.fileSizeInBytes());
        Assertions.assertEquals(org.apache.iceberg.FileContent.POSITION_DELETES, deleteFile.content());
    }

    @Test
    public void testConvertToDeleteFiles_UnsupportedDeleteContent() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/delete.parquet");
        commitData.setRowCount(20);
        commitData.setFileSize(1024);
        commitData.setFileContent(TFileContent.EQUALITY_DELETES);
        commitDataList.add(commitData);

        Assertions.assertThrows(com.google.common.base.VerifyException.class, () -> {
            IcebergWriterHelper.convertToDeleteFiles(
                    format, unpartitionedSpec, commitDataList);
        });
    }

    @Test
    public void testConvertToDeleteFiles_MultipleDeleteFiles() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();

        // Add position delete
        TIcebergCommitData commitData1 = new TIcebergCommitData();
        commitData1.setFilePath("/path/to/delete1.parquet");
        commitData1.setRowCount(10);
        commitData1.setFileSize(512);
        commitData1.setFileContent(TFileContent.POSITION_DELETES);
        commitDataList.add(commitData1);

        // Add another position delete
        TIcebergCommitData commitData2 = new TIcebergCommitData();
        commitData2.setFilePath("/path/to/delete2.parquet");
        commitData2.setRowCount(20);
        commitData2.setFileSize(1024);
        commitData2.setFileContent(TFileContent.POSITION_DELETES);
        commitDataList.add(commitData2);

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, commitDataList);

        Assertions.assertEquals(2, deleteFiles.size());
    }
}
