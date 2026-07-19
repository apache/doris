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
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public void testConvertToWriterResultRespectsNoneMetricsMode() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(unpartitionedSpec);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(Map.of(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet",
                TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));

        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setColumnSizes(Map.of(2, 128L));
        columnStats.setValueCounts(Map.of(2, 10L));
        columnStats.setNullValueCounts(Map.of(2, 0L));
        columnStats.setLowerBounds(Map.of(2, ByteBuffer.wrap(new byte[] {0x01})));
        columnStats.setUpperBounds(Map.of(2, ByteBuffer.wrap(new byte[] {0x02})));

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/data.parquet");
        commitData.setRowCount(10);
        commitData.setFileSize(1024);
        commitData.setColumnStats(columnStats);

        WriteResult result = IcebergWriterHelper.convertToWriterResult(table, List.of(commitData));
        DataFile dataFile = result.dataFiles()[0];

        Assertions.assertTrue(dataFile.columnSizes() == null || dataFile.columnSizes().isEmpty());
        Assertions.assertTrue(dataFile.valueCounts() == null || dataFile.valueCounts().isEmpty());
        Assertions.assertTrue(dataFile.nullValueCounts() == null || dataFile.nullValueCounts().isEmpty());
        Assertions.assertTrue(dataFile.lowerBounds() == null || dataFile.lowerBounds().isEmpty());
        Assertions.assertTrue(dataFile.upperBounds() == null || dataFile.upperBounds().isEmpty());
    }

    @Test
    public void testConvertToWriterResultCountsModeOmitsBounds() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(unpartitionedSpec);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(Map.of(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet",
                TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));

        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setColumnSizes(Map.of(2, 128L));
        columnStats.setValueCounts(Map.of(2, 10L));
        columnStats.setNullValueCounts(Map.of(2, 0L));
        columnStats.setLowerBounds(Map.of(
                2, Conversions.toByteBuffer(Types.StringType.get(), "abcdefgh")));
        columnStats.setUpperBounds(Map.of(
                2, Conversions.toByteBuffer(Types.StringType.get(), "ijklmnop")));

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/data.parquet");
        commitData.setRowCount(10);
        commitData.setFileSize(1024);
        commitData.setColumnStats(columnStats);

        DataFile dataFile = IcebergWriterHelper.convertToWriterResult(table, List.of(commitData)).dataFiles()[0];

        Assertions.assertEquals(Map.of(2, 128L), dataFile.columnSizes());
        Assertions.assertEquals(Map.of(2, 10L), dataFile.valueCounts());
        Assertions.assertEquals(Map.of(2, 0L), dataFile.nullValueCounts());
        Assertions.assertTrue(dataFile.lowerBounds() == null || dataFile.lowerBounds().isEmpty());
        Assertions.assertTrue(dataFile.upperBounds() == null || dataFile.upperBounds().isEmpty());
    }

    @Test
    public void testConvertToWriterResultTruncatesStringAndBinaryBounds() {
        Schema boundsSchema = new Schema(
                Types.NestedField.optional(1, "text", Types.StringType.get()),
                Types.NestedField.optional(2, "payload", Types.BinaryType.get()));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(boundsSchema);
        Mockito.when(table.spec()).thenReturn(unpartitionedSpec);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(Map.of(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet",
                TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(3)"));

        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setLowerBounds(Map.of(
                1, Conversions.toByteBuffer(Types.StringType.get(), "abcdef"),
                2, ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));
        columnStats.setUpperBounds(Map.of(
                1, Conversions.toByteBuffer(Types.StringType.get(), "uvwxyz"),
                2, ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/data.parquet");
        commitData.setRowCount(10);
        commitData.setFileSize(1024);
        commitData.setColumnStats(columnStats);

        DataFile dataFile = IcebergWriterHelper.convertToWriterResult(table, List.of(commitData)).dataFiles()[0];

        Assertions.assertEquals("abc", Conversions.fromByteBuffer(
                Types.StringType.get(), dataFile.lowerBounds().get(1)).toString());
        Assertions.assertEquals("uvx", Conversions.fromByteBuffer(
                Types.StringType.get(), dataFile.upperBounds().get(1)).toString());
        Assertions.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), dataFile.lowerBounds().get(2));
        Assertions.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 4}), dataFile.upperBounds().get(2));
    }

    @Test
    public void testConvertToWriterResultBuildsMetricsPolicyOncePerBatch() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(unpartitionedSpec);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(Map.of(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet",
                TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));

        TIcebergCommitData firstCommit = new TIcebergCommitData();
        firstCommit.setFilePath("/path/to/first.parquet");
        firstCommit.setRowCount(10);
        firstCommit.setFileSize(1024);

        TIcebergCommitData secondCommit = new TIcebergCommitData();
        secondCommit.setFilePath("/path/to/second.parquet");
        secondCommit.setRowCount(20);
        secondCommit.setFileSize(2048);

        IcebergWriterHelper.convertToWriterResult(table, List.of(firstCommit, secondCommit));

        // One schema lookup is made by Iceberg's policy builder and one is captured for all files in the batch.
        Mockito.verify(table, Mockito.times(2)).schema();
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
    public void testConvertToDeleteFiles_DeletionVectorUsesPuffinMetadata() {
        List<TIcebergCommitData> commitDataList = new ArrayList<>();

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/delete.puffin");
        commitData.setRowCount(7);
        commitData.setFileSize(2048);
        commitData.setFileContent(TFileContent.DELETION_VECTOR);
        commitData.setContentOffset(128L);
        commitData.setContentSizeInBytes(64L);
        commitData.setReferencedDataFilePath("/path/to/data.parquet");
        commitDataList.add(commitData);

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, commitDataList);

        Assertions.assertEquals(1, deleteFiles.size());
        DeleteFile deleteFile = deleteFiles.get(0);
        Assertions.assertEquals(FileFormat.PUFFIN, deleteFile.format());
        Assertions.assertEquals(128L, deleteFile.contentOffset());
        Assertions.assertEquals(64L, deleteFile.contentSizeInBytes());
        Assertions.assertEquals("/path/to/data.parquet", deleteFile.referencedDataFile());
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
