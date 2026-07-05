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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.VerifyException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Parity oracle for the connector-resident {@link IcebergWriterHelper} — the self-contained port of legacy
 * {@code org.apache.doris.datasource.iceberg.helper.IcebergWriterHelper} (P6.3-T04). The connector cannot
 * import fe-core, so the data-file / delete-file / PartitionData / Metrics conversion is reproduced
 * byte-faithfully against the iceberg SDK. The delete-file cases mirror the legacy
 * {@code IcebergWriterHelperTest} cell-by-cell; the data-file / getFileFormat cases are added for T04.
 *
 * <p>Deliberate, documented deltas vs legacy: {@code CommonStatistics} is inlined (row count + file size are
 * passed straight to {@code genDataFile}), and the partition-value time-zone is a resolved {@code ZoneId}
 * argument (legacy reads a thread-local) — irrelevant for the unpartitioned specs used here.</p>
 */
public class IcebergWriterHelperTest {

    private final Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()));
    private final PartitionSpec unpartitionedSpec = PartitionSpec.unpartitioned();
    private final FileFormat format = FileFormat.PARQUET;

    private Table tableWith(String... props) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        for (int i = 0; i + 1 < props.length; i += 2) {
            properties.put(props[i], props[i + 1]);
        }
        return catalog.createTable(TableIdentifier.of("db1", "t"), schema, unpartitionedSpec, properties);
    }

    // ─────────────────── convertToDeleteFiles: ported from fe-core IcebergWriterHelperTest ───────────────────

    @Test
    public void convertToDeleteFilesEmptyList() {
        Assertions.assertTrue(IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, new ArrayList<>(), ZoneOffset.UTC).isEmpty());
    }

    @Test
    public void convertToDeleteFilesIgnoresDataFiles() {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("/path/to/data.parquet");
        d.setRowCount(100);
        d.setFileSize(1024);
        d.setFileContent(TFileContent.DATA);

        Assertions.assertTrue(IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, Collections.singletonList(d), ZoneOffset.UTC).isEmpty());
    }

    @Test
    public void convertToDeleteFilesPositionDelete() {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("/path/to/delete.parquet");
        d.setRowCount(10);
        d.setFileSize(512);
        d.setFileContent(TFileContent.POSITION_DELETES);
        d.setReferencedDataFilePath("/path/to/data.parquet");

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, Collections.singletonList(d), ZoneOffset.UTC);

        Assertions.assertEquals(1, deleteFiles.size());
        DeleteFile df = deleteFiles.get(0);
        Assertions.assertEquals("/path/to/delete.parquet", df.path());
        Assertions.assertEquals(10, df.recordCount());
        Assertions.assertEquals(512, df.fileSizeInBytes());
        Assertions.assertEquals(org.apache.iceberg.FileContent.POSITION_DELETES, df.content());
    }

    @Test
    public void convertToDeleteFilesDeletionVectorUsesPuffinMetadata() {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("/path/to/delete.puffin");
        d.setRowCount(7);
        d.setFileSize(2048);
        d.setFileContent(TFileContent.DELETION_VECTOR);
        d.setContentOffset(128L);
        d.setContentSizeInBytes(64L);
        d.setReferencedDataFilePath("/path/to/data.parquet");

        List<DeleteFile> deleteFiles = IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, Collections.singletonList(d), ZoneOffset.UTC);

        Assertions.assertEquals(1, deleteFiles.size());
        DeleteFile df = deleteFiles.get(0);
        Assertions.assertEquals(FileFormat.PUFFIN, df.format());
        Assertions.assertEquals(128L, df.contentOffset());
        Assertions.assertEquals(64L, df.contentSizeInBytes());
        Assertions.assertEquals("/path/to/data.parquet", df.referencedDataFile());
        Assertions.assertEquals(org.apache.iceberg.FileContent.POSITION_DELETES, df.content());
    }

    @Test
    public void convertToDeleteFilesRejectsEqualityDelete() {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("/path/to/delete.parquet");
        d.setRowCount(20);
        d.setFileSize(1024);
        d.setFileContent(TFileContent.EQUALITY_DELETES);

        Assertions.assertThrows(VerifyException.class, () -> IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, Collections.singletonList(d), ZoneOffset.UTC));
    }

    @Test
    public void convertToDeleteFilesMultiple() {
        TIcebergCommitData d1 = new TIcebergCommitData();
        d1.setFilePath("/path/to/delete1.parquet");
        d1.setRowCount(10);
        d1.setFileSize(512);
        d1.setFileContent(TFileContent.POSITION_DELETES);
        TIcebergCommitData d2 = new TIcebergCommitData();
        d2.setFilePath("/path/to/delete2.parquet");
        d2.setRowCount(20);
        d2.setFileSize(1024);
        d2.setFileContent(TFileContent.POSITION_DELETES);

        Assertions.assertEquals(2, IcebergWriterHelper.convertToDeleteFiles(
                format, unpartitionedSpec, Arrays.asList(d1, d2), ZoneOffset.UTC).size());
    }

    // ─────────────────── convertToWriterResult: data-file conversion (T04) ───────────────────

    @Test
    public void convertToWriterResultBuildsDataFiles() {
        Table table = tableWith("write.format.default", "parquet");

        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("s3://b/db1/t/f.parquet");
        d.setRowCount(100L);
        d.setFileSize(4096L);
        d.setFileContent(TFileContent.DATA);

        WriteResult result = IcebergWriterHelper.convertToWriterResult(
                table, Collections.singletonList(d), ZoneOffset.UTC);

        Assertions.assertEquals(1, result.dataFiles().length);
        DataFile df = result.dataFiles()[0];
        Assertions.assertEquals("s3://b/db1/t/f.parquet", df.path());
        Assertions.assertEquals(100L, df.recordCount());
        Assertions.assertEquals(4096L, df.fileSizeInBytes());
        Assertions.assertEquals(FileFormat.PARQUET, df.format());
    }

    @Test
    public void convertToWriterResultCarriesColumnMetrics() {
        Table table = tableWith("write.format.default", "parquet");

        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.putToColumnSizes(1, 100L);
        stats.putToValueCounts(1, 10L);
        stats.putToNullValueCounts(1, 2L);
        stats.putToLowerBounds(1, ByteBuffer.wrap(new byte[] {1}));
        stats.putToUpperBounds(1, ByteBuffer.wrap(new byte[] {9}));

        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath("s3://b/db1/t/f.parquet");
        d.setRowCount(10L);
        d.setFileSize(512L);
        d.setFileContent(TFileContent.DATA);
        d.setColumnStats(stats);

        WriteResult result = IcebergWriterHelper.convertToWriterResult(
                table, Collections.singletonList(d), ZoneOffset.UTC);

        DataFile df = result.dataFiles()[0];
        // MUTATION: dropping the TIcebergColumnStats -> Metrics maps absent -> red.
        Assertions.assertEquals(Long.valueOf(100L), df.columnSizes().get(1));
        Assertions.assertEquals(Long.valueOf(10L), df.valueCounts().get(1));
        Assertions.assertEquals(Long.valueOf(2L), df.nullValueCounts().get(1));
    }

    // ─────────────────── getFileFormat: 3-tier resolution (T04) ───────────────────

    @Test
    public void getFileFormatReadsWriteFormatDefault() {
        Assertions.assertEquals(FileFormat.ORC,
                IcebergWriterHelper.getFileFormat(tableWith("write.format.default", "orc")));
        Assertions.assertEquals(FileFormat.PARQUET,
                IcebergWriterHelper.getFileFormat(tableWith("write.format.default", "parquet")));
    }

    @Test
    public void getFileFormatReadsWriteFormatNickname() {
        // "write-format" (Flink/Spark nickname) wins over the standard property.
        Assertions.assertEquals(FileFormat.ORC,
                IcebergWriterHelper.getFileFormat(tableWith("write-format", "orc")));
    }

    @Test
    public void getFileFormatDefaultsToParquetWhenUnset() {
        // No format property + no data files -> infer falls back to parquet (legacy default).
        Assertions.assertEquals(FileFormat.PARQUET, IcebergWriterHelper.getFileFormat(tableWith()));
    }

    @Test
    public void getFileFormatThrowsOnUnsupported() {
        Assertions.assertThrows(RuntimeException.class,
                () -> IcebergWriterHelper.getFileFormat(tableWith("write.format.default", "avro")));
    }
}
