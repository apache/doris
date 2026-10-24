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
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        return tableWith(schema, props);
    }

    private Table tableWith(Schema tableSchema, String... props) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        for (int i = 0; i + 1 < props.length; i += 2) {
            properties.put(props[i], props[i + 1]);
        }
        return catalog.createTable(TableIdentifier.of("db1", "t"), tableSchema, unpartitionedSpec, properties);
    }

    // Builds one DATA-content commit fragment and returns the single converted data file. Row count / file size
    // are immaterial to the metrics assertions, so they are fixed; the column metrics come from {@code stats}.
    private DataFile writeSingle(Table table, TIcebergColumnStats stats, String path) {
        TIcebergCommitData d = new TIcebergCommitData();
        d.setFilePath(path);
        d.setRowCount(10L);
        d.setFileSize(1024L);
        d.setFileContent(TFileContent.DATA);
        d.setColumnStats(stats);
        return IcebergWriterHelper.convertToWriterResult(
                table, Collections.singletonList(d), ZoneOffset.UTC).dataFiles()[0];
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
    public void convertToWriterResultUsesPinnedMetadataAfterTableEvolution() {
        // Files are labeled with the spec, sort order, format and metrics policy the writer was pinned to, even after
        // the live table evolved every one of them.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Map<String, String> properties = new HashMap<>();
        properties.put("write.format.default", "orc");
        properties.put("write.metadata.metrics.default", "full");
        Table table = catalog.createTable(TableIdentifier.of("db1", "t"), schema,
                PartitionSpec.builderFor(schema).identity("id").build(), properties);
        table.replaceSortOrder().desc("age").commit();
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.create(
                table, "db1.t", Optional.empty(), false, false);
        int pinnedSortOrderId = table.sortOrder().orderId();
        table.updateSpec().removeField("id").commit();
        table.replaceSortOrder().asc("name").commit();
        table.updateProperties().set("write.format.default", "parquet")
                .set("write.metadata.metrics.default", "none").commit();

        ByteBuffer ageBound = Conversions.toByteBuffer(Types.IntegerType.get(), 42);
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setLowerBounds(Collections.singletonMap(3, ageBound));
        stats.setUpperBounds(Collections.singletonMap(3, ageBound));
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("/path/to/pinned-data.orc");
        commitData.setPartitionValues(Collections.singletonList("7"));
        commitData.setRowCount(1);
        commitData.setFileSize(128);
        commitData.setFileContent(TFileContent.DATA);
        commitData.setColumnStats(stats);

        DataFile dataFile = IcebergWriterHelper.convertToWriterResult(
                context, Collections.singletonList(commitData), ZoneOffset.UTC).dataFiles()[0];

        Assertions.assertNotEquals(table.spec().specId(), dataFile.specId(), "the live spec must have moved on");
        Assertions.assertEquals(context.getPartitionSpec().specId(), dataFile.specId());
        Assertions.assertEquals(Integer.valueOf(7), dataFile.partition().get(0, Integer.class));
        Assertions.assertEquals(Integer.valueOf(pinnedSortOrderId), dataFile.sortOrderId());
        Assertions.assertEquals(FileFormat.ORC, dataFile.format());
        Assertions.assertEquals(ageBound, dataFile.lowerBounds().get(3));
        Assertions.assertEquals(ageBound, dataFile.upperBounds().get(3));
    }

    @Test
    public void convertToWriterResultDecodesBinaryAndUuidPartitionValues() {
        // BE commits binary-like identity partitions as 0x hex; UUID may also arrive as canonical text.
        Schema binarySchema = new Schema(
                Types.NestedField.optional(1, "key", Types.BinaryType.get()),
                Types.NestedField.optional(2, "fixed_key", Types.FixedType.ofLength(2)),
                Types.NestedField.optional(3, "uuid_key", Types.UUIDType.get()));
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "binary_t"), binarySchema,
                PartitionSpec.builderFor(binarySchema).identity("key").identity("fixed_key").identity("uuid_key")
                        .build(),
                Collections.singletonMap("write.format.default", "parquet"));
        java.util.UUID uuid = java.util.UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        for (String uuidValue : Arrays.asList("0x00112233445566778899aabbccddeeff", uuid.toString())) {
            TIcebergCommitData commit = new TIcebergCommitData();
            commit.setFilePath("s3://b/db1/binary_t/binary.parquet");
            commit.setPartitionValues(Arrays.asList("0xdead", "0xdead", uuidValue));
            commit.setRowCount(1L);
            commit.setFileSize(128L);
            commit.setFileContent(TFileContent.DATA);
            DataFile file = IcebergWriterHelper.convertToWriterResult(
                    table, Collections.singletonList(commit), ZoneOffset.UTC).dataFiles()[0];
            Assertions.assertEquals(ByteBuffer.wrap(new byte[] {(byte) 0xde, (byte) 0xad}),
                    file.partition().get(0, ByteBuffer.class));
            Assertions.assertEquals(file.partition().get(0, ByteBuffer.class),
                    file.partition().get(1, ByteBuffer.class));
            Assertions.assertEquals(uuid, file.partition().get(2, java.util.UUID.class), uuidValue);
        }
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

    /**
     * NaN counts are the only metadata that can prove a float column holds no NaN, because iceberg keeps NaN
     * out of the bounds by spec. A reported zero is therefore a positive claim that lets a float range
     * predicate prune the file, so it must travel from BE untouched — and an absent count must stay absent.
     */
    @Test
    public void convertToWriterResultCarriesNanValueCounts() {
        Schema floatSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "d", Types.DoubleType.get()),
                Types.NestedField.optional(3, "f", Types.FloatType.get()));
        Table table = tableWith(floatSchema, "write.format.default", "parquet");

        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.putToValueCounts(2, 10L);
        stats.putToValueCounts(3, 10L);
        stats.putToNanValueCounts(2, 3L);
        // An explicit zero is the whole point: it is what brings pruning back for a NaN-free column.
        stats.putToNanValueCounts(3, 0L);

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.parquet");

        Assertions.assertEquals(Long.valueOf(3L), df.nanValueCounts().get(2));
        Assertions.assertEquals(Long.valueOf(0L), df.nanValueCounts().get(3));
    }

    /**
     * A BE that reports no NaN counts (an older one, or a format whose writer cannot count them) must leave the
     * metric absent, not zero: iceberg reads absent as "may contain NaN" and keeps the file, whereas a zero
     * would license pruning away rows that do match.
     */
    @Test
    public void convertToWriterResultLeavesNanValueCountsUnsetWhenBeReportsNone() {
        Table table = tableWith("write.format.default", "parquet");
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.putToValueCounts(1, 10L);
        stats.putToNullValueCounts(1, 0L);

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.parquet");

        Assertions.assertTrue(df.nanValueCounts() == null || df.nanValueCounts().isEmpty(),
                "an unreported NaN count must not become an empty-but-present or zero claim");
    }

    /**
     * Counting NaNs is an extra pass over the data (the parquet footer carries no NaN count), so BE must not
     * pay it for a field FE would then drop. {@code shouldCollectColumnStats} is table-wide and stays true as
     * soon as any one field is enabled, so it cannot be that gate — these are the cases where the two differ.
     */
    @Test
    public void nanCountFieldIdsFollowThePerFieldMetricsPolicy() {
        Schema floatSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "d", Types.DoubleType.get()),
                Types.NestedField.optional(3, "f", Types.FloatType.get()));

        // Default policy: every float field is eligible.
        Table dflt = tableWith(floatSchema, "write.format.default", "parquet");
        Assertions.assertEquals(Arrays.asList(2, 3),
                IcebergWriterHelper.nanCountFieldIds(dflt, floatSchema));

        // default=none with one NON-float field re-enabled: the table-wide flag is still true, but no float
        // field survives the policy, so BE must be told to count nothing.
        Table mixed = tableWith(floatSchema, "write.format.default", "parquet",
                "write.metadata.metrics.default", "none",
                "write.metadata.metrics.column.id", "full");
        Assertions.assertTrue(IcebergWriterHelper.shouldCollectColumnStats(mixed, floatSchema),
                "one enabled field keeps table-wide collection on -- which is exactly why it cannot gate the scan");
        Assertions.assertTrue(IcebergWriterHelper.nanCountFieldIds(mixed, floatSchema).isEmpty());

        // A single float field disabled while the rest stay default.
        Table oneOff = tableWith(floatSchema, "write.format.default", "parquet",
                "write.metadata.metrics.column.d", "none");
        Assertions.assertEquals(Collections.singletonList(3),
                IcebergWriterHelper.nanCountFieldIds(oneOff, floatSchema));
    }

    /**
     * Iceberg gives the default metrics mode only to the first
     * {@code write.metadata.metrics.max-inferred-column-defaults} fields and {@code none} to everything after,
     * so a wide table disables most of its columns with no property set at all. That is the case that makes
     * this list worth threading rather than a nice-to-have.
     */
    @Test
    public void nanCountFieldIdsHonorTheInferredColumnCapOnAWideTable() {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int id = 1; id <= 6; id++) {
            fields.add(Types.NestedField.optional(id, "d" + id, Types.DoubleType.get()));
        }
        Schema wide = new Schema(fields);
        // Cap the inferred defaults at 4 rather than building a 100-column fixture; the mechanism is the same.
        Table table = tableWith(wide, "write.format.default", "parquet",
                "write.metadata.metrics.max-inferred-column-defaults", "4");

        Assertions.assertTrue(IcebergWriterHelper.shouldCollectColumnStats(table, wide));
        Assertions.assertEquals(Arrays.asList(1, 2, 3, 4),
                IcebergWriterHelper.nanCountFieldIds(table, wide),
                "fields past the inferred-column cap default to metrics=none and must not be scanned");
    }

    // ──────────── convertToWriterResult: #65782 honor iceberg metrics policy (ported from fe-core) ────────────

    @Test
    public void convertToWriterResultRespectsNoneMetricsMode() {
        Table table = tableWith("write.format.default", "parquet", "write.metadata.metrics.default", "none");
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setColumnSizes(Map.of(2, 128L));
        stats.setValueCounts(Map.of(2, 10L));
        stats.setNullValueCounts(Map.of(2, 0L));
        stats.setLowerBounds(Map.of(2, ByteBuffer.wrap(new byte[] {0x01})));
        stats.setUpperBounds(Map.of(2, ByteBuffer.wrap(new byte[] {0x02})));

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.parquet");

        Assertions.assertTrue(df.columnSizes() == null || df.columnSizes().isEmpty());
        Assertions.assertTrue(df.valueCounts() == null || df.valueCounts().isEmpty());
        Assertions.assertTrue(df.nullValueCounts() == null || df.nullValueCounts().isEmpty());
        Assertions.assertTrue(df.lowerBounds() == null || df.lowerBounds().isEmpty());
        Assertions.assertTrue(df.upperBounds() == null || df.upperBounds().isEmpty());
    }

    @Test
    public void convertToWriterResultCountsModeOmitsBounds() {
        Table table = tableWith("write.format.default", "parquet", "write.metadata.metrics.default", "counts");
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setColumnSizes(Map.of(2, 128L));
        stats.setValueCounts(Map.of(2, 10L));
        stats.setNullValueCounts(Map.of(2, 0L));
        stats.setLowerBounds(Map.of(2, Conversions.toByteBuffer(Types.StringType.get(), "abcdefgh")));
        stats.setUpperBounds(Map.of(2, Conversions.toByteBuffer(Types.StringType.get(), "ijklmnop")));

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.parquet");

        Assertions.assertEquals(Long.valueOf(128L), df.columnSizes().get(2));
        Assertions.assertEquals(Long.valueOf(10L), df.valueCounts().get(2));
        Assertions.assertEquals(Long.valueOf(0L), df.nullValueCounts().get(2));
        Assertions.assertTrue(df.lowerBounds() == null || df.lowerBounds().isEmpty());
        Assertions.assertTrue(df.upperBounds() == null || df.upperBounds().isEmpty());
    }

    @Test
    public void convertToWriterResultTruncatesStringAndBinaryBounds() {
        Schema boundsSchema = new Schema(
                Types.NestedField.optional(1, "text", Types.StringType.get()),
                Types.NestedField.optional(2, "payload", Types.BinaryType.get()));
        Table table = tableWith(boundsSchema, "write.format.default", "parquet",
                "write.metadata.metrics.default", "truncate(3)");
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setLowerBounds(Map.of(
                1, Conversions.toByteBuffer(Types.StringType.get(), "abcdef"),
                2, ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));
        stats.setUpperBounds(Map.of(
                1, Conversions.toByteBuffer(Types.StringType.get(), "uvwxyz"),
                2, ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.parquet");

        Assertions.assertEquals("abc",
                Conversions.fromByteBuffer(Types.StringType.get(), df.lowerBounds().get(1)).toString());
        Assertions.assertEquals("uvx",
                Conversions.fromByteBuffer(Types.StringType.get(), df.upperBounds().get(1)).toString());
        Assertions.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), df.lowerBounds().get(2));
        Assertions.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 4}), df.upperBounds().get(2));
    }

    @Test
    public void convertToWriterResultPreservesOrcUpperBoundWithoutTruncatedSuccessor() {
        Schema boundsSchema = new Schema(Types.NestedField.optional(1, "text", Types.StringType.get()));
        Table table = tableWith(boundsSchema, "write.format.default", "orc",
                "write.metadata.metrics.default", "truncate(1)");
        String maxWithoutSuccessor = new String(Character.toChars(Character.MAX_CODE_POINT)) + "tail";
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setUpperBounds(Map.of(1, Conversions.toByteBuffer(Types.StringType.get(), maxWithoutSuccessor)));

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/f.orc");

        Assertions.assertNotNull(df.upperBounds());
        Assertions.assertEquals(maxWithoutSuccessor,
                Conversions.fromByteBuffer(Types.StringType.get(), df.upperBounds().get(1)).toString());
    }

    @Test
    public void convertToWriterResultHandlesV3TransactionTableLineageMetrics() {
        // A commit runs against transaction.table(), which is a HasTableOperations but NOT a BaseTable.
        // getFormatVersion must read the real v3 through operations (the reserved format-version property is
        // stripped at creation), else the row-lineage columns are not appended and their metric field ids fail
        // schema resolution — this test is the regression guard for the BaseTable -> HasTableOperations change.
        Table base = tableWith("format-version", "3", "write.format.default", "parquet",
                "write.metadata.metrics.default", "truncate(16)");
        Table transactionTable = base.newTransaction().table();

        int rowId = MetadataColumns.ROW_ID.fieldId();
        int seqId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
        ByteBuffer rowIdBound = Conversions.toByteBuffer(MetadataColumns.ROW_ID.type(), 7L);
        ByteBuffer seqBound = Conversions.toByteBuffer(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.type(), 3L);
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setLowerBounds(Map.of(rowId, rowIdBound, seqId, seqBound));
        stats.setUpperBounds(Map.of(rowId, rowIdBound, seqId, seqBound));

        DataFile df = writeSingle(transactionTable, stats, "s3://b/db1/t/v3.parquet");

        Assertions.assertEquals(rowIdBound, df.lowerBounds().get(rowId));
        Assertions.assertEquals(rowIdBound, df.upperBounds().get(rowId));
        Assertions.assertEquals(seqBound, df.lowerBounds().get(seqId));
        Assertions.assertEquals(seqBound, df.upperBounds().get(seqId));
    }

    @Test
    public void convertToWriterResultSuppressesLogicalMetricsBelowRepeatedFields() {
        Schema repeatedSchema = new Schema(
                Types.NestedField.optional(1, "items", Types.ListType.ofOptional(2, Types.IntegerType.get())),
                Types.NestedField.optional(3, "attributes",
                        Types.MapType.ofOptional(4, 5, Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.optional(6, "top_level", Types.IntegerType.get()));
        Table table = tableWith(repeatedSchema, "write.format.default", "parquet",
                "write.metadata.metrics.default", "full");
        // InMemoryCatalog.createTable reassigns fresh field ids, so resolve the real ids from the stored schema
        // (the input ids 2/4/5/6 no longer map to the same fields). The BE keys its stats by these same ids.
        Schema stored = table.schema();
        int elementId = ((Types.ListType) stored.findField("items").type()).elementId();
        int keyId = ((Types.MapType) stored.findField("attributes").type()).keyId();
        int valueId = ((Types.MapType) stored.findField("attributes").type()).valueId();
        int topId = stored.findField("top_level").fieldId();
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setColumnSizes(Map.of(elementId, 20L, keyId, 40L, valueId, 50L, topId, 60L));
        stats.setValueCounts(Map.of(elementId, 2L, keyId, 4L, valueId, 5L, topId, 6L));
        stats.setNullValueCounts(Map.of(elementId, 0L, keyId, 0L, valueId, 0L, topId, 0L));
        stats.setLowerBounds(Map.of(
                elementId, Conversions.toByteBuffer(Types.IntegerType.get(), 2),
                keyId, Conversions.toByteBuffer(Types.StringType.get(), "key"),
                valueId, Conversions.toByteBuffer(Types.StringType.get(), "value"),
                topId, Conversions.toByteBuffer(Types.IntegerType.get(), 6)));
        stats.setUpperBounds(stats.getLowerBounds());

        DataFile df = writeSingle(table, stats, "s3://b/db1/t/repeated.parquet");

        // columnSizes ignore the repeated-field rule; only the logical count/bound metrics below list/map drop.
        Assertions.assertEquals(Map.of(elementId, 20L, keyId, 40L, valueId, 50L, topId, 60L), df.columnSizes());
        Assertions.assertEquals(Map.of(topId, 6L), df.valueCounts());
        Assertions.assertEquals(Map.of(topId, 0L), df.nullValueCounts());
        Assertions.assertEquals(Map.of(topId, stats.getLowerBounds().get(topId)), df.lowerBounds());
        Assertions.assertEquals(Map.of(topId, stats.getUpperBounds().get(topId)), df.upperBounds());
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

    @Test
    public void writeFormatStillInfersOrcFromExistingFiles() {
        Table table = tableWith();
        table.newAppend().appendFile(DataFiles.builder(unpartitionedSpec)
                .withPath("s3://b/db1/t/f0.orc").withFileSizeInBytes(100)
                .withRecordCount(1).withFormat(FileFormat.ORC).build()).commit();
        Assertions.assertEquals(FileFormat.ORC, IcebergWriterHelper.getFileFormat(table));
    }
}
