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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Self-contained port of the legacy fe-core {@code IcebergWriterHelper} (P6.3-T04). The connector cannot
 * import fe-core, so the conversion from BE commit fragments ({@link TIcebergCommitData}) to iceberg
 * {@link DataFile}/{@link DeleteFile}/{@link Metrics}/{@link PartitionData} is reproduced byte-faithfully
 * against the iceberg SDK.
 *
 * <p>Deliberate, documented deltas vs legacy: {@code CommonStatistics} is inlined (the row count + file size
 * are passed straight to {@link #genDataFile}, DV-T04-e), the partition-value time zone is a resolved
 * {@link ZoneId} argument threaded from {@code IcebergConnectorTransaction.beginWrite} (legacy reads a
 * thread-local, DV-T04-f), and the partition-data JSON is parsed via iceberg's bundled Jackson
 * ({@link IcebergPartitionUtils#parsePartitionValuesFromJson}, DV-T04-d).</p>
 */
final class IcebergWriterHelper {

    private static final Logger LOG = LogManager.getLogger(IcebergWriterHelper.class);

    private static final String WRITE_FORMAT = "write-format";
    private static final String PARQUET_NAME = "parquet";
    private static final String ORC_NAME = "orc";

    private IcebergWriterHelper() {
    }

    /**
     * Converts the BE data-file commit fragments into an iceberg {@link WriteResult} of {@link DataFile}s
     * (the INSERT / OVERWRITE / MERGE-data path). A partitioned table requires non-empty partition values per
     * file; {@code "null"} partition tokens map to a real {@code null}.
     */
    static WriteResult convertToWriterResult(Table table, List<TIcebergCommitData> commitDataList, ZoneId zone) {
        List<DataFile> dataFiles = new ArrayList<>();

        PartitionSpec spec = table.spec();
        FileFormat fileFormat = getFileFormat(table);

        for (TIcebergCommitData commitData : commitDataList) {
            String location = commitData.getFilePath();
            long fileSize = commitData.getFileSize();
            long recordCount = commitData.getRowCount();
            Metrics metrics = buildDataFileMetrics(fileFormat, commitData);
            Optional<PartitionData> partitionData = Optional.empty();
            if (spec.isPartitioned()) {
                List<String> partitionValues = commitData.getPartitionValues();
                if (Objects.isNull(partitionValues) || partitionValues.isEmpty()) {
                    throw new VerifyException("No partition data for partitioned table");
                }
                partitionValues = partitionValues.stream().map(s -> s.equals("null") ? null : s)
                        .collect(Collectors.toList());
                partitionData = Optional.of(convertToPartitionData(partitionValues, spec, zone));
            }
            DataFile dataFile = genDataFile(fileFormat, location, spec, partitionData, recordCount, fileSize,
                    metrics, table.sortOrder());
            dataFiles.add(dataFile);
        }
        return WriteResult.builder()
                .addDataFiles(dataFiles)
                .build();
    }

    private static DataFile genDataFile(FileFormat format, String location, PartitionSpec spec,
            Optional<PartitionData> partitionData, long recordCount, long fileSize, Metrics metrics,
            SortOrder sortOrder) {
        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(location)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .withMetrics(metrics)
                .withSortOrder(sortOrder)
                .withFormat(format);
        partitionData.ifPresent(builder::withPartition);
        return builder.build();
    }

    /**
     * Convert human-readable partition values (from BE) to {@link PartitionData}: DATE strings like
     * {@code "2025-01-25"} and DATETIME strings like {@code "2025-01-25 10:00:00"} become the iceberg internal
     * partition objects.
     */
    private static PartitionData convertToPartitionData(List<String> humanReadableValues, PartitionSpec spec,
            ZoneId zone) {
        PartitionData partitionData = new PartitionData(spec.partitionType());
        Types.StructType partitionType = spec.partitionType();
        List<Types.NestedField> partitionTypeFields = partitionType.fields();

        for (int i = 0; i < humanReadableValues.size(); i++) {
            String humanReadableValue = humanReadableValues.get(i);
            if (humanReadableValue == null) {
                partitionData.set(i, null);
                continue;
            }
            Type partitionFieldType = partitionTypeFields.get(i).type();
            Object internalValue = IcebergPartitionUtils.parsePartitionValueFromString(
                    humanReadableValue, partitionFieldType, zone);
            partitionData.set(i, internalValue);
        }
        return partitionData;
    }

    private static Metrics buildDataFileMetrics(FileFormat fileFormat, TIcebergCommitData commitData) {
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        if (commitData.isSetColumnStats()) {
            TIcebergColumnStats stats = commitData.column_stats;
            if (stats.isSetColumnSizes()) {
                columnSizes = stats.column_sizes;
            }
            if (stats.isSetValueCounts()) {
                valueCounts = stats.value_counts;
            }
            if (stats.isSetNullValueCounts()) {
                nullValueCounts = stats.null_value_counts;
            }
            if (stats.isSetLowerBounds()) {
                lowerBounds = stats.lower_bounds;
            }
            if (stats.isSetUpperBounds()) {
                upperBounds = stats.upper_bounds;
            }
        }
        return new Metrics(commitData.getRowCount(), columnSizes, valueCounts,
                nullValueCounts, null, lowerBounds, upperBounds);
    }

    /**
     * Convert the BE delete-file commit fragments to iceberg {@link DeleteFile}s for the DELETE / MERGE path.
     * Position deletes and deletion vectors (rendered as a {@link FileFormat#PUFFIN} position-delete with a
     * content offset/size) are supported; equality deletes are rejected (Doris MOR writes position deletes).
     */
    static List<DeleteFile> convertToDeleteFiles(FileFormat format, PartitionSpec spec,
            List<TIcebergCommitData> commitDataList, ZoneId zone) {
        List<DeleteFile> deleteFiles = new ArrayList<>();

        for (TIcebergCommitData commitData : commitDataList) {
            if (commitData.getFileContent() == null
                    || commitData.getFileContent() == TFileContent.DATA) {
                continue;
            }

            String deleteFilePath = commitData.getFilePath();
            long fileSize = commitData.getFileSize();
            long recordCount = commitData.getRowCount();
            boolean isDeletionVector = commitData.isSetContentOffset()
                    && commitData.isSetContentSizeInBytes();
            FileFormat effectiveFormat = isDeletionVector ? FileFormat.PUFFIN : format;

            FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(spec)
                    .withPath(deleteFilePath)
                    .withFormat(effectiveFormat)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount);

            if (commitData.getFileContent() == TFileContent.POSITION_DELETES) {
                deleteBuilder.ofPositionDeletes();
            } else if (commitData.getFileContent() == TFileContent.DELETION_VECTOR) {
                deleteBuilder.ofPositionDeletes();
            } else {
                throw new VerifyException("Iceberg delete only supports position deletes, but got "
                        + commitData.getFileContent());
            }

            if (isDeletionVector) {
                deleteBuilder.withContentOffset(commitData.getContentOffset());
                deleteBuilder.withContentSizeInBytes(commitData.getContentSizeInBytes());
            }

            if (commitData.isSetReferencedDataFilePath()
                    && commitData.getReferencedDataFilePath() != null
                    && !commitData.getReferencedDataFilePath().isEmpty()) {
                deleteBuilder.withReferencedDataFile(commitData.getReferencedDataFilePath());
            }

            if (spec.isPartitioned()) {
                PartitionData partitionData;
                if (commitData.getPartitionValues() != null && !commitData.getPartitionValues().isEmpty()) {
                    List<String> partitionValues = commitData.getPartitionValues().stream()
                            .map(s -> s.equals("null") ? null : s)
                            .collect(Collectors.toList());
                    partitionData = convertToPartitionData(partitionValues, spec, zone);
                } else if (commitData.getPartitionDataJson() != null
                        && !commitData.getPartitionDataJson().isEmpty()) {
                    List<String> partitionValues = IcebergPartitionUtils.parsePartitionValuesFromJson(
                            commitData.getPartitionDataJson());
                    if (!partitionValues.isEmpty()) {
                        partitionData = convertToPartitionData(partitionValues, spec, zone);
                    } else {
                        partitionData = new PartitionData(spec.partitionType());
                    }
                } else {
                    throw new VerifyException("No partition data for partitioned table");
                }
                deleteBuilder.withPartition(partitionData);
            }

            deleteFiles.add(deleteBuilder.build());
        }

        return deleteFiles;
    }

    /**
     * Resolve the table's write file format (port of legacy {@code IcebergUtils.getFileFormat}): the
     * {@code write-format} nickname, then the standard {@code write.format.default} property, then an inference
     * from the current snapshot's data files (defaulting to parquet). Throws on a non-orc/parquet format.
     */
    static FileFormat getFileFormat(Table table) {
        Map<String, String> properties = table.properties();
        String fileFormatName = resolveFileFormatName(table, properties);
        if (fileFormatName.toLowerCase().contains(ORC_NAME)) {
            return FileFormat.ORC;
        } else if (fileFormatName.toLowerCase().contains(PARQUET_NAME)) {
            return FileFormat.PARQUET;
        } else {
            throw new RuntimeException("Unsupported input format type: " + fileFormatName);
        }
    }

    private static String resolveFileFormatName(Table table, Map<String, String> properties) {
        if (properties.containsKey(WRITE_FORMAT)) {
            return properties.get(WRITE_FORMAT);
        }
        if (properties.containsKey(TableProperties.DEFAULT_FILE_FORMAT)) {
            return properties.get(TableProperties.DEFAULT_FILE_FORMAT);
        }
        return inferFileFormatFromDataFiles(table);
    }

    private static String inferFileFormatFromDataFiles(Table table) {
        if (table.currentSnapshot() == null) {
            return PARQUET_NAME;
        }
        try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
            Iterator<FileScanTask> it = files.iterator();
            if (it.hasNext()) {
                return it.next().file().format().name().toLowerCase();
            }
        } catch (Exception e) {
            LOG.warn("Failed to infer file format from data files for table {}, defaulting to {}",
                    table.name(), PARQUET_NAME, e);
        }
        return PARQUET_NAME;
    }

    /**
     * Reads the real table format version (port of legacy {@code IcebergUtils.getFormatVersion}): from a
     * {@link BaseTable}'s current metadata when available, else from the {@code format-version} table
     * property, defaulting to 2. Kept here (the shared write-side helper) so the sink dialects share one
     * implementation; the per-class private copies in {@code IcebergConnectorMetadata}/{@code
     * IcebergConnectorTransaction} are left untouched (DV-T05-e).
     */
    static int getFormatVersion(Table table) {
        int formatVersion = 2;
        if (table instanceof BaseTable) {
            formatVersion = ((BaseTable) table).operations().current().formatVersion();
        } else if (table != null && table.properties() != null) {
            String version = table.properties().get(TableProperties.FORMAT_VERSION);
            if (version != null) {
                try {
                    formatVersion = Integer.parseInt(version);
                } catch (NumberFormatException ignored) {
                    // keep the default
                }
            }
        }
        return formatVersion;
    }

    /**
     * Appends the format-version 3 row-lineage fields ({@code _row_id}, {@code _last_updated_sequence_number})
     * to the schema (port of legacy {@code IcebergUtils.appendRowLineageFieldsForV3}); pure iceberg SDK. The
     * merge sink's BE writer expects the row-lineage columns in the schema-json for a v3 table.
     */
    static Schema appendRowLineageFieldsForV3(Schema schema) {
        return TypeUtil.join(schema, new Schema(
                MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER));
    }
}
