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

import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.statistics.CommonStatistics;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.VerifyException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergWriterHelper {
    private static final Logger LOG = LogManager.getLogger(IcebergWriterHelper.class);

    private static final int DEFAULT_FILE_COUNT = 1;

    public static WriteResult convertToWriterResult(
            Table table,
            List<TIcebergCommitData> commitDataList) {
        List<DataFile> dataFiles = new ArrayList<>();

        // Get table specification information
        PartitionSpec spec = table.spec();
        FileFormat fileFormat = IcebergUtils.getFileFormat(table);
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        Schema schema = table.schema();

        for (TIcebergCommitData commitData : commitDataList) {
            //get the files path
            String location = commitData.getFilePath();

            //get the commit file statistics
            long fileSize = commitData.getFileSize();
            long recordCount = commitData.getRowCount();
            CommonStatistics stat = new CommonStatistics(recordCount, DEFAULT_FILE_COUNT, fileSize);
            Metrics metrics = buildDataFileMetrics(commitData, schema, metricsConfig);
            Optional<PartitionData> partitionData = Optional.empty();
            //get and check partitionValues when table is partitionedTable
            if (spec.isPartitioned()) {
                List<String> partitionValues = commitData.getPartitionValues();
                if (Objects.isNull(partitionValues) || partitionValues.isEmpty()) {
                    throw new VerifyException("No partition data for partitioned table");
                }
                partitionValues = partitionValues.stream().map(s -> s.equals("null") ? null : s)
                        .collect(Collectors.toList());

                // Convert human-readable partition values to PartitionData
                partitionData = Optional.of(convertToPartitionData(partitionValues, spec));
            }
            DataFile dataFile = genDataFile(fileFormat, location, spec, partitionData, stat, metrics,
                    table.sortOrder());
            dataFiles.add(dataFile);
        }
        return WriteResult.builder()
                .addDataFiles(dataFiles)
                .build();

    }

    public static DataFile genDataFile(
            FileFormat format,
            String location,
            PartitionSpec spec,
            Optional<PartitionData> partitionData,
            CommonStatistics statistics, Metrics metrics, SortOrder sortOrder) {

        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(location)
                .withFileSizeInBytes(statistics.getTotalFileBytes())
                .withRecordCount(statistics.getRowCount())
                .withMetrics(metrics)
                .withSortOrder(sortOrder)
                .withFormat(format);

        partitionData.ifPresent(builder::withPartition);

        return builder.build();
    }

    /**
     * Convert human-readable partition values (from Backend) to PartitionData.
     *
     * Backend sends partition values as human-readable strings:
     * - DATE: "2025-01-25"
     * - DATETIME: "2025-01-25 10:00:00"
     */
    private static PartitionData convertToPartitionData(
            List<String> humanReadableValues, PartitionSpec spec) {
        // Create PartitionData instance using the partition type from spec
        PartitionData partitionData = new PartitionData(spec.partitionType());

        // Get partition type fields to determine the result type of each partition field
        Types.StructType partitionType = spec.partitionType();
        List<Types.NestedField> partitionTypeFields = partitionType.fields();

        for (int i = 0; i < humanReadableValues.size(); i++) {
            String humanReadableValue = humanReadableValues.get(i);

            if (humanReadableValue == null) {
                partitionData.set(i, null);
                continue;
            }

            // Get the partition field's result type
            Types.NestedField partitionTypeField = partitionTypeFields.get(i);
            org.apache.iceberg.types.Type partitionFieldType = partitionTypeField.type();

            // Convert the human-readable value to internal format object
            Object internalValue = IcebergUtils.parsePartitionValueFromString(
                    humanReadableValue, partitionFieldType);

            // Set the value in PartitionData
            partitionData.set(i, internalValue);
        }

        return partitionData;
    }

    private static Metrics buildDataFileMetrics(
            TIcebergCommitData commitData, Schema schema, MetricsConfig metricsConfig) {
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

        // Physical file stats may contain every column, but manifest metrics must honor the table's metadata policy.
        return new Metrics(commitData.getRowCount(),
                filterDisabledMetrics(columnSizes, schema, metricsConfig),
                filterDisabledMetrics(valueCounts, schema, metricsConfig),
                filterDisabledMetrics(nullValueCounts, schema, metricsConfig),
                null,
                filterDisabledMetrics(lowerBounds, schema, metricsConfig),
                filterDisabledMetrics(upperBounds, schema, metricsConfig));
    }

    private static <T> Map<Integer, T> filterDisabledMetrics(
            Map<Integer, T> metrics, Schema schema, MetricsConfig metricsConfig) {
        Map<Integer, T> filteredMetrics = new HashMap<>();
        metrics.forEach((fieldId, value) -> {
            if (MetricsUtil.metricsMode(schema, metricsConfig, fieldId) != MetricsModes.None.get()) {
                filteredMetrics.put(fieldId, value);
            }
        });
        return filteredMetrics;
    }

    /**
     * Convert TIcebergCommitData list to DeleteFile list for delete operations.
     *
     * @param format File format (Parquet/ORC)
     * @param spec Partition specification
     * @param commitDataList List of commit data from BE
     * @return List of DeleteFile objects ready to be committed
     */
    public static List<DeleteFile> convertToDeleteFiles(
            FileFormat format,
            PartitionSpec spec,
            List<TIcebergCommitData> commitDataList) {
        List<DeleteFile> deleteFiles = new ArrayList<>();

        for (TIcebergCommitData commitData : commitDataList) {
            // Only process delete files
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

            // Build delete file metadata
            FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(spec)
                    .withPath(deleteFilePath)
                    .withFormat(effectiveFormat)
                    .withFileSizeInBytes(fileSize)
                    .withRecordCount(recordCount);

            // Set delete file content type
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

            // Add partition information if table is partitioned
            if (spec.isPartitioned()) {
                PartitionData partitionData;
                if (commitData.getPartitionValues() != null && !commitData.getPartitionValues().isEmpty()) {
                    // Convert partition values to PartitionData
                    List<String> partitionValues = commitData.getPartitionValues().stream()
                            .map(s -> s.equals("null") ? null : s)
                            .collect(Collectors.toList());
                    partitionData = convertToPartitionData(partitionValues, spec);
                } else if (commitData.getPartitionDataJson() != null && !commitData.getPartitionDataJson().isEmpty()) {
                    List<String> partitionValues = IcebergUtils.parsePartitionValuesFromJson(
                            commitData.getPartitionDataJson());
                    if (!partitionValues.isEmpty()) {
                        partitionData = convertToPartitionData(partitionValues, spec);
                    } else {
                        partitionData = new PartitionData(spec.partitionType());
                    }
                } else {
                    throw new VerifyException("No partition data for partitioned table");
                }
                deleteBuilder.withPartition(partitionData);
            }

            DeleteFile deleteFile = deleteBuilder.build();
            deleteFiles.add(deleteFile);
        }

        return deleteFiles;
    }
}
