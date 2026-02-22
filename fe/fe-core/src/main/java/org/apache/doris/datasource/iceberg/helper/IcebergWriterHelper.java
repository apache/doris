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
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.base.VerifyException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
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

        for (TIcebergCommitData commitData : commitDataList) {
            //get the files path
            String location = commitData.getFilePath();

            //get the commit file statistics
            long fileSize = commitData.getFileSize();
            long recordCount = commitData.getRowCount();
            CommonStatistics stat = new CommonStatistics(recordCount, DEFAULT_FILE_COUNT, fileSize);
            Metrics metrics = buildDataFileMetrics(table, fileFormat, commitData);
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

    private static Metrics buildDataFileMetrics(Table table, FileFormat fileFormat, TIcebergCommitData commitData) {
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
}
