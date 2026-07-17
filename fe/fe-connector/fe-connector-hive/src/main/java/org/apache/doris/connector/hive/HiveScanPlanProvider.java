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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan plan provider for Hive tables.
 *
 * <p>Implements the core scan planning pipeline:</p>
 * <ol>
 *   <li>Resolve partitions from HMS (using pruned list if available)</li>
 *   <li>List files in each partition directory via Hadoop FileSystem</li>
 *   <li>Split large files based on target split size</li>
 *   <li>Return {@link HiveScanRange} for each file split</li>
 * </ol>
 *
 * <p>Current limitations (Phase 2 initial):</p>
 * <ul>
 *   <li>No file listing cache (lists files directly each time)</li>
 *   <li>No ACID transaction support (non-transactional tables only)</li>
 *   <li>No batch/lazy split mode</li>
 *   <li>No table sampling</li>
 * </ul>
 */
public class HiveScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(HiveScanPlanProvider.class);

    /** Default target split size: 256 MB. */
    private static final long DEFAULT_TARGET_SPLIT_SIZE = 256 * 1024 * 1024L;

    /** Maximum number of partitions to list from HMS. */
    private static final int MAX_PARTITIONS = 100000;

    /** Scan node property keys. */
    public static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    public static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    public static final String PROP_LOCATION_PREFIX = "location.";

    private final HmsClient hmsClient;
    private final Map<String, String> catalogProperties;

    public HiveScanPlanProvider(HmsClient hmsClient, Map<String, String> catalogProperties) {
        this.hmsClient = hmsClient;
        this.catalogProperties = catalogProperties;
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        List<PartitionScanInfo> partitions = resolvePartitions(hiveHandle);
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib());
        long targetSplitSize = getTargetSplitSize(session);
        boolean splittable = fileFormat.isSplittable();

        List<ConnectorScanRange> ranges = new ArrayList<>();
        Configuration hadoopConf = buildHadoopConf();

        for (PartitionScanInfo partition : partitions) {
            HiveFileFormat partFormat = partition.fileFormat != null
                    ? partition.fileFormat : fileFormat;
            try {
                listAndSplitFiles(hadoopConf, partition, partFormat,
                        splittable, targetSplitSize, ranges);
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to list files for partition: " + partition.location, e);
            }
        }

        LOG.info("Hive scan plan: table={}.{}, partitions={}, splits={}",
                dbName, tableName, partitions.size(), ranges.size());
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        Map<String, String> props = new HashMap<>();

        // File format type
        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib());
        props.put(PROP_FILE_FORMAT_TYPE, fileFormat.getFormatName());

        // Partition key column names
        List<String> partKeys = hiveHandle.getPartitionKeyNames();
        if (partKeys != null && !partKeys.isEmpty()) {
            props.put(PROP_PATH_PARTITION_KEYS, String.join(",", partKeys));
        }

        // Location properties (Hadoop/S3 config for BE file access)
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            String key = entry.getKey();
            if (isLocationProperty(key)) {
                props.put(PROP_LOCATION_PREFIX + key, entry.getValue());
            }
        }

        // Text format properties (if applicable)
        if (fileFormat == HiveFileFormat.TEXT || fileFormat == HiveFileFormat.JSON) {
            Map<String, String> textProps = HiveTextProperties.extract(
                    hiveHandle.getSerializationLib(),
                    hiveHandle.getSdParameters(),
                    hiveHandle.getTableParameters());
            props.putAll(textProps);
        }

        return props;
    }

    /**
     * Resolves the partitions to scan, using pruned partitions from the handle
     * if available, or listing all partitions from HMS.
     */
    private List<PartitionScanInfo> resolvePartitions(HiveTableHandle handle) {
        List<String> partKeyNames = handle.getPartitionKeyNames();

        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Unpartitioned table: single partition using table location
            return Collections.singletonList(new PartitionScanInfo(
                    handle.getLocation(), Collections.emptyMap(), null));
        }

        // Check for pruned partitions in handle (set by applyFilter)
        List<HmsPartitionInfo> prunedPartitions = handle.getPrunedPartitions();
        if (prunedPartitions != null) {
            return convertPartitions(prunedPartitions, partKeyNames);
        }

        // No pruning: list all partitions from HMS
        List<String> partNames = hmsClient.listPartitionNames(
                handle.getDbName(), handle.getTableName(), MAX_PARTITIONS);
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<HmsPartitionInfo> hmsPartitions = hmsClient.getPartitions(
                handle.getDbName(), handle.getTableName(), partNames);
        return convertPartitions(hmsPartitions, partKeyNames);
    }

    private List<PartitionScanInfo> convertPartitions(
            List<HmsPartitionInfo> hmsPartitions, List<String> partKeyNames) {
        List<PartitionScanInfo> result = new ArrayList<>(hmsPartitions.size());
        for (HmsPartitionInfo part : hmsPartitions) {
            Map<String, String> partValues = new LinkedHashMap<>();
            List<String> values = part.getValues();
            for (int i = 0; i < partKeyNames.size() && i < values.size(); i++) {
                partValues.put(partKeyNames.get(i), values.get(i));
            }
            HiveFileFormat partFormat = HiveFileFormat.detect(
                    part.getInputFormat(), part.getSerializationLib());
            result.add(new PartitionScanInfo(
                    part.getLocation(), partValues, partFormat));
        }
        return result;
    }

    /**
     * Lists files in a partition directory and splits them into scan ranges.
     */
    private void listAndSplitFiles(Configuration conf,
            PartitionScanInfo partition, HiveFileFormat fileFormat,
            boolean splittable, long targetSplitSize,
            List<ConnectorScanRange> ranges) throws IOException {
        Path partPath = new Path(partition.location);
        FileSystem fs = FileSystem.get(partPath.toUri(), conf);
        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(partPath);
        } catch (IOException e) {
            LOG.warn("Cannot list files in partition: {}", partition.location, e);
            return;
        }

        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                // Skip directories (could be _temporary, etc.)
                continue;
            }
            String fileName = status.getPath().getName();
            if (shouldSkipFile(fileName)) {
                continue;
            }
            splitFile(status, partition, fileFormat, splittable,
                    targetSplitSize, ranges);
        }
    }

    /**
     * Splits a file into scan ranges based on target split size.
     */
    private void splitFile(FileStatus fileStatus, PartitionScanInfo partition,
            HiveFileFormat fileFormat, boolean splittable,
            long targetSplitSize, List<ConnectorScanRange> ranges) {
        long fileSize = fileStatus.getLen();
        String filePath = fileStatus.getPath().toString();
        long modTime = fileStatus.getModificationTime();

        if (fileSize == 0) {
            return;
        }

        if (!splittable || targetSplitSize <= 0 || fileSize <= targetSplitSize) {
            // Single range for the whole file
            ranges.add(HiveScanRange.builder()
                    .path(filePath)
                    .start(0)
                    .length(fileSize)
                    .fileSize(fileSize)
                    .modificationTime(modTime)
                    .fileFormat(fileFormat.getFormatName())
                    .tableFormatType("hive")
                    .partitionValues(partition.partitionValues)
                    .build());
            return;
        }

        // Split file into ranges
        long offset = 0;
        while (offset < fileSize) {
            long splitSize = Math.min(targetSplitSize, fileSize - offset);
            ranges.add(HiveScanRange.builder()
                    .path(filePath)
                    .start(offset)
                    .length(splitSize)
                    .fileSize(fileSize)
                    .modificationTime(modTime)
                    .fileFormat(fileFormat.getFormatName())
                    .tableFormatType("hive")
                    .partitionValues(partition.partitionValues)
                    .build());
            offset += splitSize;
        }
    }

    private boolean shouldSkipFile(String fileName) {
        return fileName.startsWith("_") || fileName.startsWith(".");
    }

    private long getTargetSplitSize(ConnectorSession session) {
        String splitSizeStr = session.getProperty(
                "file_split_size", String.class);
        if (splitSizeStr != null && !splitSizeStr.isEmpty()) {
            try {
                long val = Long.parseLong(splitSizeStr);
                if (val > 0) {
                    return val;
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return DEFAULT_TARGET_SPLIT_SIZE;
    }

    private Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        // Set default FS from location properties if present
        String defaultFs = catalogProperties.get("fs.defaultFS");
        if (defaultFs == null) {
            defaultFs = catalogProperties.get("hadoop.fs.defaultFS");
        }
        if (defaultFs != null) {
            conf.set("fs.defaultFS", defaultFs);
        }
        return conf;
    }

    private boolean isLocationProperty(String key) {
        return key.startsWith("fs.")
                || key.startsWith("hadoop.")
                || key.startsWith("dfs.")
                || key.startsWith("s3.")
                || key.startsWith("s3a.")
                || key.startsWith("cos.")
                || key.startsWith("obs.")
                || key.startsWith("oss.")
                || key.equals("uri");
    }

    /**
     * Internal representation of a partition to scan.
     */
    private static final class PartitionScanInfo {
        final String location;
        final Map<String, String> partitionValues;
        final HiveFileFormat fileFormat;

        PartitionScanInfo(String location, Map<String, String> partitionValues,
                HiveFileFormat fileFormat) {
            this.location = location;
            this.partitionValues = partitionValues;
            this.fileFormat = fileFormat;
        }
    }
}
