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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Scan plan provider for Hudi tables.
 *
 * <p>Implements the core scan planning pipeline:
 * <ol>
 *   <li>Build {@link HoodieTableMetaClient} from the table's base path</li>
 *   <li>Resolve the query instant from the completed timeline</li>
 *   <li>Resolve partitions (pruned via applyFilter or all partitions)</li>
 *   <li>For each partition:
 *     <ul>
 *       <li>COW: list latest base files → native reader splits (Parquet/ORC)</li>
 *       <li>MOR: list latest merged file slices → JNI splits (or native if no logs)</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>Scope: Snapshot reads of non-incremental tables.
 * Incremental reads, schema evolution, and time travel are deferred.</p>
 */
public class HudiScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(HudiScanPlanProvider.class);

    private final Map<String, String> properties;

    public HudiScanPlanProvider(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        String basePath = hudiHandle.getBasePath();
        boolean isCow = "COPY_ON_WRITE".equals(hudiHandle.getHudiTableType());

        Configuration conf = buildHadoopConf();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(conf))
                .setBasePath(basePath)
                .build();

        HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline()
                .filterCompletedInstants();

        Optional<HoodieInstant> lastInstant = timeline.lastInstant().toJavaOptional();
        if (!lastInstant.isPresent()) {
            LOG.info("No completed instants on timeline for {}, returning empty splits", basePath);
            return Collections.emptyList();
        }
        String queryInstant = lastInstant.get().requestedTime();

        // Resolve column names and types for JNI reader
        List<String> columnNames;
        List<String> columnTypes;
        try {
            TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
            Schema avroSchema = schemaResolver.getTableAvroSchema();
            columnNames = avroSchema.getFields().stream()
                    .map(Schema.Field::name).collect(Collectors.toList());
            columnTypes = avroSchema.getFields().stream()
                    .map(f -> HudiTypeMapping.fromAvroSchema(unwrapNullable(f.schema())).getTypeName())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to resolve Hudi schema for JNI reader, JNI splits may fail: {}",
                    e.getMessage());
            columnNames = Collections.emptyList();
            columnTypes = Collections.emptyList();
        }

        // Build file system view via FileSystemViewManager (Hudi 1.0.2 API)
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
                .build();
        HoodieLocalEngineContext engineCtx = new HoodieLocalEngineContext(metaClient.getStorageConf());
        HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
                engineCtx, metaClient, metadataConfig);

        // Resolve partitions
        List<String> partitionPaths = resolvePartitions(hudiHandle, metaClient);

        String inputFormat = hudiHandle.getInputFormat();
        String serdeLib = hudiHandle.getSerdeLib();

        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (String partitionPath : partitionPaths) {
            Map<String, String> partValues = parsePartitionValues(
                    partitionPath, hudiHandle.getPartitionKeyNames());

            if (isCow) {
                collectCowSplits(fsView, partitionPath, queryInstant,
                        basePath, partValues, ranges);
            } else {
                collectMorSplits(fsView, partitionPath, queryInstant,
                        basePath, inputFormat, serdeLib,
                        columnNames, columnTypes, partValues, ranges);
            }
        }

        LOG.info("Hudi scan planning: {}.{} type={} partitions={} splits={}",
                hudiHandle.getDbName(), hudiHandle.getTableName(),
                hudiHandle.getHudiTableType(), partitionPaths.size(), ranges.size());

        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        boolean isCow = "COPY_ON_WRITE".equals(hudiHandle.getHudiTableType());

        Map<String, String> props = new LinkedHashMap<>();
        // For COW tables, we default to parquet (may be overridden per-split).
        // For MOR tables, default is JNI.
        props.put("file_format_type", isCow ? "parquet" : "jni");
        props.put("table_format_type", "hudi");

        // Partition keys
        List<String> partKeys = hudiHandle.getPartitionKeyNames();
        if (partKeys != null && !partKeys.isEmpty()) {
            props.put("path_partition_keys", String.join(",", partKeys));
        }

        // Location/storage properties for native and JNI readers
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")
                    || key.startsWith("s3.") || key.startsWith("cos.")
                    || key.startsWith("oss.") || key.startsWith("obs.")) {
                props.put("location." + key, entry.getValue());
            }
        }

        return props;
    }

    /**
     * Collect splits for COW (Copy on Write) tables.
     * COW tables only have base files — use native Parquet/ORC reader.
     */
    private void collectCowSplits(
            HoodieTableFileSystemView fsView,
            String partitionPath, String queryInstant,
            String basePath,
            Map<String, String> partValues,
            List<ConnectorScanRange> ranges) {
        fsView.getLatestBaseFilesBeforeOrOn(partitionPath, queryInstant)
                .forEach(baseFile -> {
                    String filePath = baseFile.getPath();
                    long fileSize = baseFile.getFileSize();
                    String format = detectFileFormat(filePath);

                    ranges.add(new HudiScanRange.Builder()
                            .path(filePath)
                            .start(0)
                            .length(fileSize)
                            .fileSize(fileSize)
                            .fileFormat(format)
                            .partitionValues(partValues)
                            .build());
                });
    }

    /**
     * Collect splits for MOR (Merge on Read) tables.
     * MOR tables may have base files + delta log files. If a file slice
     * has no delta logs, we can use the native reader; otherwise JNI.
     */
    private void collectMorSplits(
            HoodieTableFileSystemView fsView,
            String partitionPath, String queryInstant,
            String basePath, String inputFormat, String serdeLib,
            List<String> columnNames, List<String> columnTypes,
            Map<String, String> partValues,
            List<ConnectorScanRange> ranges) {
        fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant)
                .forEach(fileSlice -> {
                    Optional<HoodieBaseFile> baseFileOpt = fileSlice.getBaseFile().toJavaOptional();
                    String filePath = baseFileOpt.map(BaseFile::getPath).orElse("");
                    long fileSize = baseFileOpt.map(BaseFile::getFileSize).orElse(0L);

                    List<String> logs = fileSlice.getLogFiles()
                            .map(HoodieLogFile::getPath)
                            .map(StoragePath::toString)
                            .collect(Collectors.toList());

                    // Dynamic format decision: no logs → native reader
                    boolean useNative = logs.isEmpty() && !filePath.isEmpty();
                    String format = useNative ? detectFileFormat(filePath) : "jni";

                    // For log-only slices, use first log as agency path
                    String agencyPath = filePath.isEmpty() && !logs.isEmpty()
                            ? logs.get(0) : filePath;

                    HudiScanRange.Builder builder = new HudiScanRange.Builder()
                            .path(agencyPath)
                            .start(0)
                            .length(fileSize)
                            .fileSize(fileSize)
                            .fileFormat(format)
                            .partitionValues(partValues);

                    if (!useNative) {
                        // JNI reader needs full metadata
                        builder.instantTime(queryInstant)
                                .serde(serdeLib)
                                .inputFormat(inputFormat)
                                .basePath(basePath)
                                .dataFilePath(filePath)
                                .dataFileLength(fileSize)
                                .deltaLogs(logs)
                                .columnNames(columnNames)
                                .columnTypes(columnTypes);
                    }

                    ranges.add(builder.build());
                });
    }

    /**
     * Resolve partition paths from handle or by listing all partitions.
     */
    private List<String> resolvePartitions(
            HudiTableHandle handle, HoodieTableMetaClient metaClient) {
        // Check if partitions were pruned via applyFilter
        List<String> prunedPaths = handle.getPrunedPartitionPaths();
        if (prunedPaths != null) {
            return prunedPaths;
        }

        // No pruning — list all partitions
        List<String> partKeyNames = handle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Unpartitioned table
            return Collections.singletonList("");
        }

        try {
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
                    .build();
            HoodieLocalEngineContext engineCtx = new HoodieLocalEngineContext(metaClient.getStorageConf());
            HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
                    engineCtx, metaClient.getStorage(), metadataConfig,
                    metaClient.getBasePath().toString(), true);
            return tableMetadata.getAllPartitionPaths();
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to list partitions for " + handle.getBasePath(), e);
        }
    }

    /**
     * Parse partition path "year=2024/month=01" into column→value map.
     */
    private Map<String, String> parsePartitionValues(
            String partitionPath, List<String> partKeyNames) {
        if (partitionPath == null || partitionPath.isEmpty()
                || partKeyNames == null || partKeyNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> values = new LinkedHashMap<>();
        String[] parts = partitionPath.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                values.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return values;
    }

    /**
     * Detect file format from file path suffix.
     */
    private static String detectFileFormat(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return "parquet";
        }
        String lower = filePath.toLowerCase();
        if (lower.endsWith(".parquet")) {
            return "parquet";
        } else if (lower.endsWith(".orc")) {
            return "orc";
        }
        return "parquet";
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    return s;
                }
            }
        }
        return schema;
    }

    private Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")) {
                conf.set(key, entry.getValue());
            }
        }
        return conf;
    }
}
