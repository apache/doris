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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Scan plan provider for Paimon tables.
 *
 * <p>Three split paths:
 * <ol>
 *   <li><b>JNI reader</b> (default): Serializes the entire Paimon {@code Split} object.
 *       BE calls back into Paimon Java code via JNI.</li>
 *   <li><b>Native reader</b>: When {@code DataSplit.convertToRawFiles()} succeeds and
 *       all files are ORC/Parquet. BE reads files natively.</li>
 *   <li><b>COUNT pushdown</b>: When the query is COUNT(*) and the split has
 *       pre-computed merged row count.</li>
 * </ol>
 *
 * <p><b>Partition pruning (P5-T09): pure predicate pushdown.</b> Only the 4-arg
 * {@link #planScan} is overridden; the engine's 6-arg {@code planScan(..., requiredPartitions)}
 * (the Nereids-pruned partition set) is intentionally NOT overridden. Paimon prunes partitions
 * <em>and</em> data files internally: the Doris filter is converted by
 * {@link PaimonPredicateConverter} and pushed via {@code ReadBuilder.withFilter}, and the Paimon
 * SDK's {@code newScan().plan().splits()} eliminates non-matching partitions/files from those
 * predicates. Partition columns are ordinary columns in Paimon's {@code RowType}, so a partition
 * predicate is just another pushed predicate. This differs from MaxCompute (whose ODPS read
 * session needs explicit {@code PartitionSpec}s and therefore consumes {@code requiredPartitions});
 * for Paimon the engine set would be redundant with the predicate it already pushes. The SPI
 * default chain (6-arg &rarr; 5-arg &rarr; 4-arg) routes correctly with {@code requiredPartitions}
 * dropped. Consequence: FE EXPLAIN shows {@code partition=0/0} (no engine-level partition count)
 * because the FE currently treats Paimon tables as non-partitioned; that is a known display gap
 * tracked with the {@code partition_columns} wiring deferred to a later batch (B5), and does not
 * affect read-row correctness.
 */
public class PaimonScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonScanPlanProvider.class);

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    private final Map<String, String> properties;
    private final PaimonCatalogOps catalogOps;

    public PaimonScanPlanProvider(Map<String, String> properties, PaimonCatalogOps catalogOps) {
        this.properties = properties;
        this.catalogOps = catalogOps;
    }

    /**
     * Returns the handle's transient Paimon {@link Table}, reloading it from the catalog seam
     * when the transient reference is null (e.g. after a serialization round-trip across the
     * FE/BE boundary or plan reuse). Delegates to the single sys-aware {@link PaimonTableResolver}
     * shared with the metadata path, so a deserialized SYSTEM handle reloads its own (sys) Table
     * via the 4-arg sys {@link Identifier} instead of silently scanning the base table.
     * Package-private for direct unit testing.
     *
     * <p>NOTE: the reloaded Table may come from a different {@link org.apache.paimon.catalog.Catalog}
     * instance than the one that produced the handle. That is acceptable for this fallback safety
     * net (it is not snapshot-consistent with the handle's originating catalog).
     */
    Table resolveTable(PaimonTableHandle paimonHandle) {
        try {
            return PaimonTableResolver.resolve(catalogOps, paimonHandle);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Paimon table: " + paimonHandle, e);
        }
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {

        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = resolveTable(paimonHandle);

        // Build predicates from filter expression
        RowType rowType = table.rowType();
        List<org.apache.paimon.predicate.Predicate> predicates = Collections.emptyList();
        if (filter.isPresent()) {
            PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
            predicates = converter.convert(filter.get());
        }

        // Build column projection
        List<String> fieldNames = rowType.getFieldNames().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        int[] projected = columns.stream()
                .filter(c -> c instanceof PaimonColumnHandle)
                .mapToInt(c -> fieldNames.indexOf(
                        ((PaimonColumnHandle) c).getName().toLowerCase()))
                .filter(i -> i >= 0)
                .toArray();

        // Call Paimon SDK
        ReadBuilder readBuilder = table.newReadBuilder();
        if (!predicates.isEmpty()) {
            readBuilder.withFilter(predicates);
        }
        if (projected.length > 0) {
            readBuilder.withProjection(projected);
        }
        TableScan scan = readBuilder.newScan();
        List<Split> paimonSplits = scan.plan().splits();

        // Determine table location
        String tableLocation = getTableLocation(table);
        String defaultFileFormat = table.options().getOrDefault(
                CoreOptions.FILE_FORMAT.key(), "parquet");

        // Separate DataSplit vs non-DataSplit
        List<DataSplit> dataSplits = new ArrayList<>();
        List<Split> nonDataSplits = new ArrayList<>();
        for (Split split : paimonSplits) {
            if (split instanceof DataSplit) {
                dataSplits.add((DataSplit) split);
            } else {
                nonDataSplits.add(split);
            }
        }

        List<ConnectorScanRange> ranges = new ArrayList<>();

        // Non-DataSplit → always JNI
        for (Split split : nonDataSplits) {
            ranges.add(buildJniScanRange(split, tableLocation, defaultFileFormat,
                    Collections.emptyMap(), false));
        }

        // Process DataSplits
        for (DataSplit dataSplit : dataSplits) {
            Map<String, String> partitionValues = getPartitionInfoMap(
                    table, dataSplit.partition());

            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();

            if (shouldUseNativeReader(paimonHandle.isForceJni(), optRawFiles)) {
                // Native reader path
                List<RawFile> rawFiles = optRawFiles.get();
                for (int i = 0; i < rawFiles.size(); i++) {
                    RawFile file = rawFiles.get(i);
                    String fileFormat = getFileFormatBySuffix(file.path())
                            .orElse(defaultFileFormat);

                    PaimonScanRange.Builder builder = new PaimonScanRange.Builder()
                            .path(file.path())
                            .start(0)
                            .length(file.length())
                            .fileSize(file.length())
                            .fileFormat(fileFormat)
                            .partitionValues(partitionValues)
                            .schemaId(file.schemaId());

                    if (optDeletionFiles.isPresent()
                            && i < optDeletionFiles.get().size()
                            && optDeletionFiles.get().get(i) != null) {
                        DeletionFile df = optDeletionFiles.get().get(i);
                        builder.deletionFile(df.path(), df.offset(), df.length());
                    }

                    ranges.add(builder.build());
                }
            } else {
                // JNI reader path
                ranges.add(buildJniScanRange(
                        dataSplit, tableLocation, defaultFileFormat,
                        partitionValues, true));
            }
        }

        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {

        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = resolveTable(paimonHandle);

        Map<String, String> props = new LinkedHashMap<>();

        // File format type (default)
        props.put("file_format_type", "jni");
        props.put("table_format_type", "paimon");

        // Serialized table for BE's JNI reader
        String serializedTable = encodeObjectToString(table);
        props.put("paimon.serialized_table", serializedTable);

        // Serialized predicates for BE's JNI scanner
        if (filter.isPresent()) {
            RowType rowType = table.rowType();
            PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
            List<org.apache.paimon.predicate.Predicate> predicates = converter.convert(filter.get());
            if (!predicates.isEmpty()) {
                String serializedPredicate = encodeObjectToString(predicates);
                props.put("paimon.predicate", serializedPredicate);
            }
        }

        // Paimon JDBC metastore options for BE (if applicable)
        Map<String, String> backendOptions = getBackendPaimonOptions();
        if (!backendOptions.isEmpty()) {
            // Encode as JSON for transport
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : backendOptions.entrySet()) {
                if (!first) {
                    sb.append(",");
                }
                sb.append("\"").append(escapeJson(entry.getKey()))
                        .append("\":\"").append(escapeJson(entry.getValue())).append("\"");
                first = false;
            }
            sb.append("}");
            props.put("paimon.options_json", sb.toString());
        }

        // Location / storage properties
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

    private PaimonScanRange buildJniScanRange(Split split, String tableLocation,
            String defaultFileFormat, Map<String, String> partitionValues,
            boolean isDataSplit) {
        long splitWeight = 0;
        if (isDataSplit) {
            splitWeight = computeSplitWeight((DataSplit) split);
        } else {
            splitWeight = split.rowCount();
        }

        String serializedSplit = encodeObjectToString(split);

        return new PaimonScanRange.Builder()
                .fileFormat("jni")
                .paimonSplit(serializedSplit)
                .tableLocation(tableLocation)
                .partitionValues(partitionValues)
                .selfSplitWeight(splitWeight)
                .build();
    }

    private long computeSplitWeight(DataSplit dataSplit) {
        List<DataFileMeta> metas = dataSplit.dataFiles();
        if (metas != null && !metas.isEmpty()) {
            return metas.stream().mapToLong(DataFileMeta::fileSize).sum();
        }
        return dataSplit.rowCount();
    }

    /**
     * Decides whether a {@link DataSplit} may take the native (ORC/Parquet) reader path.
     *
     * <p>The split is native-eligible iff (a) it is NOT name-forced to JNI by the handle, AND (b) its
     * raw files all support the native reader (see {@link #supportNativeReader}). Gating on
     * {@code forceJni} is the T19 fix: {@code binlog} / {@code audit_log} system tables are paimon
     * {@code DataTable}s whose {@code DataSplit.convertToRawFiles()} may succeed, but the native
     * reader cannot reproduce their read semantics (binlog pack/merge + array materialization;
     * audit_log rowkind/sequence-number projection), so they would silently return wrong rows. Legacy
     * forces them to JNI ({@code PaimonScanNode.shouldForceJniForSystemTable}, captured by
     * {@link PaimonTableHandle#isForceJni()}). ONLY the {@code forceJni} flag gates this: metadata sys
     * tables already go JNI via the non-DataSplit path, and a non-forced {@code DataTable} like "ro"
     * (forceJni=false) must still be allowed native — so this must not over-force.
     *
     * <p>Extracted as a pure static so the correctness-critical routing decision is unit-testable
     * with real {@link RawFile}s, without driving a full Paimon {@code ReadBuilder}/{@code TableScan}.
     */
    static boolean shouldUseNativeReader(boolean forceJni, Optional<List<RawFile>> optRawFiles) {
        return !forceJni && supportNativeReader(optRawFiles);
    }

    private static boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
        if (!optRawFiles.isPresent() || optRawFiles.get().isEmpty()) {
            return false;
        }
        for (RawFile file : optRawFiles.get()) {
            String path = file.path().toLowerCase();
            if (!path.endsWith(".orc") && !path.endsWith(".parquet")) {
                return false;
            }
        }
        return true;
    }

    private Map<String, String> getPartitionInfoMap(Table table, BinaryRow partitionValue) {
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        RowType partitionType = table.rowType().project(partitionKeys);
        RowDataToObjectArrayConverter converter =
                new RowDataToObjectArrayConverter(partitionType);
        Object[] values = converter.convert(partitionValue);

        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            String key = partitionKeys.get(i);
            String value = values[i] != null ? values[i].toString() : null;
            result.put(key, value);
        }
        return result;
    }

    private String getTableLocation(Table table) {
        if (table instanceof FileStoreTable) {
            return ((FileStoreTable) table).location().toString();
        }
        return table.options().get("path");
    }

    private Map<String, String> getBackendPaimonOptions() {
        String metastoreType = properties.get("paimon.catalog.type");
        if (!"jdbc".equalsIgnoreCase(metastoreType)) {
            return Collections.emptyMap();
        }
        Map<String, String> options = new HashMap<>();
        // Forward relevant JDBC catalog properties for BE's paimon-cpp reader
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("jdbc.") || key.equals("warehouse")
                    || key.equals("uri") || key.equals("metastore")
                    || key.equals("catalog-key")) {
                options.put(key, entry.getValue());
            }
        }
        return options;
    }

    private static Optional<String> getFileFormatBySuffix(String path) {
        if (path == null) {
            return Optional.empty();
        }
        String lower = path.toLowerCase();
        if (lower.endsWith(".orc")) {
            return Optional.of("orc");
        } else if (lower.endsWith(".parquet") || lower.endsWith(".parq")) {
            return Optional.of("parquet");
        }
        return Optional.empty();
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> properties) {
        String predicate = properties.get("paimon.predicate");
        if (predicate != null) {
            params.setPaimonPredicate(predicate);
        }

        String optionsJson = properties.get("paimon.options_json");
        if (optionsJson != null && !optionsJson.isEmpty()) {
            try {
                Map<String, String> options = OBJECT_MAPPER
                        .readValue(optionsJson, MAP_TYPE_REF);
                params.setPaimonOptions(options);
            } catch (Exception e) {
                LOG.warn("Failed to parse paimon.options_json", e);
            }
        }
    }

    @Override
    public String getSerializedTable(Map<String, String> properties) {
        return properties.get("paimon.serialized_table");
    }

    @SuppressWarnings("unchecked")
    private static <T> String encodeObjectToString(T obj) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(obj);
            return new String(BASE64_ENCODER.encode(bytes), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object: " + e.getMessage(), e);
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
