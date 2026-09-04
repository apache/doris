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

import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan range for a Hudi file split.
 *
 * <p>Supports two read paths:
 * <ul>
 *   <li><b>Native reader</b> (COW tables, MOR without delta logs): Uses Parquet/ORC
 *       format directly in BE. Only needs the base file path and schema_id.</li>
 *   <li><b>JNI reader</b> (MOR with delta logs): Uses Hudi's own merge reader via
 *       JNI in BE. Needs full metadata: instant_time, serde, input_format, base_path,
 *       data_file_path, delta_logs, column_names, column_types.</li>
 * </ul>
 */
public class HudiScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    // How hudi spells a NULL partition value in columns_from_path. Byte-frozen: it is what this connector has
    // always sent, and it is also accepted as an INPUT spelling (a "\N" partition directory means NULL too).
    private static final String HUDI_NULL_PARTITION_VALUE = "\\N";

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;
    private final Map<String, String> partitionValues;
    private final Map<String, String> properties;
    // JNI reader list fields. Kept as typed lists (NOT joined into the
    // properties map) because Hive type strings contain commas
    // (e.g. decimal(10,2), struct<a:int,b:string>): a comma join+split
    // round-trip would shatter them and misalign column_names/column_types.
    // BE (hudi_jni_reader.cpp) joins these lists itself with the correct
    // delimiters (names ',', types '#', delta logs ',').
    private final List<String> deltaLogs;
    private final List<String> columnNames;
    private final List<String> columnTypes;
    // When true (force_jni_scanner), the JNI escape hatch is engaged for this split: the no-delta-log native
    // downgrade in populateRangeParams is suppressed so a native-eligible slice still reads via the JNI reader
    // (dodging native-reader bugs). Baked in at plan time by HudiScanPlanProvider from the session flag, so
    // populateRangeParams (which has no session) stays CONSISTENT with planScan's native/JNI branch. Legacy
    // parity: HudiScanNode.setScanParams guards the same downgrade with !sessionVariable.isForceJniScanner().
    private final boolean forceJni;

    private HudiScanRange(Builder builder) {
        this.path = builder.path;
        this.start = builder.start;
        this.length = builder.length;
        this.fileSize = builder.fileSize;
        this.fileFormat = builder.fileFormat;
        this.partitionValues = builder.partitionValues != null
                ? Collections.unmodifiableMap(builder.partitionValues)
                : Collections.emptyMap();

        Map<String, String> props = new HashMap<>();
        // JNI reader fields
        if (builder.instantTime != null) {
            props.put("hudi.instant_time", builder.instantTime);
        }
        if (builder.serde != null) {
            props.put("hudi.serde", builder.serde);
        }
        if (builder.inputFormat != null) {
            props.put("hudi.input_format", builder.inputFormat);
        }
        if (builder.basePath != null) {
            props.put("hudi.base_path", builder.basePath);
        }
        if (builder.dataFilePath != null) {
            props.put("hudi.data_file_path", builder.dataFilePath);
        }
        props.put("hudi.data_file_length", String.valueOf(builder.dataFileLength));
        // Per-split native-reader schema version (mirror paimon.schema_id). Only carried when the provider
        // resolved one for a native slice; populateRangeParams stamps THudiFileDesc.schema_id (field 12) from it
        // ONLY on the native branch (never JNI). Absent -> BE BY_NAME.
        if (builder.schemaId != null) {
            props.put("hudi.schema_id", String.valueOf(builder.schemaId));
        }
        this.properties = Collections.unmodifiableMap(props);

        this.deltaLogs = builder.deltaLogs != null
                ? Collections.unmodifiableList(new ArrayList<>(builder.deltaLogs))
                : Collections.emptyList();
        this.columnNames = builder.columnNames != null
                ? Collections.unmodifiableList(new ArrayList<>(builder.columnNames))
                : Collections.emptyList();
        this.columnTypes = builder.columnTypes != null
                ? Collections.unmodifiableList(new ArrayList<>(builder.columnTypes))
                : Collections.emptyList();
        this.forceJni = builder.forceJni;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.ofNullable(path);
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String getFileFormat() {
        return fileFormat;
    }

    @Override
    public String getTableFormatType() {
        return "hudi";
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "HudiScanRange{path=" + path + ", format=" + fileFormat
                + ", start=" + start + ", length=" + length + "}";
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc,
            TFileRangeDesc rangeDesc) {
        Map<String, String> props = getProperties();
        THudiFileDesc fileDesc = new THudiFileDesc();

        boolean isJni = "jni".equalsIgnoreCase(getFileFormat());

        // A JNI-format split with no delta logs (a read-optimized / log-less slice) reads natively — UNLESS
        // force_jni is engaged (legacy HudiScanNode.setScanParams' !isForceJniScanner() guard). In practice
        // collectMorSplits/collectCowSplits already stamp the native format directly, so this only resolves a
        // defensively-built "jni"+no-log range.
        if (isJni && deltaLogs.isEmpty() && !forceJni) {
            String dataFilePath = props.getOrDefault("hudi.data_file_path", "");
            String lower = dataFilePath.toLowerCase();
            if (lower.endsWith(".parquet") || lower.endsWith(".orc")) {
                isJni = false;
            }
        }

        // Set the per-range format EXPLICITLY (mirroring PaimonScanRange): the node-level file_format_type is a
        // SINGLE default per table and cannot be correct for every slice — a MOR table mixes native no-log
        // slices with JNI log slices, a COW ORC table's node default is parquet, and force_jni keeps a COW slice
        // on JNI. Relying on that default silently delivered the wrong reader to BE (an empty THudiFileDesc under
        // FORMAT_JNI for a native no-log slice, or the native reader for a force_jni / ORC slice).
        if (isJni) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            fileDesc.setInstantTime(
                    props.getOrDefault("hudi.instant_time", ""));
            fileDesc.setSerde(props.getOrDefault("hudi.serde", ""));
            fileDesc.setInputFormat(
                    props.getOrDefault("hudi.input_format", ""));
            fileDesc.setBasePath(
                    props.getOrDefault("hudi.base_path", ""));
            fileDesc.setDataFilePath(
                    props.getOrDefault("hudi.data_file_path", ""));
            fileDesc.setDataFileLength(Long.parseLong(
                    props.getOrDefault("hudi.data_file_length", "0")));

            // Set typed lists directly. BE (hudi_jni_reader.cpp) joins them with
            // the correct delimiters: column_names ',', column_types '#', delta
            // logs ','. Joining/splitting here would shatter comma-bearing Hive
            // type strings (decimal(10,2), struct<...>).
            if (!deltaLogs.isEmpty()) {
                fileDesc.setDeltaLogs(deltaLogs);
            }
            if (!columnNames.isEmpty()) {
                fileDesc.setColumnNames(columnNames);
            }
            if (!columnTypes.isEmpty()) {
                fileDesc.setColumnTypes(columnTypes);
            }
        } else {
            rangeDesc.setFormatType(nativeFormatType(props));
            // Native field-id path only (paimon parity): the per-split schema version the native reader matches
            // the base file's columns against. The JNI reader consumes no schema_id (it reads column_names/types
            // @instant), so this is NEVER set on the JNI branch. Absent -> BE BY_NAME (no evolution).
            String schemaId = props.get("hudi.schema_id");
            if (schemaId != null) {
                fileDesc.setSchemaId(Long.parseLong(schemaId));
            }
        }

        formatDesc.setHudiParams(fileDesc);

        // Set partition values for path-based partition extraction
        Map<String, String> partValues = getPartitionValues();
        if (partValues != null && !partValues.isEmpty()) {
            List<String> pathKeys = new ArrayList<>();
            List<String> pathValues = new ArrayList<>();
            List<Boolean> pathIsNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : partValues.entrySet()) {
                // A hudi partition value is a DIRECTORY NAME (HudiScanPlanProvider.parsePartitionValues
                // unescapes it out of the partition path), so three spellings all mean SQL NULL: the
                // hive-canonical sentinel, the older text-table "\N", and — defensively — a Java null.
                // This 3-way rule lives here rather than in the neutral module because it only holds for
                // directory-name partitioning: hive narrows it (a hive column may hold "\N" as DATA) and
                // paimon rejects it outright (its partition values are typed, so "\N" is ordinary data).
                // Rendering: hudi emits "\N" for a NULL where hive/paimon/iceberg emit "" — BE ignores the
                // string whenever the flag is set, but the bytes stay as they were (see
                // HudiScanRangePartitionValuesTest).
                String value = entry.getValue();
                boolean nullValue = value == null
                        || ConnectorPartitionValues.NULL_PARTITION_NAME.equals(value)
                        || HUDI_NULL_PARTITION_VALUE.equals(value);
                pathKeys.add(entry.getKey());
                pathValues.add(nullValue ? HUDI_NULL_PARTITION_VALUE : value);
                pathIsNull.add(nullValue);
            }
            rangeDesc.setColumnsFromPathKeys(pathKeys);
            rangeDesc.setColumnsFromPath(pathValues);
            rangeDesc.setColumnsFromPathIsNull(pathIsNull);
        }
    }

    /**
     * The BE native reader format for a non-JNI slice: from the range's own file format when it is already
     * native (collectCowSplits / a no-log MOR slice stamp "parquet"/"orc" directly), else — for a "jni" range
     * downgraded above — from the base file suffix. Defaults to parquet (matching {@code detectFileFormat}).
     */
    private TFileFormatType nativeFormatType(Map<String, String> props) {
        String fmt = getFileFormat();
        if ("orc".equalsIgnoreCase(fmt)) {
            return TFileFormatType.FORMAT_ORC;
        }
        if ("parquet".equalsIgnoreCase(fmt)) {
            return TFileFormatType.FORMAT_PARQUET;
        }
        String dataFilePath = props.getOrDefault("hudi.data_file_path", "");
        return dataFilePath.toLowerCase().endsWith(".orc")
                ? TFileFormatType.FORMAT_ORC : TFileFormatType.FORMAT_PARQUET;
    }

    /** Builder for constructing HudiScanRange instances. */
    public static class Builder {
        private String path;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        private String fileFormat = "jni";
        private Map<String, String> partitionValues;

        // JNI reader metadata
        private String instantTime;
        private String serde;
        private String inputFormat;
        private String basePath;
        private String dataFilePath;
        private long dataFileLength;
        private List<String> deltaLogs;
        private List<String> columnNames;
        private List<String> columnTypes;
        private boolean forceJni;
        // Native-reader per-split schema version (nullable = not stamped; JNI slices never carry one).
        private Long schemaId;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder start(long start) {
            this.start = start;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder fileFormat(String fileFormat) {
            this.fileFormat = fileFormat;
            return this;
        }

        public Builder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public Builder instantTime(String instantTime) {
            this.instantTime = instantTime;
            return this;
        }

        public Builder serde(String serde) {
            this.serde = serde;
            return this;
        }

        public Builder inputFormat(String inputFormat) {
            this.inputFormat = inputFormat;
            return this;
        }

        public Builder basePath(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public Builder dataFilePath(String dataFilePath) {
            this.dataFilePath = dataFilePath;
            return this;
        }

        public Builder dataFileLength(long dataFileLength) {
            this.dataFileLength = dataFileLength;
            return this;
        }

        public Builder deltaLogs(List<String> deltaLogs) {
            this.deltaLogs = deltaLogs;
            return this;
        }

        public Builder columnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder columnTypes(List<String> columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public Builder forceJni(boolean forceJni) {
            this.forceJni = forceJni;
            return this;
        }

        public Builder schemaId(long schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        public HudiScanRange build() {
            return new HudiScanRange(this);
        }
    }
}
