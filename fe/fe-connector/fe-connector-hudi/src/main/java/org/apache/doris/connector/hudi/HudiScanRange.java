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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

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

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;
    private final Map<String, String> partitionValues;
    private final Map<String, String> properties;

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
        if (builder.deltaLogs != null && !builder.deltaLogs.isEmpty()) {
            props.put("hudi.delta_logs", String.join(",", builder.deltaLogs));
        }
        if (builder.columnNames != null && !builder.columnNames.isEmpty()) {
            props.put("hudi.column_names", String.join(",", builder.columnNames));
        }
        if (builder.columnTypes != null && !builder.columnTypes.isEmpty()) {
            props.put("hudi.column_types", String.join(",", builder.columnTypes));
        }
        this.properties = Collections.unmodifiableMap(props);
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
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

        public HudiScanRange build() {
            return new HudiScanRange(this);
        }
    }
}
