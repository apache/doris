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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Scan range for Paimon tables.
 *
 * <p>Supports three read paths:
 * <ul>
 *   <li><b>JNI reader</b> (default): The entire Paimon {@code Split} object is serialized
 *       and sent to BE. BE calls back into Paimon Java code via JNI.
 *       Properties carry {@code paimon.split} (serialized split).</li>
 *   <li><b>Native reader</b>: When {@code DataSplit.convertToRawFiles()} succeeds and
 *       all files are ORC/Parquet. Properties carry {@code paimon.schema_id} and
 *       optional {@code paimon.deletion_file.*}.</li>
 *   <li><b>COUNT pushdown</b>: When the query is a simple COUNT(*) and the split
 *       has pre-computed merged row count. Properties carry {@code paimon.row_count}.</li>
 * </ul>
 */
public class PaimonScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;
    private final Map<String, String> partitionValues;
    private final Map<String, String> properties;
    private final long selfSplitWeight;

    private PaimonScanRange(Builder builder) {
        this.path = builder.path;
        this.start = builder.start;
        this.length = builder.length;
        this.fileSize = builder.fileSize;
        this.fileFormat = builder.fileFormat;
        this.selfSplitWeight = builder.selfSplitWeight;
        this.partitionValues = builder.partitionValues != null
                ? Collections.unmodifiableMap(builder.partitionValues)
                : Collections.emptyMap();

        Map<String, String> props = new HashMap<>();
        if (builder.paimonSplit != null) {
            props.put("paimon.split", builder.paimonSplit);
        }
        if (builder.tableLocation != null) {
            props.put("paimon.table_location", builder.tableLocation);
        }
        if (builder.schemaId != null) {
            props.put("paimon.schema_id", String.valueOf(builder.schemaId));
        }
        if (builder.deletionFilePath != null) {
            props.put("paimon.deletion_file.path", builder.deletionFilePath);
            props.put("paimon.deletion_file.offset", String.valueOf(builder.deletionFileOffset));
            props.put("paimon.deletion_file.length", String.valueOf(builder.deletionFileLength));
        }
        if (builder.rowCount != null) {
            props.put("paimon.row_count", String.valueOf(builder.rowCount));
        }
        if (builder.selfSplitWeight > 0) {
            props.put("paimon.self_split_weight", String.valueOf(builder.selfSplitWeight));
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
        return "paimon";
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public long getSelfSplitWeight() {
        return selfSplitWeight;
    }

    @Override
    public String toString() {
        return "PaimonScanRange{path=" + path + ", format=" + fileFormat
                + ", start=" + start + ", length=" + length + "}";
    }

    /** Builder for constructing PaimonScanRange instances. */
    public static class Builder {
        private String path;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        private String fileFormat = "jni";
        private Map<String, String> partitionValues;
        private long selfSplitWeight;

        // JNI reader fields
        private String paimonSplit;
        private String tableLocation;

        // Native reader fields
        private Long schemaId;
        private String deletionFilePath;
        private long deletionFileOffset;
        private long deletionFileLength;

        // COUNT pushdown
        private Long rowCount;

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

        public Builder selfSplitWeight(long selfSplitWeight) {
            this.selfSplitWeight = selfSplitWeight;
            return this;
        }

        public Builder paimonSplit(String paimonSplit) {
            this.paimonSplit = paimonSplit;
            return this;
        }

        public Builder tableLocation(String tableLocation) {
            this.tableLocation = tableLocation;
            return this;
        }

        public Builder schemaId(long schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        public Builder deletionFile(String path, long offset, long length) {
            this.deletionFilePath = path;
            this.deletionFileOffset = offset;
            this.deletionFileLength = length;
            return this;
        }

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public PaimonScanRange build() {
            return new PaimonScanRange(this);
        }
    }
}
