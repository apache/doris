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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan range for a Hive file split.
 *
 * <p>Represents a byte range within a file in a Hive table partition.
 * Unlike JNI-based scan ranges (ES, JDBC, MaxCompute), Hive scan ranges
 * carry real file paths with byte offsets, native file format types
 * (PARQUET/ORC/TEXT), and partition values.</p>
 *
 * <p>For ACID tables, additional properties carry transaction info
 * (partition location, delete delta paths).</p>
 */
public class HiveScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long modificationTime;
    private final List<String> hosts;
    private final String fileFormat;
    private final String tableFormatType;
    private final Map<String, String> partitionValues;
    private final Map<String, String> properties;

    private HiveScanRange(Builder builder) {
        this.path = builder.path;
        this.start = builder.start;
        this.length = builder.length;
        this.fileSize = builder.fileSize;
        this.modificationTime = builder.modificationTime;
        this.hosts = builder.hosts != null
                ? Collections.unmodifiableList(builder.hosts)
                : Collections.emptyList();
        this.fileFormat = builder.fileFormat;
        this.tableFormatType = builder.tableFormatType;
        this.partitionValues = builder.partitionValues != null
                ? Collections.unmodifiableMap(builder.partitionValues)
                : Collections.emptyMap();
        this.properties = builder.properties != null
                ? Collections.unmodifiableMap(builder.properties)
                : Collections.emptyMap();
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
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public List<String> getHosts() {
        return hosts;
    }

    @Override
    public String getFileFormat() {
        return fileFormat;
    }

    @Override
    public String getTableFormatType() {
        return tableFormatType;
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
        return "HiveScanRange{path=" + path + ", start=" + start
                + ", length=" + length + ", format=" + fileFormat
                + ", tableFormat=" + tableFormatType + "}";
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for HiveScanRange.
     */
    public static final class Builder {
        private String path;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        private long modificationTime;
        private List<String> hosts;
        private String fileFormat = "parquet";
        private String tableFormatType = "hive";
        private Map<String, String> partitionValues;
        private Map<String, String> properties;

        private Builder() {
        }

        public Builder path(String val) {
            this.path = val;
            return this;
        }

        public Builder start(long val) {
            this.start = val;
            return this;
        }

        public Builder length(long val) {
            this.length = val;
            return this;
        }

        public Builder fileSize(long val) {
            this.fileSize = val;
            return this;
        }

        public Builder modificationTime(long val) {
            this.modificationTime = val;
            return this;
        }

        public Builder hosts(List<String> val) {
            this.hosts = val;
            return this;
        }

        public Builder fileFormat(String val) {
            this.fileFormat = val;
            return this;
        }

        public Builder tableFormatType(String val) {
            this.tableFormatType = val;
            return this;
        }

        public Builder partitionValues(Map<String, String> val) {
            this.partitionValues = val;
            return this;
        }

        public Builder properties(Map<String, String> val) {
            this.properties = val;
            return this;
        }

        /**
         * Adds ACID-related properties for transactional Hive tables.
         *
         * @param partitionLocation the partition directory path
         * @param deleteDeltas      list of delete delta descriptors (dir|file1,file2)
         */
        public Builder acidInfo(String partitionLocation, List<String> deleteDeltas) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.tableFormatType = "transactional_hive";
            this.properties.put("acid.partition_location", partitionLocation);
            if (deleteDeltas != null) {
                this.properties.put("acid.delete_delta_count",
                        String.valueOf(deleteDeltas.size()));
                for (int i = 0; i < deleteDeltas.size(); i++) {
                    this.properties.put("acid.delete_delta." + i, deleteDeltas.get(i));
                }
            }
            return this;
        }

        public HiveScanRange build() {
            return new HiveScanRange(this);
        }
    }
}
