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

import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.ArrayList;
import java.util.Collections;
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

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc,
            TFileRangeDesc rangeDesc) {
        // Rewrite columns-from-path from the connector's partition values, mirroring
        // IcebergScanRange/PaimonScanRange. This connector now OWNS the Hive default-partition
        // sentinel (__HIVE_DEFAULT_PARTITION__ -> SQL NULL) mapping that fe-core's
        // FilePartitionUtils.normalizeColumnsFromPath used to do — hive was the last connector
        // relying on that engine-side string match. The parent (FileQueryScanNode) has pre-filled a
        // path-parsed columns-from-path; unset it, then re-set from partitionValues so BE receives the
        // authoritative keys/values/is_null. partitionValues keys are the partition column names (same
        // order as path_partition_keys, both from HiveTableHandle.getPartitionKeyNames), so the emitted
        // bytes are unchanged from the legacy path. Use the NARROW NULL_PARTITION_NAME.equals — hudi's
        // wider directory-name rule (HudiScanRange.populateRangeParams, which also nulls a literal "\N")
        // must NOT be reused here: an HMS partition value is either a real value or the
        // __HIVE_DEFAULT_PARTITION__ directory sentinel, never a Java null, and a hive column may carry the
        // two characters "\N" as DATA; matching legacy normalizeColumnsFromPath, a null value maps to SQL
        // NULL defensively.
        rangeDesc.unsetColumnsFromPath();
        rangeDesc.unsetColumnsFromPathKeys();
        rangeDesc.unsetColumnsFromPathIsNull();
        if (!partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>(partitionValues.size());
            List<String> values = new ArrayList<>(partitionValues.size());
            List<Boolean> isNull = new ArrayList<>(partitionValues.size());
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                String value = entry.getValue();
                boolean nullValue = value == null
                        || ConnectorPartitionValues.NULL_PARTITION_NAME.equals(value);
                keys.add(entry.getKey());
                values.add(nullValue ? "" : value);
                isNull.add(nullValue);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(values);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
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

        public HiveScanRange build() {
            return new HiveScanRange(this);
        }
    }
}
