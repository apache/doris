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

package org.apache.doris.connector.api.write;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a connector write operation.
 *
 * <p>Returned by {@link org.apache.doris.connector.api.ConnectorWriteOps#getWriteConfig}
 * to describe how data should be written. The engine (fe-core) uses this to
 * construct the appropriate Thrift data sink for BE execution.</p>
 *
 * <p>This is a value object — all fields are immutable once constructed.</p>
 */
public class ConnectorWriteConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConnectorWriteType writeType;
    private final String fileFormat;
    private final String compression;
    private final String writeLocation;
    private final List<String> partitionColumns;
    private final Map<String, String> staticPartitionValues;
    private final Map<String, String> properties;

    private ConnectorWriteConfig(Builder builder) {
        this.writeType = builder.writeType;
        this.fileFormat = builder.fileFormat;
        this.compression = builder.compression;
        this.writeLocation = builder.writeLocation;
        this.partitionColumns = builder.partitionColumns != null
                ? Collections.unmodifiableList(builder.partitionColumns)
                : Collections.emptyList();
        this.staticPartitionValues = builder.staticPartitionValues != null
                ? Collections.unmodifiableMap(builder.staticPartitionValues)
                : Collections.emptyMap();
        this.properties = builder.properties != null
                ? Collections.unmodifiableMap(builder.properties)
                : Collections.emptyMap();
    }

    /** Returns the write type determining BE sink behavior. */
    public ConnectorWriteType getWriteType() {
        return writeType;
    }

    /** Returns the file format for file-based writes (e.g., "parquet", "orc"). */
    public String getFileFormat() {
        return fileFormat;
    }

    /** Returns the compression codec (e.g., "snappy", "zstd"). */
    public String getCompression() {
        return compression;
    }

    /** Returns the target location for file writes. */
    public String getWriteLocation() {
        return writeLocation;
    }

    /** Returns partition column names for partitioned writes. */
    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    /** Returns static partition values (column name → value) for static partition inserts. */
    public Map<String, String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    /** Returns connector-specific properties passed through to BE. */
    public Map<String, String> getProperties() {
        return properties;
    }

    /** Creates a new builder. */
    public static Builder builder(ConnectorWriteType writeType) {
        return new Builder(writeType);
    }

    @Override
    public String toString() {
        return "ConnectorWriteConfig{type=" + writeType
                + ", format=" + fileFormat
                + ", compression=" + compression
                + ", location=" + writeLocation + "}";
    }

    /**
     * Builder for {@link ConnectorWriteConfig}.
     */
    public static class Builder {
        private final ConnectorWriteType writeType;
        private String fileFormat;
        private String compression;
        private String writeLocation;
        private List<String> partitionColumns;
        private Map<String, String> staticPartitionValues;
        private Map<String, String> properties;

        private Builder(ConnectorWriteType writeType) {
            this.writeType = writeType;
        }

        public Builder fileFormat(String fileFormat) {
            this.fileFormat = fileFormat;
            return this;
        }

        public Builder compression(String compression) {
            this.compression = compression;
            return this;
        }

        public Builder writeLocation(String writeLocation) {
            this.writeLocation = writeLocation;
            return this;
        }

        public Builder partitionColumns(List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder staticPartitionValues(Map<String, String> staticPartitionValues) {
            this.staticPartitionValues = staticPartitionValues;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public ConnectorWriteConfig build() {
            return new ConnectorWriteConfig(this);
        }
    }
}
