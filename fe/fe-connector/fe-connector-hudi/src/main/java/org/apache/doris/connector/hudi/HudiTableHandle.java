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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Opaque table handle for a Hudi table, carrying database/table coordinates,
 * base path for MetaClient, Hudi table type (COW/MOR), and scan-related
 * metadata populated by {@link HudiConnectorMetadata#getTableHandle}.
 *
 * <p>After {@code applyFilter}, a new handle may carry
 * {@link #getPrunedPartitionPaths()} for partition-pruned scans.</p>
 */
public class HudiTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;
    private final String basePath;
    private final String hudiTableType;

    // Scan-related metadata (populated from HmsTableInfo)
    private final String inputFormat;
    private final String serdeLib;
    private final List<String> partitionKeyNames;
    private final Map<String, String> tableParameters;

    // Set after applyFilter for partition pruning
    private final List<String> prunedPartitionPaths;

    private HudiTableHandle(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.basePath = builder.basePath;
        this.hudiTableType = builder.hudiTableType;
        this.inputFormat = builder.inputFormat;
        this.serdeLib = builder.serdeLib;
        this.partitionKeyNames = builder.partitionKeyNames != null
                ? Collections.unmodifiableList(builder.partitionKeyNames)
                : Collections.emptyList();
        this.tableParameters = builder.tableParameters != null
                ? Collections.unmodifiableMap(builder.tableParameters)
                : Collections.emptyMap();
        this.prunedPartitionPaths = builder.prunedPartitionPaths;
    }

    /** Legacy constructor for Phase 1 compatibility (metadata-only). */
    public HudiTableHandle(String dbName, String tableName, String basePath, String hudiTableType) {
        this(new Builder(dbName, tableName, basePath, hudiTableType));
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getBasePath() {
        return basePath;
    }

    public String getHudiTableType() {
        return hudiTableType;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getSerdeLib() {
        return serdeLib;
    }

    public List<String> getPartitionKeyNames() {
        return partitionKeyNames;
    }

    public Map<String, String> getTableParameters() {
        return tableParameters;
    }

    public List<String> getPrunedPartitionPaths() {
        return prunedPartitionPaths;
    }

    /** Returns a builder pre-populated with this handle's state, for creating modified copies. */
    public Builder toBuilder() {
        Builder b = new Builder(dbName, tableName, basePath, hudiTableType);
        b.inputFormat = this.inputFormat;
        b.serdeLib = this.serdeLib;
        b.partitionKeyNames = this.partitionKeyNames;
        b.tableParameters = this.tableParameters;
        b.prunedPartitionPaths = this.prunedPartitionPaths;
        return b;
    }

    @Override
    public String toString() {
        return "HudiTableHandle{" + dbName + "." + tableName
                + ", basePath=" + basePath + ", type=" + hudiTableType + "}";
    }

    /**
     * Builder for constructing HudiTableHandle with scan metadata.
     */
    public static final class Builder {
        private final String dbName;
        private final String tableName;
        private final String basePath;
        private final String hudiTableType;
        private String inputFormat;
        private String serdeLib;
        private List<String> partitionKeyNames;
        private Map<String, String> tableParameters;
        private List<String> prunedPartitionPaths;

        public Builder(String dbName, String tableName, String basePath, String hudiTableType) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.basePath = basePath;
            this.hudiTableType = hudiTableType;
        }

        public Builder inputFormat(String val) {
            this.inputFormat = val;
            return this;
        }

        public Builder serdeLib(String val) {
            this.serdeLib = val;
            return this;
        }

        public Builder partitionKeyNames(List<String> val) {
            this.partitionKeyNames = val;
            return this;
        }

        public Builder tableParameters(Map<String, String> val) {
            this.tableParameters = val;
            return this;
        }

        public Builder prunedPartitionPaths(List<String> val) {
            this.prunedPartitionPaths = val;
            return this;
        }

        public HudiTableHandle build() {
            return new HudiTableHandle(this);
        }
    }
}
