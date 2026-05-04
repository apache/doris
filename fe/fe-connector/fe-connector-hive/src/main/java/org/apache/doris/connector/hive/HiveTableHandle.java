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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.hms.HmsPartitionInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Opaque table handle for a Hive table, carrying the database and table
 * coordinates, detected table format type, and scan-related metadata
 * populated by {@link HiveConnectorMetadata#getTableHandle}.
 *
 * <p>After {@code applyFilter}, a new handle may carry
 * {@link #getPrunedPartitions()} for partition-pruned scans.</p>
 */
public class HiveTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;
    private final HiveTableType tableType;

    // Scan-related metadata (populated from HmsTableInfo)
    private final String inputFormat;
    private final String serializationLib;
    private final String location;
    private final List<String> partitionKeyNames;
    private final Map<String, String> sdParameters;
    private final Map<String, String> tableParameters;

    // Set after applyFilter for partition pruning
    private final List<HmsPartitionInfo> prunedPartitions;

    private HiveTableHandle(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.tableType = builder.tableType;
        this.inputFormat = builder.inputFormat;
        this.serializationLib = builder.serializationLib;
        this.location = builder.location;
        this.partitionKeyNames = builder.partitionKeyNames != null
                ? Collections.unmodifiableList(builder.partitionKeyNames)
                : Collections.emptyList();
        this.sdParameters = builder.sdParameters != null
                ? Collections.unmodifiableMap(builder.sdParameters)
                : Collections.emptyMap();
        this.tableParameters = builder.tableParameters != null
                ? Collections.unmodifiableMap(builder.tableParameters)
                : Collections.emptyMap();
        this.prunedPartitions = builder.prunedPartitions;
    }

    /** Legacy constructor for Phase 1 compatibility (metadata-only). */
    public HiveTableHandle(String dbName, String tableName, HiveTableType tableType) {
        this(new Builder(dbName, tableName, tableType));
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public HiveTableType getTableType() {
        return tableType;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    public String getLocation() {
        return location;
    }

    public List<String> getPartitionKeyNames() {
        return partitionKeyNames;
    }

    public Map<String, String> getSdParameters() {
        return sdParameters;
    }

    public Map<String, String> getTableParameters() {
        return tableParameters;
    }

    public List<HmsPartitionInfo> getPrunedPartitions() {
        return prunedPartitions;
    }

    /** Returns a builder pre-populated with this handle's state, for creating modified copies. */
    public Builder toBuilder() {
        Builder b = new Builder(dbName, tableName, tableType);
        b.inputFormat = this.inputFormat;
        b.serializationLib = this.serializationLib;
        b.location = this.location;
        b.partitionKeyNames = this.partitionKeyNames;
        b.sdParameters = this.sdParameters;
        b.tableParameters = this.tableParameters;
        b.prunedPartitions = this.prunedPartitions;
        return b;
    }

    @Override
    public String toString() {
        return "HiveTableHandle{" + dbName + "." + tableName + ", type=" + tableType + "}";
    }

    /**
     * Builder for constructing HiveTableHandle with scan metadata.
     */
    public static final class Builder {
        private final String dbName;
        private final String tableName;
        private final HiveTableType tableType;
        private String inputFormat;
        private String serializationLib;
        private String location;
        private List<String> partitionKeyNames;
        private Map<String, String> sdParameters;
        private Map<String, String> tableParameters;
        private List<HmsPartitionInfo> prunedPartitions;

        public Builder(String dbName, String tableName, HiveTableType tableType) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.tableType = tableType;
        }

        public Builder inputFormat(String val) {
            this.inputFormat = val;
            return this;
        }

        public Builder serializationLib(String val) {
            this.serializationLib = val;
            return this;
        }

        public Builder location(String val) {
            this.location = val;
            return this;
        }

        public Builder partitionKeyNames(List<String> val) {
            this.partitionKeyNames = val;
            return this;
        }

        public Builder sdParameters(Map<String, String> val) {
            this.sdParameters = val;
            return this;
        }

        public Builder tableParameters(Map<String, String> val) {
            this.tableParameters = val;
            return this;
        }

        public Builder prunedPartitions(List<HmsPartitionInfo> val) {
            this.prunedPartitions = val;
            return this;
        }

        public HiveTableHandle build() {
            return new HiveTableHandle(this);
        }
    }
}
