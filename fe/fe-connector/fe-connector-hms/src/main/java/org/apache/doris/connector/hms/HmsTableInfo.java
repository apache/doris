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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorColumn;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HMS table metadata DTO using connector-SPI types.
 *
 * <p>Contains all metadata needed by connector plugins to process
 * a Hive-compatible table without depending on fe-core types.</p>
 */
public final class HmsTableInfo {

    private final String dbName;
    private final String tableName;
    private final String tableType;
    private final String location;
    private final String inputFormat;
    private final String outputFormat;
    private final String serializationLib;
    private final List<ConnectorColumn> columns;
    private final List<ConnectorColumn> partitionKeys;
    private final Map<String, String> parameters;
    private final Map<String, String> sdParameters;

    private HmsTableInfo(Builder builder) {
        this.dbName = Objects.requireNonNull(builder.dbName, "dbName");
        this.tableName = Objects.requireNonNull(builder.tableName, "tableName");
        this.tableType = builder.tableType;
        this.location = builder.location;
        this.inputFormat = builder.inputFormat;
        this.outputFormat = builder.outputFormat;
        this.serializationLib = builder.serializationLib;
        this.columns = builder.columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(builder.columns);
        this.partitionKeys = builder.partitionKeys == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(builder.partitionKeys);
        this.parameters = builder.parameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.parameters);
        this.sdParameters = builder.sdParameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.sdParameters);
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    /** e.g. "MANAGED_TABLE", "EXTERNAL_TABLE", "VIRTUAL_VIEW". */
    public String getTableType() {
        return tableType;
    }

    public String getLocation() {
        return location;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    /** Data columns (excludes partition keys). */
    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    /** Partition key columns. */
    public List<ConnectorColumn> getPartitionKeys() {
        return partitionKeys;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    /** SerDe / StorageDescriptor parameters (field.delim, serialization.format, etc.). */
    public Map<String, String> getSdParameters() {
        return sdParameters;
    }

    @Override
    public String toString() {
        return "HmsTableInfo{" + dbName + "." + tableName
                + ", type=" + tableType + "}";
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for HmsTableInfo.
     */
    public static final class Builder {
        private String dbName;
        private String tableName;
        private String tableType;
        private String location;
        private String inputFormat;
        private String outputFormat;
        private String serializationLib;
        private List<ConnectorColumn> columns;
        private List<ConnectorColumn> partitionKeys;
        private Map<String, String> parameters;
        private Map<String, String> sdParameters;

        private Builder() {
        }

        public Builder dbName(String val) {
            this.dbName = val;
            return this;
        }

        public Builder tableName(String val) {
            this.tableName = val;
            return this;
        }

        public Builder tableType(String val) {
            this.tableType = val;
            return this;
        }

        public Builder location(String val) {
            this.location = val;
            return this;
        }

        public Builder inputFormat(String val) {
            this.inputFormat = val;
            return this;
        }

        public Builder outputFormat(String val) {
            this.outputFormat = val;
            return this;
        }

        public Builder serializationLib(String val) {
            this.serializationLib = val;
            return this;
        }

        public Builder columns(List<ConnectorColumn> val) {
            this.columns = val;
            return this;
        }

        public Builder partitionKeys(List<ConnectorColumn> val) {
            this.partitionKeys = val;
            return this;
        }

        public Builder parameters(Map<String, String> val) {
            this.parameters = val;
            return this;
        }

        public Builder sdParameters(Map<String, String> val) {
            this.sdParameters = val;
            return this;
        }

        public HmsTableInfo build() {
            return new HmsTableInfo(this);
        }
    }
}
