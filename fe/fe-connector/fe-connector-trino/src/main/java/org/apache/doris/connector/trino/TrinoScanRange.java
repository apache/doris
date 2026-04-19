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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scan range for Trino connectors. Carries JSON-serialized Trino SPI objects
 * in its properties map, which BE deserializes on the JNI side.
 *
 * <p>Property keys match the fields of {@code TTrinoConnectorFileDesc} in
 * the Thrift definition (PlanNodes.thrift).</p>
 */
public class TrinoScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    /** Property key for JSON-serialized ConnectorSplit. */
    public static final String KEY_SPLIT = "trino_connector_split";
    /** Property key for catalog name. */
    public static final String KEY_CATALOG_NAME = "catalog_name";
    /** Property key for database name. */
    public static final String KEY_DB_NAME = "db_name";
    /** Property key for table name. */
    public static final String KEY_TABLE_NAME = "table_name";
    /** Property key for JSON-serialized catalog options map. */
    public static final String KEY_OPTIONS = "trino_connector_options";
    /** Property key for JSON-serialized ConnectorTableHandle. */
    public static final String KEY_TABLE_HANDLE = "trino_connector_table_handle";
    /** Property key for JSON-serialized List of ColumnHandle. */
    public static final String KEY_COLUMN_HANDLES = "trino_connector_column_handles";
    /** Property key for JSON-serialized column metadata. */
    public static final String KEY_COLUMN_METADATA = "trino_connector_column_metadata";
    /** Property key for JSON-serialized ConnectorTransactionHandle. */
    public static final String KEY_TRANSACTION_HANDLE = "trino_connector_trascation_handle";

    private final Map<String, String> properties;
    private final List<String> hosts;

    private TrinoScanRange(Map<String, String> properties, List<String> hosts) {
        this.properties = Collections.unmodifiableMap(properties);
        this.hosts = hosts;
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public String getTableFormatType() {
        return "trino_connector";
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public List<String> getHosts() {
        return hosts;
    }

    /**
     * Builder for constructing TrinoScanRange instances.
     */
    public static class Builder {
        private final Map<String, String> props = new HashMap<>();
        private List<String> hosts = Collections.emptyList();

        public Builder split(String json) {
            props.put(KEY_SPLIT, json);
            return this;
        }

        public Builder catalogName(String name) {
            props.put(KEY_CATALOG_NAME, name);
            return this;
        }

        public Builder dbName(String name) {
            props.put(KEY_DB_NAME, name);
            return this;
        }

        public Builder tableName(String name) {
            props.put(KEY_TABLE_NAME, name);
            return this;
        }

        public Builder options(String json) {
            props.put(KEY_OPTIONS, json);
            return this;
        }

        public Builder tableHandle(String json) {
            props.put(KEY_TABLE_HANDLE, json);
            return this;
        }

        public Builder columnHandles(String json) {
            props.put(KEY_COLUMN_HANDLES, json);
            return this;
        }

        public Builder columnMetadata(String json) {
            props.put(KEY_COLUMN_METADATA, json);
            return this;
        }

        public Builder transactionHandle(String json) {
            props.put(KEY_TRANSACTION_HANDLE, json);
            return this;
        }

        public Builder hosts(List<String> hosts) {
            this.hosts = hosts;
            return this;
        }

        public TrinoScanRange build() {
            return new TrinoScanRange(props, hosts);
        }
    }
}
