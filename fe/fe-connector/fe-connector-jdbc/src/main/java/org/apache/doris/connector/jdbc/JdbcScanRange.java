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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A single JDBC scan range representing one remote query execution.
 *
 * <p>JDBC scans always produce exactly one scan range per query since the
 * remote query cannot be partitioned. The properties map carries all the
 * JDBC connection parameters and the SQL query needed by BE's JdbcJniReader.</p>
 */
public class JdbcScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    private static final String TABLE_FORMAT_TYPE = "jdbc";

    private final Map<String, String> properties;

    private JdbcScanRange(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of("jdbc://virtual");
    }

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT_TYPE;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Builder for constructing a JdbcScanRange with all required JDBC parameters.
     */
    public static class Builder {
        private final Map<String, String> props = new LinkedHashMap<>();

        public Builder querySql(String sql) {
            props.put("query_sql", sql);
            return this;
        }

        public Builder jdbcUrl(String url) {
            props.put("jdbc_url", url);
            return this;
        }

        public Builder jdbcUser(String user) {
            props.put("jdbc_user", user);
            return this;
        }

        public Builder jdbcPassword(String password) {
            props.put("jdbc_password", password);
            return this;
        }

        public Builder driverClass(String cls) {
            props.put("jdbc_driver_class", cls);
            return this;
        }

        public Builder driverUrl(String url) {
            props.put("jdbc_driver_url", url);
            return this;
        }

        public Builder driverChecksum(String checksum) {
            props.put("jdbc_driver_checksum", checksum != null ? checksum : "");
            return this;
        }

        public Builder catalogId(long id) {
            props.put("catalog_id", String.valueOf(id));
            return this;
        }

        public Builder tableType(JdbcDbType dbType) {
            props.put("table_type", dbType.getLabel());
            return this;
        }

        public Builder connectionPoolMinSize(int size) {
            props.put("connection_pool_min_size", String.valueOf(size));
            return this;
        }

        public Builder connectionPoolMaxSize(int size) {
            props.put("connection_pool_max_size", String.valueOf(size));
            return this;
        }

        public Builder connectionPoolMaxWaitTime(int ms) {
            props.put("connection_pool_max_wait_time", String.valueOf(ms));
            return this;
        }

        public Builder connectionPoolMaxLifeTime(int ms) {
            props.put("connection_pool_max_life_time", String.valueOf(ms));
            return this;
        }

        public Builder connectionPoolKeepAlive(boolean keepAlive) {
            props.put("connection_pool_keep_alive", keepAlive ? "true" : "false");
            return this;
        }

        public JdbcScanRange build() {
            return new JdbcScanRange(props);
        }
    }
}
