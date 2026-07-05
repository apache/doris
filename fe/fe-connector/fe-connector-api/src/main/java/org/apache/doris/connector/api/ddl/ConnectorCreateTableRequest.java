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

package org.apache.doris.connector.api.ddl;

import org.apache.doris.connector.api.ConnectorColumn;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Full {@code CREATE TABLE} payload passed to
 * {@code ConnectorTableOps.createTable(session, request)}.
 *
 * <p>Carries partition / bucket / external / {@code IF NOT EXISTS} information
 * absent from the legacy
 * {@code createTable(session, ConnectorTableSchema, Map<String,String>)}
 * signature.</p>
 *
 * <p>{@code partitionSpec} and {@code bucketSpec} are nullable when the
 * underlying DDL omits them.</p>
 */
public final class ConnectorCreateTableRequest {

    private final String dbName;
    private final String tableName;
    private final List<ConnectorColumn> columns;
    private final ConnectorPartitionSpec partitionSpec;
    private final ConnectorBucketSpec bucketSpec;
    private final List<ConnectorSortField> sortOrder;
    private final String comment;
    private final Map<String, String> properties;
    private final boolean ifNotExists;
    private final boolean external;

    private ConnectorCreateTableRequest(Builder b) {
        this.dbName = Objects.requireNonNull(b.dbName, "dbName");
        this.tableName = Objects.requireNonNull(b.tableName, "tableName");
        this.columns = b.columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.columns);
        this.partitionSpec = b.partitionSpec;
        this.bucketSpec = b.bucketSpec;
        this.sortOrder = b.sortOrder == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.sortOrder);
        this.comment = b.comment;
        this.properties = b.properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(b.properties);
        this.ifNotExists = b.ifNotExists;
        this.external = b.external;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    /** @return partition spec, or {@code null} for non-partitioned tables. */
    public ConnectorPartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    /** @return bucket spec, or {@code null} when no bucketing is declared. */
    public ConnectorBucketSpec getBucketSpec() {
        return bucketSpec;
    }

    /**
     * @return the {@code ORDER BY (...)} write-order fields (never {@code null}; empty when the DDL
     *         omits an ORDER BY clause or the engine drops it). Iceberg builds a write sort order from
     *         these; engines without a write order ignore them.
     */
    public List<ConnectorSortField> getSortOrder() {
        return sortOrder;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isExternal() {
        return external;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ConnectorCreateTableRequest{" + dbName + "." + tableName
                + ", cols=" + columns.size()
                + ", partition=" + partitionSpec
                + ", bucket=" + bucketSpec
                + ", external=" + external
                + ", ifNotExists=" + ifNotExists + "}";
    }

    public static final class Builder {
        private String dbName;
        private String tableName;
        private List<ConnectorColumn> columns;
        private ConnectorPartitionSpec partitionSpec;
        private ConnectorBucketSpec bucketSpec;
        private List<ConnectorSortField> sortOrder;
        private String comment;
        private Map<String, String> properties;
        private boolean ifNotExists;
        private boolean external;

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder columns(List<ConnectorColumn> columns) {
            this.columns = columns;
            return this;
        }

        public Builder partitionSpec(ConnectorPartitionSpec partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder bucketSpec(ConnectorBucketSpec bucketSpec) {
            this.bucketSpec = bucketSpec;
            return this;
        }

        public Builder sortOrder(List<ConnectorSortField> sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            // copy to preserve caller's map identity and keep insertion order
            this.properties = properties == null
                    ? null
                    : new LinkedHashMap<>(properties);
            return this;
        }

        public Builder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;
            return this;
        }

        public Builder external(boolean external) {
            this.external = external;
            return this;
        }

        public ConnectorCreateTableRequest build() {
            return new ConnectorCreateTableRequest(this);
        }
    }
}
