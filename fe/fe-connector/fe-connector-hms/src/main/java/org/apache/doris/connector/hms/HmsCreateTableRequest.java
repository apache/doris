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
 * Write-side spec describing a Hive-compatible table to create in the metastore.
 *
 * <p>The write-side counterpart of the read DTO {@link HmsTableInfo}. It is
 * SPI-clean (connector-api + JDK types only) and mirrors fe-core's
 * {@code HiveTableMetadata} without depending on any fe-core class. A connector
 * plugin assembles it from a CREATE TABLE request (resolving file format, owner,
 * location, bucketing and the text-compression default on its side) and hands it
 * to {@link HmsClient#createTable}.</p>
 *
 * <p>{@link #getColumns()} carries <b>all</b> columns (data + partition, in
 * declaration order); {@link #getPartitionKeys()} lists the names of the ones that
 * are partition keys. The write converter splits them apart, matching legacy
 * {@code HiveUtil.toHiveSchema}. Per-column default values ride on the
 * {@link ConnectorColumn#getDefaultValue()} of each column.</p>
 */
public final class HmsCreateTableRequest {

    private final String dbName;
    private final String tableName;
    private final String location;
    private final List<ConnectorColumn> columns;
    private final List<String> partitionKeys;
    private final List<String> bucketCols;
    private final int numBuckets;
    private final String fileFormat;
    private final String comment;
    private final Map<String, String> properties;
    private final String defaultTextCompression;
    private final String dorisVersion;

    private HmsCreateTableRequest(Builder b) {
        this.dbName = Objects.requireNonNull(b.dbName, "dbName");
        this.tableName = Objects.requireNonNull(b.tableName, "tableName");
        this.location = b.location;
        this.columns = b.columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.columns);
        this.partitionKeys = b.partitionKeys == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.partitionKeys);
        this.bucketCols = b.bucketCols == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(b.bucketCols);
        this.numBuckets = b.numBuckets;
        this.fileFormat = Objects.requireNonNull(b.fileFormat, "fileFormat");
        this.comment = b.comment;
        this.properties = b.properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(b.properties);
        this.defaultTextCompression = b.defaultTextCompression;
        this.dorisVersion = b.dorisVersion;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    /** Optional table location; {@code null} when the metastore should pick a default under the db. */
    public String getLocation() {
        return location;
    }

    /** All columns (data + partition keys) in declaration order. */
    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    /** Names of the columns that are partition keys (subset of {@link #getColumns()} names). */
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getBucketCols() {
        return bucketCols;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    /** File format string: one of "orc", "parquet", "text" (case-insensitive). */
    public String getFileFormat() {
        return fileFormat;
    }

    /** Table comment; never {@code null} (empty when unset). */
    public String getComment() {
        return comment == null ? "" : comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * The compression to fall back to for a {@code text} table when the user did not set a
     * {@code compression} property. The connector resolves it from its session (legacy read the
     * {@code hive_text_compression} session variable); {@code null} falls back to "plain".
     */
    public String getDefaultTextCompression() {
        return defaultTextCompression;
    }

    /**
     * The value stamped into the {@code doris.version} table parameter. The connector sources it from
     * its injected properties (fe-core has the build version; the plugin must not import it). When
     * {@code null}/empty the write converter omits the parameter.
     */
    public String getDorisVersion() {
        return dorisVersion;
    }

    @Override
    public String toString() {
        return "HmsCreateTableRequest{" + dbName + "." + tableName + "}";
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link HmsCreateTableRequest}.
     */
    public static final class Builder {
        private String dbName;
        private String tableName;
        private String location;
        private List<ConnectorColumn> columns;
        private List<String> partitionKeys;
        private List<String> bucketCols;
        private int numBuckets;
        private String fileFormat;
        private String comment;
        private Map<String, String> properties;
        private String defaultTextCompression;
        private String dorisVersion;

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

        public Builder location(String val) {
            this.location = val;
            return this;
        }

        public Builder columns(List<ConnectorColumn> val) {
            this.columns = val;
            return this;
        }

        public Builder partitionKeys(List<String> val) {
            this.partitionKeys = val;
            return this;
        }

        public Builder bucketCols(List<String> val) {
            this.bucketCols = val;
            return this;
        }

        public Builder numBuckets(int val) {
            this.numBuckets = val;
            return this;
        }

        public Builder fileFormat(String val) {
            this.fileFormat = val;
            return this;
        }

        public Builder comment(String val) {
            this.comment = val;
            return this;
        }

        public Builder properties(Map<String, String> val) {
            this.properties = val;
            return this;
        }

        public Builder defaultTextCompression(String val) {
            this.defaultTextCompression = val;
            return this;
        }

        public Builder dorisVersion(String val) {
            this.dorisVersion = val;
            return this;
        }

        public HmsCreateTableRequest build() {
            return new HmsCreateTableRequest(this);
        }
    }
}
