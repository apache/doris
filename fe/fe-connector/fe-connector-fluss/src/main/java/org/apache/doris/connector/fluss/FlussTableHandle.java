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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A fluss table, as everything downstream of {@code getTableHandle} needs to know about it.
 *
 * <p>The fields are the ones split planning reads, snapshotted from the {@link TableInfo} the handle
 * was built from: whether the table has a primary key (which of the two scanner families reads it),
 * the bucket count and keys (how many splits and how they line up with the lake's), the partition keys,
 * and whether the table is tiered into a lake at all. They are copied rather than looked up again
 * because planning must see one coherent view of the table, not a field from before an ALTER and a
 * field from after it.
 *
 * <p>{@link #getProperties()} is the fluss table's own property map, and it carries more than the user
 * wrote: for a datalake-enabled table the fluss coordinator merges the cluster's lake-catalog
 * connection settings into it under {@code table.datalake.<format>.*}, which is where the lake side of
 * a union read gets its catalog configuration. Nothing else in Doris supplies it — the Doris catalog is
 * configured with fluss bootstrap servers only.
 *
 * <p>The column schema is deliberately NOT here. It is the one part that a statement re-reads through
 * {@link FlussStatementScope}, so the handle stays a small, serializable identity object.
 */
public class FlussTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final long tableId;
    private final int schemaId;
    private final boolean hasPrimaryKey;
    private final List<String> primaryKeys;
    private final List<String> bucketKeys;
    private final int bucketCount;
    private final List<String> partitionKeys;
    private final boolean dataLakeEnabled;
    /** The lake format's fluss name ({@code "paimon"}), or {@code null} when the table declares none. */
    private final String dataLakeFormat;
    private final Map<String, String> properties;

    public FlussTableHandle(String databaseName, String tableName, long tableId, int schemaId,
            boolean hasPrimaryKey, List<String> primaryKeys, List<String> bucketKeys, int bucketCount,
            List<String> partitionKeys, boolean dataLakeEnabled, String dataLakeFormat,
            Map<String, String> properties) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.hasPrimaryKey = hasPrimaryKey;
        this.primaryKeys = copyOf(primaryKeys);
        this.bucketKeys = copyOf(bucketKeys);
        this.bucketCount = bucketCount;
        this.partitionKeys = copyOf(partitionKeys);
        this.dataLakeEnabled = dataLakeEnabled;
        this.dataLakeFormat = dataLakeFormat;
        this.properties = properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /** Snapshots {@code tableInfo} into a handle. */
    public static FlussTableHandle of(TableInfo tableInfo) {
        TablePath path = tableInfo.getTablePath();
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        return new FlussTableHandle(
                path.getDatabaseName(),
                path.getTableName(),
                tableInfo.getTableId(),
                tableInfo.getSchemaId(),
                tableInfo.hasPrimaryKey(),
                tableInfo.getPrimaryKeys(),
                tableInfo.getBucketKeys(),
                tableInfo.getNumBuckets(),
                tableInfo.getPartitionKeys(),
                tableInfo.getTableConfig().isDataLakeEnabled(),
                lakeFormat == null ? null : lakeFormat.toString(),
                tableInfo.getProperties().toMap());
    }

    public TablePath toTablePath() {
        return TablePath.of(databaseName, tableName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableId() {
        return tableId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public boolean hasPrimaryKey() {
        return hasPrimaryKey;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    public boolean isDataLakeEnabled() {
        return dataLakeEnabled;
    }

    public String getDataLakeFormat() {
        return dataLakeFormat;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Identity is the table plus the schema version it was read at: two handles for the same table at
     * different schema versions describe different column sets and must not compare equal. The
     * remaining fields all derive from that pair.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlussTableHandle)) {
            return false;
        }
        FlussTableHandle that = (FlussTableHandle) o;
        return tableId == that.tableId
                && schemaId == that.schemaId
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tableId, schemaId);
    }

    @Override
    public String toString() {
        return "FlussTableHandle{" + databaseName + "." + tableName
                + ", tableId=" + tableId + ", schemaId=" + schemaId
                + ", primaryKey=" + hasPrimaryKey + ", buckets=" + bucketCount
                + ", partitionKeys=" + partitionKeys
                + ", dataLake=" + (dataLakeEnabled ? dataLakeFormat : "disabled") + "}";
    }

    private static List<String> copyOf(List<String> values) {
        return values == null || values.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(values));
    }
}
