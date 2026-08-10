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

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

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
 * <p>The column schema is deliberately NOT here — with one exception. It is the one part that a
 * statement re-reads through {@link FlussStatementScope}, so the handle stays a small, serializable
 * identity object; {@link #getKeyColumnTypes()} carries the types of the primary-key and partition-key
 * columns alone, because planning has to reason about those by type (see its javadoc) and re-reading
 * the whole schema for them would both cost a round trip and risk answering from a different schema
 * version than the rest of this handle describes.
 *
 * <p>{@link #getReadMode()} is the one field that is NOT a fact about the table: it says WHICH SEGMENT
 * of the table this handle stands for. A handle reached through {@code tbl} covers the whole table; one
 * reached through {@code tbl$log} covers only the part that is still in fluss's log — the complement of
 * what {@code tbl$lake} serves. The two segments partition the table, so the field belongs to identity
 * (see {@link #equals}): two handles that name the same table but different segments describe different
 * row sets.
 */
public class FlussTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    /**
     * Which segment of the table a handle stands for.
     *
     * <p>The names describe the data, not the freshness: {@link #LOG_ONLY} is "what is in the log", which
     * stays true however far behind tiering has fallen, whereas a name like "realtime" would be a lie the
     * moment tiering stops. Path selection — reading a whole table from fluss alone versus as its lake
     * plus its log — is a different question and is NOT expressed here; it is the catalog's (or the
     * session's) union-read mode.
     */
    public enum ReadMode {
        /** The whole table. */
        DEFAULT,
        /** Only the part not yet tiered into the lake: the log past the lake snapshot's offsets. */
        LOG_ONLY
    }

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
    private final Map<String, DataType> keyColumnTypes;
    private final ReadMode readMode;

    public FlussTableHandle(String databaseName, String tableName, long tableId, int schemaId,
            boolean hasPrimaryKey, List<String> primaryKeys, List<String> bucketKeys, int bucketCount,
            List<String> partitionKeys, boolean dataLakeEnabled, String dataLakeFormat,
            Map<String, String> properties, Map<String, DataType> keyColumnTypes) {
        this.readMode = ReadMode.DEFAULT;
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
        this.keyColumnTypes = keyColumnTypes == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(keyColumnTypes));
    }

    /**
     * Re-reads {@code source} at a different segment. The table facts are aliased rather than copied —
     * every one of them is already unmodifiable — so the two handles cannot drift apart into describing
     * the same table two different ways.
     */
    private FlussTableHandle(FlussTableHandle source, ReadMode readMode) {
        this.readMode = readMode;
        this.databaseName = source.databaseName;
        this.tableName = source.tableName;
        this.tableId = source.tableId;
        this.schemaId = source.schemaId;
        this.hasPrimaryKey = source.hasPrimaryKey;
        this.primaryKeys = source.primaryKeys;
        this.bucketKeys = source.bucketKeys;
        this.bucketCount = source.bucketCount;
        this.partitionKeys = source.partitionKeys;
        this.dataLakeEnabled = source.dataLakeEnabled;
        this.dataLakeFormat = source.dataLakeFormat;
        this.properties = source.properties;
        this.keyColumnTypes = source.keyColumnTypes;
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
                tableInfo.getProperties().toMap(),
                keyColumnTypes(tableInfo));
    }

    /**
     * The types of the columns that are part of the primary key or of the partition key, taken from the
     * same {@link TableInfo} as every other field here.
     */
    private static Map<String, DataType> keyColumnTypes(TableInfo tableInfo) {
        RowType rowType = tableInfo.getRowType();
        Map<String, DataType> types = new LinkedHashMap<>();
        List<String> keyColumns = new ArrayList<>(tableInfo.getPrimaryKeys());
        keyColumns.addAll(tableInfo.getPartitionKeys());
        for (String column : keyColumns) {
            int index = rowType.getFieldIndex(column);
            if (index >= 0) {
                types.put(column, rowType.getTypeAt(index));
            }
        }
        return types;
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

    public ReadMode getReadMode() {
        return readMode;
    }

    /** Whether this handle covers only the log past the lake snapshot, i.e. it was reached as {@code $log}. */
    public boolean isLogOnly() {
        return readMode == ReadMode.LOG_ONLY;
    }

    /** The same table, read as its log tail alone. */
    public FlussTableHandle asLogOnly() {
        return readMode == ReadMode.LOG_ONLY ? this : new FlussTableHandle(this, ReadMode.LOG_ONLY);
    }

    /**
     * The fluss types of the primary-key and partition-key columns, by column name.
     *
     * <p>Split planning needs these two, and only these two, by type. A primary-key table read as the
     * union of its lake and its log tail identifies rows across the two halves BY KEY, so a key column
     * whose values do not compare exactly the same way on both sides (a float, a timestamp Doris rounds)
     * cannot be read that way at all. A partition column is matched the same way one level up: a lake
     * split is bound to a fluss partition by comparing the rendered partition values, which is only
     * sound for a type both sides render identically.
     *
     * <p>Everything else about the schema stays out of the handle — the point is not "some of the
     * schema", it is the columns whose type decides how the table can be PLANNED.
     */
    public Map<String, DataType> getKeyColumnTypes() {
        return keyColumnTypes;
    }

    /** The primary-key columns that are not partition columns — what a bucket's rows are keyed by. */
    public List<String> getPhysicalPrimaryKeys() {
        List<String> physical = new ArrayList<>(primaryKeys);
        physical.removeAll(partitionKeys);
        return Collections.unmodifiableList(physical);
    }

    /**
     * Identity is the table, the schema version it was read at, and the segment it covers: two handles
     * for the same table at different schema versions describe different column sets, and two at
     * different {@link ReadMode read modes} describe different row sets. Neither may compare equal. The
     * remaining fields all derive from the table and its schema version.
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
                && readMode == that.readMode
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tableId, schemaId, readMode);
    }

    @Override
    public String toString() {
        return "FlussTableHandle{" + databaseName + "." + tableName
                + ", tableId=" + tableId + ", schemaId=" + schemaId
                + ", primaryKey=" + hasPrimaryKey + ", buckets=" + bucketCount
                + ", partitionKeys=" + partitionKeys
                + ", dataLake=" + (dataLakeEnabled ? dataLakeFormat : "disabled")
                + ", readMode=" + readMode + "}";
    }

    private static List<String> copyOf(List<String> values) {
        return values == null || values.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(values));
    }
}
