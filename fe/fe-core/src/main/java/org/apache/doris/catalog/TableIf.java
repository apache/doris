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

package org.apache.doris.catalog;

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.statistics.analysis.AnalysisInfo;
import org.apache.doris.statistics.analysis.BaseAnalysisTask;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface TableIf {
    Logger LOG = LogManager.getLogger(TableIf.class);

    class TableStatusStats {
        private final long rows;
        private final long dataLength;
        private final long avgRowLength;
        private final long indexLength;

        public TableStatusStats(long rows, long dataLength, long avgRowLength, long indexLength) {
            this.rows = rows;
            this.dataLength = dataLength;
            this.avgRowLength = avgRowLength;
            this.indexLength = indexLength;
        }

        public long getRows() {
            return rows;
        }

        public long getDataLength() {
            return dataLength;
        }

        public long getAvgRowLength() {
            return avgRowLength;
        }

        public long getIndexLength() {
            return indexLength;
        }
    }

    long UNKNOWN_ROW_COUNT = -1;

    default void readLock() {
    }

    default boolean tryReadLock(long timeout, TimeUnit unit) {
        return true;
    }

    default void readUnlock() {
    }

    default void writeLock() {
    }

    default boolean writeLockIfExist() {
        return true;
    }

    default boolean tryWriteLock(long timeout, TimeUnit unit) {
        return true;
    }

    default void writeUnlock() {
    }

    default boolean isWriteLockHeldByCurrentThread() {
        return true;
    }

    default <E extends Exception> void writeLockOrException(E e) throws E {
    }

    default void writeLockOrDdlException() throws DdlException {
    }

    default void writeLockOrMetaException() throws MetaNotFoundException {
    }

    default void writeLockOrAlterCancelException() throws AlterCancelException {
    }

    default boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException {
        return true;
    }

    default <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E {
        return true;
    }

    default boolean tryWriteLockIfExist(long timeout, TimeUnit unit) {
        return true;
    }

    long getId();

    String getName();

    /**
     * The name a user sees in SQL. Same as {@link #getName()} except for a temporary table,
     * whose stored name is qualified with the id of the session that owns it.
     */
    default String getDisplayName() {
        return getName();
    }

    TableType getType();

    /**
     * Returns the table type name used in ENGINE= clause of SHOW CREATE TABLE.
     * By default this is the same as getType().name(); a plugin-driven table overrides it
     * with the engine name its connector declares, so that both places a user sees an
     * engine name agree.
     */
    default String getEngineTableTypeName() {
        return getType().name();
    }

    List<Column> getFullSchema();

    List<Column> getBaseSchema();

    default Set<Column> getSchemaAllIndexes(boolean full) {
        Set<Column> ret = Sets.newHashSet();
        ret.addAll(getBaseSchema());
        return ret;
    }

    default List<Column> getBaseSchemaOrEmpty() {
        try {
            return getBaseSchema();
        } catch (Exception e) {
            LOG.warn("failed to get base schema for table {}", getName(), e);
            return Lists.newArrayList();
        }
    }

    List<Column> getBaseSchema(boolean full);

    void setNewFullSchema(List<Column> newSchema);

    Column getColumn(String name);

    default int getBaseColumnIdxByName(String colName) {
        int i = 0;
        for (Column col : getBaseSchema()) {
            if (col.getName().equalsIgnoreCase(colName)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    String getMysqlType();

    String getEngine();

    String getComment();

    long getCreateTime();

    long getUpdateTime();

    long getRowCount();

    // Get the row count from cache,
    // If miss, just return 0
    // This is used for external table, because for external table, the fetching row count may be expensive
    long getCachedRowCount();

    long fetchRowCount();

    long getDataLength();

    long getAvgRowLength();

    long getIndexLength();

    default TableStatusStats getTableStatusStats() {
        return new TableStatusStats(getCachedRowCount(), getDataLength(), getAvgRowLength(), getIndexLength());
    }

    long getLastCheckTime();

    String getComment(boolean escapeQuota);

    TTableDescriptor toThrift();

    BaseAnalysisTask createAnalysisTask(AnalysisInfo info);

    DatabaseIf getDatabase();

    Optional<ColumnStatistic> getColumnStatistic(String colName);

    /**
     * @param columns Set of column names.
     * @return Set of pairs. Each pair is <IndexName, ColumnName>. For external table, index name is table name.
     */
    Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns);

    // Get all the chunk sizes of this table. Only a plugin-driven external table whose connector can list
    // file sizes implements this; the return result is a list of all the files' size.
    List<Long> getChunkSizes();

    void write(DataOutput out) throws IOException;

    /**
     * return true if this kind of table need read lock when doing query plan.
     *
     * @return
     */
    default boolean needReadLockWhenPlan() {
        return false;
    }

    /**
     * Returns whether the table can preload planning metadata before internal table locks are acquired.
     */
    default boolean supportsExternalMetadataPreload() {
        return false;
    }

    /**
     * Returns whether the table has a meaningful latest snapshot that can be preloaded ahead of analysis.
     */
    default boolean supportsLatestSnapshotPreload() {
        return false;
    }

    /**
     * Doris table type.
     *
     * <p>There is no per-data-source external table type: an external table served by a connector plugin is a
     * {@code PLUGIN_EXTERNAL_TABLE}, and the source's own name is answered by the connector
     * ({@code PluginDrivenExternalCatalog#getDisplayEngineName}), never by a mapping held here.</p>
     *
     * <p>An image written before the cutover still carries the old per-source names, and stays readable:
     * the persisted table class is remapped by {@code GsonUtils}' compatible-subtype registry, and the stale
     * {@code type} string deserializes to {@code null} (Gson returns null for an enum name it does not know,
     * it does not throw), which {@code PluginDrivenExternalTable#gsonPostProcess} then normalizes to
     * {@code PLUGIN_EXTERNAL_TABLE} — the same normalization it already applied to a recognized legacy name.</p>
     *
     * <p>The legacy internal-catalog {@code CREATE EXTERNAL TABLE ... ENGINE=iceberg|hudi} types are gone as
     * well: no {@code IcebergTable} / {@code HudiTable} class exists to carry them, and neither appears in the
     * 4.1.3 GSON table registry ({@code upgrade/413/labels.tbl.txt}), so no readable image can hold one.
     * {@code HIVE} stays because {@code HiveTable} is still registered there.</p>
     */
    enum TableType {
        MYSQL, ODBC, OLAP, SCHEMA, INLINE_VIEW, VIEW, BROKER, ELASTICSEARCH, HIVE, JDBC,
        TABLE_VALUED_FUNCTION, MATERIALIZED_VIEW, TEST_EXTERNAL_TABLE, DICTIONARY, DORIS_EXTERNAL_TABLE,
        PLUGIN_EXTERNAL_TABLE,
        STREAM;

        public String toEngineName() {
            switch (this) {
                case MYSQL:
                    return "MySQL";
                case ODBC:
                    return "Odbc";
                case OLAP:
                    return "Doris";
                case SCHEMA:
                    return "SYSTEM VIEW";
                case INLINE_VIEW:
                    return "InlineView";
                case VIEW:
                    return "View";
                case BROKER:
                    return "Broker";
                case ELASTICSEARCH:
                    return "ElasticSearch";
                case HIVE:
                    return "Hive";
                case JDBC:
                    return "jdbc";
                case TABLE_VALUED_FUNCTION:
                    return "Table_Valued_Function";
                case DICTIONARY:
                    return "dictionary";
                case DORIS_EXTERNAL_TABLE:
                    return "External_Doris";
                case PLUGIN_EXTERNAL_TABLE:
                    return "Plugin";
                case STREAM:
                    return "Stream";
                default:
                    return null;
            }
        }

        public TableType getParentType() {
            switch (this) {
                case MATERIALIZED_VIEW:
                    return OLAP;
                default:
                    return this;
            }
        }

        // Refer to https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html
        public String toMysqlType() {
            switch (this) {
                case SCHEMA:
                    return "SYSTEM VIEW";
                case INLINE_VIEW:
                case VIEW:
                case STREAM:
                    return "VIEW";
                case OLAP:
                case MYSQL:
                case ODBC:
                case BROKER:
                case ELASTICSEARCH:
                case HIVE:
                case JDBC:
                case TABLE_VALUED_FUNCTION:
                case MATERIALIZED_VIEW:
                case DORIS_EXTERNAL_TABLE:
                case PLUGIN_EXTERNAL_TABLE:
                    return "BASE TABLE";
                default:
                    return null;
            }
        }
    }

    default List<Column> getColumns() {
        return Collections.emptyList();
    }

    default Set<String> getPartitionNames() {
        return Collections.emptySet();
    }

    default Partition getPartition(String name) {
        return null;
    }

    default List<String> getFullQualifiers() {
        return ImmutableList.of(getDatabase().getCatalog().getName(),
                getDatabase().getFullName(),
                getName());
    }

    default String getNameWithFullQualifiers() {
        DatabaseIf db = getDatabase();
        // Some kind of table like FunctionGenTable does not belong to any database
        if (db == null) {
            return "null.null." + getName();
        } else {
            return db.getCatalog().getName()
                    + "." + db.getFullName()
                    + "." + getName();
        }
    }

    default boolean isManagedTable() {
        return getType() == TableType.OLAP || getType() == TableType.MATERIALIZED_VIEW;
    }

    default long getDataSize(boolean singleReplica) {
        // TODO: Each tableIf should impl it by itself.
        return 0;
    }

    default boolean isPartitionColumn(Column column) {
        return false;
    }

    default Set<String> getDistributionColumnNames() {
        return Sets.newHashSet();
    }

    default boolean isPartitionedTable() {
        return false;
    }

    boolean autoAnalyzeEnabled();

    TableIndexes getTableIndexes();

    default boolean isTemporary() {
        return false;
    }

    /**
     * Get the map of supported system table types for this table.
     * Key is the system table name (e.g., "snapshots", "partitions").
     *
     * @return map of system table name to SysTable
     */
    default Map<String, SysTable> getSupportedSysTables() {
        return Collections.emptyMap();
    }

    /**
     * Find the SysTable that matches the given table name.
     * Uses O(1) map lookup after extracting the system table name suffix.
     *
     * @param tableNameWithSysTableName e.g., "table$partitions"
     * @return the matching SysTable, or empty if not found
     */
    default Optional<SysTable> findSysTable(String tableNameWithSysTableName) {
        String sysTableName = SysTable.getTableNameWithSysTableName(tableNameWithSysTableName).second;
        if (sysTableName.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(getSupportedSysTables().get(sysTableName));
    }

    default void checkAsTableStreamBaseTable(BaseTableStream.StreamScanType streamScanType) throws DdlException {
        throw new DdlException("Base table type: " + getType()  + ", StreamScanType: " + streamScanType
                + " is not supported for create table stream");
    }
}
