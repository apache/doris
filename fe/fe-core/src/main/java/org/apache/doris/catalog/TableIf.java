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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
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

    TableType getType();

    /**
     * Returns the table type name used in ENGINE= clause of SHOW CREATE TABLE.
     * By default this is the same as getType().name(), but plugin-driven tables
     * override this to preserve the original engine name (e.g., JDBC_EXTERNAL_TABLE).
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

    // Get all the chunk sizes of this table. Now, only HMS external table implemented this interface.
    // For HMS external table, the return result is a list of all the files' size.
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
     * Doris table type.
     */
    enum TableType {
        MYSQL, ODBC, OLAP, SCHEMA, INLINE_VIEW, VIEW, BROKER, ELASTICSEARCH, HIVE,
        @Deprecated
        ICEBERG, @Deprecated
        HUDI, JDBC,
        TABLE_VALUED_FUNCTION, HMS_EXTERNAL_TABLE, ES_EXTERNAL_TABLE, MATERIALIZED_VIEW, JDBC_EXTERNAL_TABLE,
        ICEBERG_EXTERNAL_TABLE, TEST_EXTERNAL_TABLE, PAIMON_EXTERNAL_TABLE, MAX_COMPUTE_EXTERNAL_TABLE,
        HUDI_EXTERNAL_TABLE, TRINO_CONNECTOR_EXTERNAL_TABLE, LAKESOUl_EXTERNAL_TABLE, DICTIONARY, DORIS_EXTERNAL_TABLE,
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
                case ES_EXTERNAL_TABLE:
                    return "es";
                case HIVE:
                    return "Hive";
                case HUDI:
                case HUDI_EXTERNAL_TABLE:
                    return "Hudi";
                case JDBC:
                case JDBC_EXTERNAL_TABLE:
                    return "jdbc";
                case TABLE_VALUED_FUNCTION:
                    return "Table_Valued_Function";
                case HMS_EXTERNAL_TABLE:
                    return "hms";
                case ICEBERG:
                case ICEBERG_EXTERNAL_TABLE:
                    return "iceberg";
                case PAIMON_EXTERNAL_TABLE:
                    return "paimon";
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
                    return "VIEW";
                case OLAP:
                case MYSQL:
                case ODBC:
                case BROKER:
                case ELASTICSEARCH:
                case HIVE:
                case HUDI:
                case JDBC:
                case JDBC_EXTERNAL_TABLE:
                case TABLE_VALUED_FUNCTION:
                case HMS_EXTERNAL_TABLE:
                case ICEBERG_EXTERNAL_TABLE:
                case PAIMON_EXTERNAL_TABLE:
                case MATERIALIZED_VIEW:
                case TRINO_CONNECTOR_EXTERNAL_TABLE:
                case DORIS_EXTERNAL_TABLE:
                case PLUGIN_EXTERNAL_TABLE:
                case STREAM:
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
}
