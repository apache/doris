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
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.AlterConstraintLog;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public interface TableIf {
    Logger LOG = LogManager.getLogger(TableIf.class);

    default void readLock() {
    }

    default boolean tryReadLock(long timeout, TimeUnit unit) {
        return true;
    }

    default void readUnlock() {
    }

    ;

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

    long getLastCheckTime();

    String getComment(boolean escapeQuota);

    TTableDescriptor toThrift();

    BaseAnalysisTask createAnalysisTask(AnalysisInfo info);

    // For empty table, nereids require getting 1 as row count. This is a wrap function for nereids to call getRowCount.
    default long getRowCountForNereids() {
        return Math.max(getRowCount(), 1);
    }

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

    // Don't use it outside due to its thread-unsafe, use get specific constraints instead.
    default Map<String, Constraint> getConstraintsMapUnsafe() {
        throw new RuntimeException(String.format("Not implemented constraint for table %s. "
                + "And the function can't be called outside, consider get specific function "
                + "like getForeignKeyConstraints/getPrimaryKeyConstraints/getUniqueConstraints.", this));
    }

    default Set<ForeignKeyConstraint> getForeignKeyConstraints() {
        readLock();
        try {
            return getConstraintsMapUnsafe().values().stream()
                    .filter(ForeignKeyConstraint.class::isInstance)
                    .map(ForeignKeyConstraint.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
        } catch (Exception ignored) {
            return ImmutableSet.of();
        } finally {
            readUnlock();
        }
    }

    default Map<String, Constraint> getConstraintsMap() {
        readLock();
        try {
            return ImmutableMap.copyOf(getConstraintsMapUnsafe());
        } catch (Exception ignored) {
            return ImmutableMap.of();
        } finally {
            readUnlock();
        }
    }

    default Set<PrimaryKeyConstraint> getPrimaryKeyConstraints() {
        readLock();
        try {
            return getConstraintsMapUnsafe().values().stream()
                    .filter(PrimaryKeyConstraint.class::isInstance)
                    .map(PrimaryKeyConstraint.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
        } catch (Exception ignored) {
            return ImmutableSet.of();
        } finally {
            readUnlock();
        }
    }

    default Set<UniqueConstraint> getUniqueConstraints() {
        readLock();
        try {
            return getConstraintsMapUnsafe().values().stream()
                    .filter(UniqueConstraint.class::isInstance)
                    .map(UniqueConstraint.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
        } catch (Exception ignored) {
            return ImmutableSet.of();
        } finally {
            readUnlock();
        }
    }

    // Note this function is not thread safe
    default void checkConstraintNotExistenceUnsafe(String name, Constraint primaryKeyConstraint,
            Map<String, Constraint> constraintMap) {
        if (constraintMap.containsKey(name)) {
            throw new RuntimeException(String.format("Constraint name %s has existed", name));
        }
        for (Map.Entry<String, Constraint> entry : constraintMap.entrySet()) {
            if (entry.getValue().equals(primaryKeyConstraint)) {
                throw new RuntimeException(String.format(
                        "Constraint %s has existed, named %s", primaryKeyConstraint, entry.getKey()));
            }
        }
    }

    default void addUniqueConstraint(String name, ImmutableList<String> columns, boolean replay) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMapUnsafe();
            UniqueConstraint uniqueConstraint =  new UniqueConstraint(name, ImmutableSet.copyOf(columns));
            checkConstraintNotExistenceUnsafe(name, uniqueConstraint, constraintMap);
            constraintMap.put(name, uniqueConstraint);
            if (!replay) {
                Env.getCurrentEnv().getEditLog().logAddConstraint(
                        new AlterConstraintLog(uniqueConstraint, this));
            }
        } finally {
            writeUnlock();
        }
    }

    default void addPrimaryKeyConstraint(String name, ImmutableList<String> columns, boolean replay) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMapUnsafe();
            PrimaryKeyConstraint primaryKeyConstraint = new PrimaryKeyConstraint(name, ImmutableSet.copyOf(columns));
            checkConstraintNotExistenceUnsafe(name, primaryKeyConstraint, constraintMap);
            constraintMap.put(name, primaryKeyConstraint);
            if (!replay) {
                Env.getCurrentEnv().getEditLog().logAddConstraint(
                        new AlterConstraintLog(primaryKeyConstraint, this));
            }
        } finally {
            writeUnlock();
        }
    }

    default PrimaryKeyConstraint tryGetPrimaryKeyForForeignKeyUnsafe(
            PrimaryKeyConstraint requirePrimaryKey, TableIf referencedTable) {
        Optional<Constraint> primaryKeyConstraint = referencedTable.getConstraintsMapUnsafe().values().stream()
                .filter(requirePrimaryKey::equals)
                .findFirst();
        if (!primaryKeyConstraint.isPresent()) {
            throw new AnalysisException(String.format(
                    "Foreign key constraint requires a primary key constraint %s in %s",
                    requirePrimaryKey.getPrimaryKeyNames(), referencedTable.getName()));
        }
        return ((PrimaryKeyConstraint) (primaryKeyConstraint.get()));
    }

    default void addForeignConstraint(String name, ImmutableList<String> columns,
            TableIf referencedTable, ImmutableList<String> referencedColumns, boolean replay) {
        writeLock();
        referencedTable.writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMapUnsafe();
            ForeignKeyConstraint foreignKeyConstraint =
                    new ForeignKeyConstraint(name, columns, referencedTable, referencedColumns);
            checkConstraintNotExistenceUnsafe(name, foreignKeyConstraint, constraintMap);
            PrimaryKeyConstraint requirePrimaryKeyName = new PrimaryKeyConstraint(name,
                    foreignKeyConstraint.getReferencedColumnNames());
            PrimaryKeyConstraint primaryKeyConstraint =
                    tryGetPrimaryKeyForForeignKeyUnsafe(requirePrimaryKeyName, referencedTable);
            primaryKeyConstraint.addForeignTable(this);
            constraintMap.put(name, foreignKeyConstraint);
            if (!replay) {
                Env.getCurrentEnv().getEditLog().logAddConstraint(
                        new AlterConstraintLog(foreignKeyConstraint, this));
            }
        } finally {
            referencedTable.writeUnlock();
            writeUnlock();
        }
    }

    default void replayAddConstraint(Constraint constraint) {
        // Since constraints are not indispensable, we only log when replay fails
        try {
            if (constraint instanceof UniqueConstraint) {
                UniqueConstraint uniqueConstraint = (UniqueConstraint) constraint;
                this.addUniqueConstraint(constraint.getName(),
                        ImmutableList.copyOf(uniqueConstraint.getUniqueColumnNames()), true);
            } else if (constraint instanceof PrimaryKeyConstraint) {
                PrimaryKeyConstraint primaryKeyConstraint = (PrimaryKeyConstraint) constraint;
                this.addPrimaryKeyConstraint(primaryKeyConstraint.getName(),
                        ImmutableList.copyOf(primaryKeyConstraint.getPrimaryKeyNames()), true);
            } else if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) constraint;
                this.addForeignConstraint(foreignKey.getName(),
                        ImmutableList.copyOf(foreignKey.getForeignKeyNames()),
                        foreignKey.getReferencedTable(),
                        ImmutableList.copyOf(foreignKey.getReferencedColumnNames()), true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    default void replayDropConstraint(String name) {
        try {
            dropConstraint(name, true);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    default void dropConstraint(String name, boolean replay) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMapUnsafe();
            if (!constraintMap.containsKey(name)) {
                throw new AnalysisException(
                        String.format("Unknown constraint %s on table %s.", name, this.getName()));
            }
            Constraint constraint = constraintMap.get(name);
            constraintMap.remove(name);
            if (constraint instanceof PrimaryKeyConstraint) {
                ((PrimaryKeyConstraint) constraint).getForeignTables()
                        .forEach(t -> t.dropFKReferringPK(this, (PrimaryKeyConstraint) constraint));
            }
            if (!replay) {
                Env.getCurrentEnv().getEditLog().logDropConstraint(new AlterConstraintLog(constraint, this));
            }
        } finally {
            writeUnlock();
        }
    }

    default void dropFKReferringPK(TableIf table, PrimaryKeyConstraint constraint) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMapUnsafe();
            Set<String> fkName = constraintMap.entrySet().stream()
                    .filter(e -> e.getValue() instanceof ForeignKeyConstraint
                    && ((ForeignKeyConstraint) e.getValue()).isReferringPK(table, constraint))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
            fkName.forEach(constraintMap::remove);
        } finally {
            writeUnlock();
        }
    }

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
        MYSQL, ODBC, OLAP, SCHEMA, INLINE_VIEW, VIEW, BROKER, ELASTICSEARCH, HIVE, ICEBERG, @Deprecated HUDI, JDBC,
        TABLE_VALUED_FUNCTION, HMS_EXTERNAL_TABLE, ES_EXTERNAL_TABLE, MATERIALIZED_VIEW, JDBC_EXTERNAL_TABLE,
        ICEBERG_EXTERNAL_TABLE, TEST_EXTERNAL_TABLE, PAIMON_EXTERNAL_TABLE, MAX_COMPUTE_EXTERNAL_TABLE,
        HUDI_EXTERNAL_TABLE, TRINO_CONNECTOR_EXTERNAL_TABLE, LAKESOUl_EXTERNAL_TABLE;

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
                case ES_EXTERNAL_TABLE:
                    return "es";
                case ICEBERG:
                case ICEBERG_EXTERNAL_TABLE:
                    return "iceberg";
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
                case MATERIALIZED_VIEW:
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
                case ES_EXTERNAL_TABLE:
                case ICEBERG_EXTERNAL_TABLE:
                case PAIMON_EXTERNAL_TABLE:
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
                ClusterNamespace.getNameFromFullName(getDatabase().getFullName()),
                getName());
    }

    default String getNameWithFullQualifiers() {
        return String.format("%s.%s.%s", getDatabase().getCatalog().getName(),
                ClusterNamespace.getNameFromFullName(getDatabase().getFullName()),
                getName());
    }

    default boolean isManagedTable() {
        return getType() == TableType.OLAP || getType() == TableType.MATERIALIZED_VIEW;
    }

    default long getDataSize(boolean singleReplica) {
        // TODO: Each tableIf should impl it by itself.
        return 0;
    }

    default boolean isDistributionColumn(String columnName) {
        return false;
    }

    default boolean isPartitionColumn(String columnName) {
        return false;
    }

    default Set<String> getDistributionColumnNames() {
        return Sets.newHashSet();
    }

    default boolean isPartitionedTable() {
        return false;
    }
}
