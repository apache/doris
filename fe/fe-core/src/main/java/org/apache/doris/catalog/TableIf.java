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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.ImmutableList;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface TableIf {
    Logger LOG = LogManager.getLogger(TableIf.class);

    void readLock();

    boolean tryReadLock(long timeout, TimeUnit unit);

    void readUnlock();

    void writeLock();

    boolean writeLockIfExist();

    boolean tryWriteLock(long timeout, TimeUnit unit);

    void writeUnlock();

    boolean isWriteLockHeldByCurrentThread();

    <E extends Exception> void writeLockOrException(E e) throws E;

    void writeLockOrDdlException() throws DdlException;

    void writeLockOrMetaException() throws MetaNotFoundException;

    void writeLockOrAlterCancelException() throws AlterCancelException;

    boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException;

    <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E;

    boolean tryWriteLockIfExist(long timeout, TimeUnit unit);

    long getId();

    String getName();

    TableType getType();

    List<Column> getFullSchema();

    List<Column> getBaseSchema();

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

    // Get the exact number of rows in the internal table;
    // Get the number of cached rows or estimated rows in the external table, if not, return -1.
    long getCacheRowCount();

    long getDataLength();

    long getAvgRowLength();

    long getLastCheckTime();

    String getComment(boolean escapeQuota);

    TTableDescriptor toThrift();

    BaseAnalysisTask createAnalysisTask(AnalysisInfo info);

    long estimatedRowCount();

    DatabaseIf getDatabase();

    Optional<ColumnStatistic> getColumnStatistic(String colName);

    boolean needReAnalyzeTable(TableStatsMeta tblStats);

    Map<String, Set<String>> findReAnalyzeNeededPartitions();

    // Get all the chunk sizes of this table. Now, only HMS external table implemented this interface.
    // For HMS external table, the return result is a list of all the files' size.
    List<Long> getChunkSizes();

    void write(DataOutput out) throws IOException;

    // Don't use it outside due to its thread-unsafe, use get specific constraints instead.
    default Map<String, Constraint> getConstraintsMap() {
        throw new RuntimeException(String.format("Not implemented constraint for table %s. "
                + "And the function can't be called outside, consider get specific function "
                + "like getForeignKeyConstraints/getPrimaryKeyConstraints/getUniqueConstraints.", this));
    }

    default Set<ForeignKeyConstraint> getForeignKeyConstraints() {
        readLock();
        try {
            return getConstraintsMap().values().stream()
                    .filter(ForeignKeyConstraint.class::isInstance)
                    .map(ForeignKeyConstraint.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
        } catch (Exception ignored) {
            return ImmutableSet.of();
        } finally {
            readUnlock();
        }
    }

    default Set<PrimaryKeyConstraint> getPrimaryKeyConstraints() {
        readLock();
        try {
            return getConstraintsMap().values().stream()
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
            return getConstraintsMap().values().stream()
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
    default void checkConstraintNotExistence(String name, Constraint primaryKeyConstraint,
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

    default void addUniqueConstraint(String name, ImmutableList<String> columns) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMap();
            UniqueConstraint uniqueConstraint =  new UniqueConstraint(name, ImmutableSet.copyOf(columns));
            checkConstraintNotExistence(name, uniqueConstraint, constraintMap);
            constraintMap.put(name, uniqueConstraint);
        } finally {
            writeUnlock();
        }
    }

    default void addPrimaryKeyConstraint(String name, ImmutableList<String> columns) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMap();
            PrimaryKeyConstraint primaryKeyConstraint = new PrimaryKeyConstraint(name, ImmutableSet.copyOf(columns));
            checkConstraintNotExistence(name, primaryKeyConstraint, constraintMap);
            constraintMap.put(name, primaryKeyConstraint);
        } finally {
            writeUnlock();
        }
    }

    default void updatePrimaryKeyForForeignKey(PrimaryKeyConstraint requirePrimaryKey, TableIf referencedTable) {
        referencedTable.writeLock();
        try {
            Optional<Constraint> primaryKeyConstraint = referencedTable.getConstraintsMap().values().stream()
                    .filter(requirePrimaryKey::equals)
                    .findFirst();
            if (!primaryKeyConstraint.isPresent()) {
                throw new AnalysisException(String.format(
                        "Foreign key constraint requires a primary key constraint %s in %s",
                        requirePrimaryKey.getPrimaryKeyNames(), referencedTable.getName()));
            }
            ((PrimaryKeyConstraint) (primaryKeyConstraint.get())).addForeignTable(this);
        } finally {
            referencedTable.writeUnlock();
        }
    }

    default void addForeignConstraint(String name, ImmutableList<String> columns,
            TableIf referencedTable, ImmutableList<String> referencedColumns) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMap();
            ForeignKeyConstraint foreignKeyConstraint =
                    new ForeignKeyConstraint(name, columns, referencedTable, referencedColumns);
            checkConstraintNotExistence(name, foreignKeyConstraint, constraintMap);
            PrimaryKeyConstraint requirePrimaryKey = new PrimaryKeyConstraint(name,
                    foreignKeyConstraint.getReferencedColumnNames());
            updatePrimaryKeyForForeignKey(requirePrimaryKey, referencedTable);
            constraintMap.put(name, foreignKeyConstraint);
        } finally {
            writeUnlock();
        }
    }

    default void dropConstraint(String name) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMap();
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
        } finally {
            writeUnlock();
        }
    }

    default void dropFKReferringPK(TableIf table, PrimaryKeyConstraint constraint) {
        writeLock();
        try {
            Map<String, Constraint> constraintMap = getConstraintsMap();
            constraintMap.entrySet().removeIf(e -> e.getValue() instanceof ForeignKeyConstraint
                    && ((ForeignKeyConstraint) e.getValue()).isReferringPK(table, constraint));
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
        HUDI_EXTERNAL_TABLE;

        public String toEngineName() {
            switch (this) {
                case MYSQL:
                    return "MySQL";
                case ODBC:
                    return "Odbc";
                case OLAP:
                    return "Doris";
                case SCHEMA:
                    return "MEMORY";
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
                case ICEBERG_EXTERNAL_TABLE:
                    return "iceberg";
                case HUDI_EXTERNAL_TABLE:
                    return "hudi";
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

        public String toMysqlType() {
            switch (this) {
                case OLAP:
                    return "BASE TABLE";
                case SCHEMA:
                    return "SYSTEM VIEW";
                case INLINE_VIEW:
                case VIEW:
                case MATERIALIZED_VIEW:
                    return "VIEW";
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
                    return "EXTERNAL TABLE";
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

    default long getLastUpdateTime() {
        return -1L;
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
}

