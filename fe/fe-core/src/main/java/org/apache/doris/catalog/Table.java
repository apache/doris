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
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.QueryableReentrantReadWriteLock;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.external.hudi.HudiTable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public abstract class Table extends MetaObject implements Writable, TableIf {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    // empirical value.
    // assume that the time a lock is held by thread is less then 100ms
    public static final long TRY_LOCK_TIMEOUT_MS = 100L;

    public volatile boolean isDropped = false;

    private boolean hasCompoundKey = false;
    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected volatile String name;
    protected volatile String qualifiedDbName;
    @SerializedName(value = "type")
    protected TableType type;
    @SerializedName(value = "createTime")
    protected long createTime;
    protected QueryableReentrantReadWriteLock rwLock;

    /*
     *  fullSchema and nameToColumn should contains all columns, both visible and shadow.
     *  eg. for OlapTable, when doing schema change, there will be some shadow columns which are not visible
     *      to query but visible to load process.
     *  If you want to get all visible columns, you should call getBaseSchema() method, which is override in
     *  sub classes.
     *
     *  NOTICE: the order of this fullSchema is meaningless to OlapTable
     */
    /**
     * The fullSchema of OlapTable includes the base columns and the SHADOW_NAME_PREFIX columns.
     * The properties of base columns in fullSchema are same as properties in baseIndex.
     * For example:
     * Table (c1 int, c2 int, c3 int)
     * Schema change (c3 to bigint)
     * When OlapTable is changing schema, the fullSchema is (c1 int, c2 int, c3 int, SHADOW_NAME_PRFIX_c3 bigint)
     * The fullSchema of OlapTable is mainly used by Scanner of Load job.
     * <p>
     * If you want to get the mv columns, you should call getIndexToSchema in Subclass OlapTable.
     */
    @SerializedName(value = "fullSchema")
    protected List<Column> fullSchema;
    // tree map for case-insensitive lookup.
    /**
     * The nameToColumn of OlapTable includes the base columns and the SHADOW_NAME_PREFIX columns.
     */
    protected Map<String, Column> nameToColumn;

    // DO NOT persist this variable.
    protected boolean isTypeRead = false;
    // table(view)'s comment
    @SerializedName(value = "comment")
    protected String comment = "";
    // sql for creating this table, default is "";
    protected String ddlSql = "";

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        this.rwLock = new QueryableReentrantReadWriteLock(true);
    }

    public Table(long id, String tableName, TableType type, List<Column> fullSchema) {
        this.id = id;
        this.name = tableName;
        this.type = type;
        // must copy the list, it should not be the same object as in indexIdToSchema
        if (fullSchema != null) {
            this.fullSchema = Lists.newArrayList(fullSchema);
        }
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (this.fullSchema != null) {
            for (Column col : this.fullSchema) {
                nameToColumn.put(col.getDefineName(), col);
            }
        } else {
            // Only view in with-clause have null base
            Preconditions.checkArgument(type == TableType.VIEW, "Table has no columns");
        }
        this.rwLock = new QueryableReentrantReadWriteLock(true);
        this.createTime = Instant.now().getEpochSecond();
    }

    public void markDropped() {
        isDropped = true;
    }

    public void unmarkDropped() {
        isDropped = false;
    }

    public void readLock() {
        this.rwLock.readLock().lock();
    }

    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            boolean res = this.rwLock.readLock().tryLock(timeout, unit);
            if (!res && unit.toSeconds(timeout) >= 1) {
                LOG.warn("Failed to try table {}'s read lock. timeout {} {}. Current owner: {}",
                        name, timeout, unit.name(), rwLock.getOwner());
            }
            return res;
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at table[" + name + "]", e);
            return false;
        }
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    public boolean writeLockIfExist() {
        this.rwLock.writeLock().lock();
        if (isDropped) {
            this.rwLock.writeLock().unlock();
            return false;
        }
        return true;
    }

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            boolean res = this.rwLock.writeLock().tryLock(timeout, unit);
            if (!res && unit.toSeconds(timeout) >= 1) {
                LOG.warn("Failed to try table {}'s write lock. timeout {} {}. Current owner: {}",
                        name, timeout, unit.name(), rwLock.getOwner());
            }
            return res;
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at table[" + name + "]", e);
            return false;
        }
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
        if (isDropped) {
            writeUnlock();
            throw e;
        }
    }

    public void writeLockOrDdlException() throws DdlException {
        writeLockOrException(new DdlException("unknown table, tableName=" + name));
    }

    public void writeLockOrMetaException() throws MetaNotFoundException {
        writeLockOrException(new MetaNotFoundException("unknown table, tableName=" + name));
    }

    public void writeLockOrAlterCancelException() throws AlterCancelException {
        writeLockOrException(new AlterCancelException("unknown table, tableName=" + name));
    }

    public boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException {
        return tryWriteLockOrException(timeout, unit, new MetaNotFoundException("unknown table, tableName=" + name));
    }

    public <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E {
        if (tryWriteLock(timeout, unit)) {
            if (isDropped) {
                writeUnlock();
                throw e;
            }
            return true;
        }
        return false;
    }

    public boolean tryWriteLockIfExist(long timeout, TimeUnit unit) {
        if (tryWriteLock(timeout, unit)) {
            if (isDropped) {
                writeUnlock();
                return false;
            }
            return true;
        }
        return false;
    }

    public boolean isTypeRead() {
        return isTypeRead;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public long getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String newName) {
        name = newName;
    }

    void setQualifiedDbName(String qualifiedDbName) {
        this.qualifiedDbName = qualifiedDbName;
    }

    public String getQualifiedDbName() {
        return qualifiedDbName;
    }

    public String getQualifiedName() {
        if (StringUtils.isEmpty(qualifiedDbName)) {
            return name;
        } else {
            return qualifiedDbName + "." + name;
        }
    }

    public TableType getType() {
        return type;
    }

    public List<Column> getFullSchema() {
        return fullSchema;
    }

    public String getDdlSql() {
        return ddlSql;
    }

    // should override in subclass if necessary
    public List<Column> getBaseSchema() {
        return getBaseSchema(Util.showHiddenColumns());
    }

    public List<Column> getBaseSchema(boolean full) {
        if (full) {
            return fullSchema;
        } else {
            return fullSchema.stream().filter(Column::isVisible).collect(Collectors.toList());
        }
    }

    public void setNewFullSchema(List<Column> newSchema) {
        this.fullSchema = newSchema;
        this.nameToColumn.clear();
        for (Column col : fullSchema) {
            nameToColumn.put(col.getName(), col);
        }
    }

    public Column getColumn(String name) {
        return nameToColumn.get(name);
    }

    public List<Column> getColumns() {
        return Lists.newArrayList(nameToColumn.values());
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getUpdateTime() {
        return -1L;
    }

    public long getRowCount() {
        return 0;
    }

    public long getAvgRowLength() {
        return 0;
    }

    public long getDataLength() {
        return 0;
    }


    public TTableDescriptor toThrift() {
        return null;
    }

    public static Table read(DataInput in) throws IOException {
        Table table = null;
        TableType type = TableType.valueOf(Text.readString(in));
        if (type == TableType.OLAP) {
            table = new OlapTable();
        } else if (type == TableType.MATERIALIZED_VIEW) {
            table = new MaterializedView();
        } else if (type == TableType.ODBC) {
            table = new OdbcTable();
        } else if (type == TableType.MYSQL) {
            table = new MysqlTable();
        } else if (type == TableType.VIEW) {
            table = new View();
        } else if (type == TableType.BROKER) {
            table = new BrokerTable();
        } else if (type == TableType.ELASTICSEARCH) {
            table = new EsTable();
        } else if (type == TableType.HIVE) {
            table = new HiveTable();
        } else if (type == TableType.ICEBERG) {
            table = new IcebergTable();
        } else if (type == TableType.HUDI) {
            table = new HudiTable();
        } else if (type == TableType.JDBC) {
            table = new JdbcTable();
        } else {
            throw new IOException("Unknown table type: " + type.name());
        }

        table.setTypeRead(true);
        table.readFields(in);
        return table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, type.name());

        // write last check time
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, name);

        // base schema
        int columnCount = fullSchema.size();
        out.writeInt(columnCount);
        for (Column column : fullSchema) {
            column.write(out);
        }

        Text.writeString(out, comment);

        // write create time
        out.writeLong(createTime);
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            type = TableType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        super.readFields(in);

        this.id = in.readLong();
        this.name = Text.readString(in);
        List<Column> keys = Lists.newArrayList();
        // base schema
        int columnCount = in.readInt();
        for (int i = 0; i < columnCount; i++) {
            Column column = Column.read(in);
            if (column.isKey()) {
                keys.add(column);
            }
            this.fullSchema.add(column);
            this.nameToColumn.put(column.getName(), column);
        }
        if (keys.size() > 1) {
            keys.forEach(key -> key.setCompoundKey(true));
            hasCompoundKey = true;
        }
        comment = Text.readString(in);

        // read create time
        this.createTime = in.readLong();
    }

    // return if this table is partitioned.
    // For OlapTable ture when is partitioned, or distributed by hash when no partition
    public boolean isPartitioned() {
        return false;
    }

    public Partition getPartition(String partitionName) {
        return null;
    }

    @Override
    public String getEngine() {
        return type.toEngineName();
    }

    @Override
    public String getMysqlType() {
        return type.toMysqlType();
    }

    @Override
    public String getComment() {
        return getComment(false);
    }

    @Override
    public String getComment(boolean escapeQuota) {
        if (!Strings.isNullOrEmpty(comment)) {
            if (!escapeQuota) {
                return comment;
            }
            return SqlUtils.escapeQuota(comment);
        }
        return type.name();
    }

    public void setComment(String comment) {
        this.comment = Strings.nullToEmpty(comment);
    }

    public void setId(long id) {
        this.id = id;
    }

    public CreateTableStmt toCreateTableStmt(String dbName) {
        throw new NotImplementedException("toCreateTableStmt not implemented");
    }

    @Override
    public String toString() {
        return "Table [id=" + id + ", name=" + name + ", type=" + type + "]";
    }

    public JSONObject toSimpleJson() {
        JSONObject table = new JSONObject();
        table.put("Type", type.toEngineName());
        table.put("Id", Long.toString(id));
        table.put("Name", name);
        return table;
    }

    /*
     * 1. Only schedule OLAP table.
     * 2. If table is colocate with other table, not schedule it.
     * 3. (deprecated). if table's state is ROLLUP or SCHEMA_CHANGE, but alter job's state is FINISHING, we should also
     *      schedule the tablet to repair it(only for VERSION_INCOMPLETE case, this will be checked in
     *      TabletScheduler).
     * 4. Even if table's state is ROLLUP or SCHEMA_CHANGE, check it. Because we can repair the tablet of base index.
     */
    public boolean needSchedule() {
        if (type != TableType.OLAP) {
            return false;
        }

        OlapTable olapTable = (OlapTable) this;

        if (Env.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
            LOG.debug("table {} is a colocate table, skip tablet checker.", name);
            return false;
        }

        return true;
    }

    public boolean isHasCompoundKey() {
        return hasCompoundKey;
    }

    public Set<String> getPartitionNames() {
        return Collections.EMPTY_SET;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        throw new NotImplementedException("createAnalysisTask not implemented");
    }

    /**
     * for NOT-ANALYZED Olap table, return estimated row count,
     * for other table, return 1
     * @return estimated row count
     */
    public long estimatedRowCount() {
        long cardinality = 0;
        if (this instanceof OlapTable) {
            OlapTable table = (OlapTable) this;
            for (long selectedPartitionId : table.getPartitionIds()) {
                final Partition partition = table.getPartition(selectedPartitionId);
                final MaterializedIndex baseIndex = partition.getBaseIndex();
                cardinality += baseIndex.getRowCount();
            }
        }
        return Math.max(cardinality, 1);
    }

    @Override
    public DatabaseIf getDatabase() {
        return Env.getCurrentInternalCatalog().getDbNullable(qualifiedDbName);
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        return Optional.empty();
    }

    public void analyze(String dbName) {}

    @Override
    public boolean needReAnalyzeTable(TableStatsMeta tblStats) {
        return true;
    }

    @Override
    public Map<String, Set<String>> findReAnalyzeNeededPartitions() {
        return Collections.emptyMap();
    }
}
