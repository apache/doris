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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantLock;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public abstract class Table extends MetaObject implements Writable, TableIf, GsonPostProcessable {
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
    protected MonitoredReentrantReadWriteLock rwLock;
    // Used for queuing commit transactifon tasks to avoid fdb transaction conflicts,
    // especially to reduce conflicts when obtaining delete bitmap update locks for
    // MoW table
    protected MonitoredReentrantLock commitLock;

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

    @SerializedName(value = "ta")
    protected TableAttributes tableAttributes = new TableAttributes();

    // check read lock leaky
    private Map<Long, String> readLockThreads = null;

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        if (Config.check_table_lock_leaky) {
            this.readLockThreads = Maps.newConcurrentMap();
        }
        this.commitLock = new MonitoredReentrantLock(true);
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
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        this.createTime = Instant.now().getEpochSecond();
        if (Config.check_table_lock_leaky) {
            this.readLockThreads = Maps.newConcurrentMap();
        }
        this.commitLock = new MonitoredReentrantLock(true);
    }

    public void markDropped() {
        isDropped = true;
    }

    public void unmarkDropped() {
        isDropped = false;
    }

    public void readLock() {
        this.rwLock.readLock().lock();
        if (this.readLockThreads != null && this.rwLock.getReadHoldCount() == 1) {
            Thread thread = Thread.currentThread();
            this.readLockThreads.put(thread.getId(),
                    "(" + thread.toString() + ", time " + System.currentTimeMillis() + ")");
        }
    }

    public boolean readLockIfExist() {
        readLock();
        if (isDropped) {
            readUnlock();
            return false;
        }
        return true;
    }

    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            boolean res = this.rwLock.readLock().tryLock(timeout, unit);
            if (res) {
                if (this.readLockThreads != null && this.rwLock.getReadHoldCount() == 1) {
                    Thread thread = Thread.currentThread();
                    this.readLockThreads.put(thread.getId(),
                            "(" + thread.toString() + ", time " + System.currentTimeMillis() + ")");
                }
            } else {
                if (unit.toSeconds(timeout) >= 1) {
                    LOG.warn("Failed to try table {}'s read lock. timeout {} {}. Current owner: {}",
                            name, timeout, unit.name(), rwLock.getOwner());
                }
            }
            return res;
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at table[" + name + "]", e);
            return false;
        }
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
        if (this.readLockThreads != null && this.rwLock.getReadHoldCount() == 0) {
            this.readLockThreads.remove(Thread.currentThread().getId());
        }
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

    // TabletStatMgr will invoke all olap tables' tryWriteLock every one minute,
    // we can set Config.check_table_lock_leaky = true
    // and check log to find out whether if the table has lock leaky.
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            boolean res = this.rwLock.writeLock().tryLock(timeout, unit);
            if (!res && unit.toSeconds(timeout) >= 1) {
                if (readLockThreads == null) {
                    LOG.warn("Failed to try table {}'s write lock. timeout {} {}. Current owner: {}",
                            name, timeout, unit.name(), rwLock.getOwner());
                } else {
                    LOG.warn("Failed to try table {}'s write lock. timeout {} {}. Current owner: {}, "
                            + "current reader: {}",
                            name, timeout, unit.name(), rwLock.getOwner(), readLockThreads);
                }
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
        writeLockOrException(new DdlException("unknown table, tableName=" + name,
                                            ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    public void writeLockOrMetaException() throws MetaNotFoundException {
        writeLockOrException(new MetaNotFoundException("unknown table, tableName=" + name,
                                            ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    public void writeLockOrAlterCancelException() throws AlterCancelException {
        writeLockOrException(new AlterCancelException("unknown table, tableName=" + name,
                                            ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    public boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException {
        return tryWriteLockOrException(timeout, unit, new MetaNotFoundException("unknown table, tableName=" + name,
                                                                ErrorCode.ERR_BAD_TABLE_ERROR));
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

    public void commitLock() {
        this.commitLock.lock();
    }

    public boolean tryCommitLock(long timeout, TimeUnit unit) {
        try {
            boolean res = this.commitLock.tryLock(timeout, unit);
            if (!res && unit.toSeconds(timeout) >= 1) {
                LOG.warn("Failed to try table {}'s cloud commit lock. timeout {} {}. Current owner: {}",
                        name, timeout, unit.name(), rwLock.getOwner());
            }
            return res;
        } catch (InterruptedException e) {
            LOG.warn("failed to try cloud commit lock at table[" + name + "]", e);
            return false;
        }
    }

    public void commitUnlock() {
        this.commitLock.unlock();
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

    public void setQualifiedDbName(String qualifiedDbName) {
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

    public String getDBName() {
        String[] strs = qualifiedDbName.split(":");
        return strs.length == 2 ? strs[1] : strs[0];
    }

    public Constraint getConstraint(String name) {
        return getConstraintsMap().get(name);
    }

    @Override
    public Map<String, Constraint> getConstraintsMapUnsafe() {
        return tableAttributes.getConstraintsMap();
    }

    public TableType getType() {
        return type;
    }

    public List<Column> getFullSchema() {
        return fullSchema;
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
        return nameToColumn.getOrDefault(name, null);
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
        return fetchRowCount();
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
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            Table table = null;
            TableType type = TableType.valueOf(Text.readString(in));
            if (type == TableType.OLAP) {
                table = new OlapTable();
            } else if (type == TableType.MATERIALIZED_VIEW) {
                table = new MTMV();
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
            } else if (type == TableType.JDBC) {
                table = new JdbcTable();
            } else {
                throw new IOException("Unknown table type: " + type.name());
            }

            table.setTypeRead(true);
            table.readFields(in);
            return table;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), Table.class);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        List<Column> keys = Lists.newArrayList();

        for (Column column : fullSchema) {
            if (column.isKey()) {
                keys.add(column);
            }
            this.nameToColumn.put(column.getName(), column);
        }
        if (keys.size() > 1) {
            keys.forEach(key -> key.setCompoundKey(true));
            hasCompoundKey = true;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
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
        // table attribute only support after version 127
        if (FeMetaVersion.VERSION_127 <= Env.getCurrentEnvJournalVersion()) {
            String json = Text.readString(in);
            this.tableAttributes = GsonUtils.GSON.fromJson(json, TableAttributes.class);

        }
        // read create time
        this.createTime = in.readLong();
    }

    // return if this table is partitioned, for planner.
    // For OlapTable ture when is partitioned, or distributed by hash when no partition
    public boolean isPartitionDistributed() {
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
        return "";
    }

    public void setComment(String comment) {
        this.comment = Strings.nullToEmpty(comment);
    }

    public void setId(long id) {
        this.id = id;
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

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        throw new NotImplementedException("createAnalysisTask not implemented");
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
    public List<Long> getChunkSizes() {
        throw new NotImplementedException("getChunkSized not implemented");
    }

    @Override
    public long fetchRowCount() {
        return 0;
    }

    @Override
    public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
        return Sets.newHashSet();
    }

    @Override
    public long getCachedRowCount() {
        return getRowCount();
    }

    @Override
    public boolean autoAnalyzeEnabled() {
        return true;
    }
}
