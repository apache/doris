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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public class Table extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    public enum TableType {
        MYSQL,
        ODBC,
        OLAP,
        SCHEMA,
        INLINE_VIEW,
        VIEW,
        BROKER,
        ELASTICSEARCH,
        HIVE
    }

    protected long id;
    protected volatile String name;
    protected TableType type;
    protected long createTime;
    protected ReentrantReadWriteLock rwLock;

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
     *
     * If you want to get the mv columns, you should call getIndexToSchema in Subclass OlapTable.
     */
    protected List<Column> fullSchema;
    // tree map for case-insensitive lookup.
    /**
     * The nameToColumn of OlapTable includes the base columns and the SHADOW_NAME_PREFIX columns.
     */
    protected Map<String, Column> nameToColumn;

    // DO NOT persist this variable.
    protected boolean isTypeRead = false;
    // table(view)'s comment
    protected String comment = "";
    // sql for creating this table, default is "";
    protected String ddlSql = "";

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        this.rwLock = new ReentrantReadWriteLock(true);
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
                nameToColumn.put(col.getName(), col);
            }
        } else {
            // Only view in with-clause have null base
            Preconditions.checkArgument(type == TableType.VIEW, "Table has no columns");
        }
        this.rwLock = new ReentrantReadWriteLock();
        this.createTime = Instant.now().getEpochSecond();
    }

    public void readLock() {
        this.rwLock.readLock().lock();
    }

    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.readLock().tryLock(timeout, unit);
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

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
           return this.rwLock.writeLock().tryLock(timeout, unit);
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

    public boolean isTypeRead() {
        return isTypeRead;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        name = newName;
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
            return fullSchema.stream().filter(column -> column.isVisible()).collect(Collectors.toList());
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

    public long getCreateTime() {
        return createTime;
    }

    public TTableDescriptor toThrift() {
        return null;
    }

    public static Table read(DataInput in) throws IOException {
        Table table = null;
        TableType type = TableType.valueOf(Text.readString(in));
        if (type == TableType.OLAP) {
            table = new OlapTable();
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

        // base schema
        int columnCount = in.readInt();
        for (int i = 0; i < columnCount; i++) {
            Column column = Column.read(in);
            this.fullSchema.add(column);
            this.nameToColumn.put(column.getName(), column);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_63) {
            comment = Text.readString(in);
        } else {
            comment = "";
        }

        // read create time
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_64) {
            this.createTime = in.readLong();
        } else {
            this.createTime = -1L;
        }
    }

    public boolean equals(Table table) {
        return true;
    }

    // return if this table is partitioned.
    // For OlapTable ture when is partitioned, or distributed by hash when no partition
    public boolean isPartitioned() {
        return false;
    }

    public Partition getPartition(String partitionName) {
        return null;
    }

    public String getEngine() {
        if (this instanceof OlapTable) {
            return "Doris";
        } else if (this instanceof OdbcTable) {
            return "Odbc";
        } else if (this instanceof MysqlTable) {
            return "MySQL";
        } else if (this instanceof SchemaTable) {
            return "MEMORY";
        } else {
            return null;
        }
    }

    public String getMysqlType() {
        if (this instanceof View) {
            return "VIEW";
        }
        return "BASE TABLE";
    }

    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return type.name();
    }

    public void setComment(String comment) {
        this.comment = Strings.nullToEmpty(comment);
    }

    public CreateTableStmt toCreateTableStmt(String dbName) {
        throw new NotImplementedException();
    }

    @Override
    public int getSignature(int signatureVersion) {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return "Table [id=" + id + ", name=" + name + ", type=" + type + "]";
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

        if (Catalog.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
            LOG.debug("table {} is a colocate table, skip tablet checker.", name);
            return false;
        }

        return true;
    }
}
