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
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public class Table extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    public enum TableType {
        MYSQL,
        OLAP,
        SCHEMA,
        INLINE_VIEW,
        VIEW,
        KUDU,
        BROKER,
        ELASTICSEARCH
    }

    protected long id;
    protected String name;
    protected TableType type;
    /*
     *  fullSchema and nameToColumn should contains all columns, both visible and shadow.
     *  eg. for OlapTable, when doing schema change, there will be some shadow columns which are not visible
     *      to query but visible to load process.
     *  If you want to get all visible columns, you should call getBaseSchema() method, which is override in
     *  sub classes.
     *  
     *  NOTICE: the order of this fullSchema is meaningless to OlapTable
     */
    protected List<Column> fullSchema;
    // tree map for case-insensitive lookup.
    protected Map<String, Column> nameToColumn;

    // DO NOT persist this variable.
    protected boolean isTypeRead = false;

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    }

    public Table(long id, String tableName, TableType type, List<Column> fullSchema) {
        this.id = id;
        this.name = tableName;
        this.type = type;
        // must copy the list, it should not be the same object as in indexIdToSchmea
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

    public TableType getType() {
        return type;
    }

    public List<Column> getFullSchema() {
        return fullSchema;
    }

    // should override in subclass if necessary
    public List<Column> getBaseSchema() {
        return fullSchema;
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

    public TTableDescriptor toThrift() {
        return null;
    }

    public static Table read(DataInput in) throws IOException {
        Table table = null;
        TableType type = TableType.valueOf(Text.readString(in));
        if (type == TableType.OLAP) {
            table = new OlapTable();
        } else if (type == TableType.MYSQL) {
            table = new MysqlTable();
        } else if (type == TableType.VIEW) {
            table = new View();
        } else if (type == TableType.KUDU) {
            table = new KuduTable();
        } else if (type == TableType.BROKER) {
            table = new BrokerTable();
        } else if (type == TableType.ELASTICSEARCH) {
            table = new EsTable();
        } else {
            throw new IOException("Unknown table type: " + type.name());
        }

        table.setTypeRead(true);
        table.readFields(in);
        if (type == TableType.VIEW) {
            View view = (View) table;
            try {
                view.init();
            } catch (UserException e) {
                throw new IOException(e.getMessage());
            }
        }

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
    }

    @Override
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
            return "Palo";
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
        if (this instanceof View) {
            return "VIEW";
        }
        return "";
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
     *      schedule the tablet to repair it(only for VERSION_IMCOMPLETE case, this will be checked in
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
