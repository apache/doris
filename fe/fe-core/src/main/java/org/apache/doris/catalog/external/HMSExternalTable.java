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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Hive metastore external table.
 */
public class HMSExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(HMSExternalTable.class);

    private final String metastoreUri;
    private final String dbName;
    private org.apache.hadoop.hive.metastore.api.Table remoteTable = null;

    /**
     * Create hive metastore external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param uri Hive metastore uri.
     */
    public HMSExternalTable(long id, String name, String dbName, String uri) throws MetaNotFoundException {
        super(id, name);
        this.dbName = dbName;
        this.metastoreUri = uri;
        init();
    }

    private void init() throws MetaNotFoundException {
        getRemoteTable();
        if (remoteTable.getParameters().containsKey("table_type")
                && remoteTable.getParameters().get("table_type").equalsIgnoreCase("ICEBERG")) {
            type = TableType.ICEBERG;
        } else if (remoteTable.getSd().getInputFormat().toLowerCase().contains("hoodie")) {
            type = TableType.HUDI;
        } else {
            type = TableType.HIVE;
        }
    }

    /**
     * Get the related remote hive metastore table.
     */
    public org.apache.hadoop.hive.metastore.api.Table getRemoteTable() throws MetaNotFoundException {
        if (remoteTable == null) {
            synchronized (this) {
                if (remoteTable == null) {
                    try {
                        remoteTable = HiveMetaStoreClientHelper.getTable(dbName, name, metastoreUri);
                    } catch (DdlException e) {
                        LOG.warn("Fail to get remote hive table. db {}, table {}, uri {}",
                                dbName, name, metastoreUri);
                        throw new MetaNotFoundException(e);
                    }
                }
            }
        }
        // TODO: Refresh cached remoteTable
        return remoteTable;
    }

    @Override
    public List<Column> getFullSchema() {
        if (fullSchema == null) {
            synchronized (this) {
                if (fullSchema == null) {
                    try {
                        for (FieldSchema field : HiveMetaStoreClientHelper.getSchema(dbName, name, metastoreUri)) {
                            fullSchema.add(new Column(field.getName(),
                                    HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()),
                                    true, null,
                                    true, null, field.getComment()));
                        }
                    } catch (DdlException e) {
                        LOG.warn("Fail to get schema of hms table {}", name, e);
                    }
                }
            }
        }
        // TODO: Refresh cached fullSchema.
        return fullSchema;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getFullSchema();
    }

    @Override
    public Column getColumn(String name) {
        if (fullSchema == null) {
            getFullSchema();
        }
        for (Column column : fullSchema) {
            if (name.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    @Override
    public String getMysqlType() {
        return type.name();
    }

    @Override
    public String getEngine() {
        switch (type) {
            case HIVE:
                return "Hive";
            case ICEBERG:
                return "Iceberg";
            case HUDI:
                return "Hudi";
            default:
                return null;
        }
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getUpdateTime() {
        return 0;
    }

    @Override
    public long getRowCount() {
        return 0;
    }

    @Override
    public long getDataLength() {
        return 0;
    }

    @Override
    public long getAvgRowLength() {
        return 0;
    }

    public long getLastCheckTime() {
        return 0;
    }

    /**
     * get database name of hms table.
     */
    public String getDbName() {
        return dbName;
    }
}
