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
import org.apache.doris.datasource.HMSExternalDataSource;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive metastore external table.
 */
public class HMSExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(HMSExternalTable.class);

    private final HMSExternalDataSource ds;
    private final String dbName;
    private org.apache.hadoop.hive.metastore.api.Table remoteTable = null;
    private DLAType dlaType = DLAType.UNKNOWN;
    private boolean initialized = false;

    public enum DLAType {
        UNKNOWN, HIVE, HUDI, ICEBERG
    }

    /**
     * Create hive metastore external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param ds HMSExternalDataSource.
     */
    public HMSExternalTable(long id, String name, String dbName, HMSExternalDataSource ds) {
        super(id, name);
        this.dbName = dbName;
        this.ds = ds;
        this.type = TableType.HMS_EXTERNAL_TABLE;
    }

    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        try {
            getRemoteTable();
        } catch (MetaNotFoundException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        if (remoteTable == null) {
            dlaType = DLAType.UNKNOWN;
            fullSchema = Lists.newArrayList();
        } else {
            if (remoteTable.getParameters().containsKey("table_type") && remoteTable.getParameters().get("table_type")
                    .equalsIgnoreCase("ICEBERG")) {
                dlaType = DLAType.ICEBERG;
            } else if (remoteTable.getSd().getInputFormat().toLowerCase().contains("hoodie")) {
                dlaType = DLAType.HUDI;
            } else {
                dlaType = DLAType.HIVE;
            }
            initSchema();
        }
    }

    private void initSchema() {
        if (fullSchema == null) {
            synchronized (this) {
                if (fullSchema == null) {
                    fullSchema = Lists.newArrayList();
                    try {
                        for (FieldSchema field : HiveMetaStoreClientHelper.getSchema(dbName, name,
                                ds.getHiveMetastoreUris())) {
                            fullSchema.add(new Column(field.getName(),
                                    HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()), true, null, true,
                                    null, field.getComment()));
                        }
                    } catch (DdlException e) {
                        LOG.warn("Fail to get schema of hms table {}", name, e);
                    }
                }
            }
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
                        remoteTable = HiveMetaStoreClientHelper.getTable(dbName, name, ds.getHiveMetastoreUris());
                    } catch (DdlException e) {
                        LOG.warn("Fail to get remote hive table. db {}, table {}, uri {}", dbName, name,
                                ds.getHiveMetastoreUris());
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
        makeSureInitialized();
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
        makeSureInitialized();
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

    /**
     * get the dla type for scan node to get right information.
     */
    public DLAType getDlaType() {
        return dlaType;
    }

    @Override
    public TTableDescriptor toThrift() {
        THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.BROKER_TABLE, fullSchema.size(), 0,
                getName(), "");
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }

    public String getMetastoreUri() {
        return ds.getHiveMetastoreUris();
    }

    public Map<String, String> getDfsProperties() {
        return ds.getDsProperty().getDfsProperties();
    }

    public Map<String, String> getS3Properties() {
        return ds.getDsProperty().getS3Properties();
    }
}
