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


import org.apache.doris.common.io.Text;
import org.apache.doris.external.iceberg.IcebergCatalogMgr;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * External Iceberg table
 */
public class IcebergTable extends Table {
    private static final Logger LOG = LogManager.getLogger(IcebergDatabase.class);

    // remote Iceberg database name
    private String icebergDb;
    // remote Iceberg table name
    private String icebergTbl;
    private Map<String, String> icebergProperties = Maps.newHashMap();

    private org.apache.iceberg.Table icebergTable;

    public IcebergTable() {
        super(TableType.ICEBERG);
    }

    public IcebergTable(long id, String tableName, List<Column> fullSchema, Map<String, String> properties,
                        org.apache.iceberg.Table icebergTable) {
        super(id, tableName, TableType.ICEBERG, fullSchema);
        this.icebergDb = properties.get(IcebergCatalogMgr.DATABASE);
        this.icebergTbl = properties.get(IcebergCatalogMgr.TABLE);

        icebergProperties.put(IcebergCatalogMgr.HIVE_METASTORE_URIS,
                properties.get(IcebergCatalogMgr.HIVE_METASTORE_URIS));
        icebergProperties.put(IcebergCatalogMgr.CATALOG_TYPE,
                properties.get(IcebergCatalogMgr.CATALOG_TYPE));
        this.icebergTable = icebergTable;
    }

    public String getIcebergDbTable() {
        return String.format("%s.%s", icebergDb, icebergTbl);
    }

    public String getIcebergDb() {
        return icebergDb;
    }

    public void setIcebergDb(String icebergDb) {
        this.icebergDb = icebergDb;
    }

    public String getIcebergTbl() {
        return icebergTbl;
    }

    public void setIcebergTbl(String icebergTbl) {
        this.icebergTbl = icebergTbl;
    }

    public Map<String, String> getIcebergProperties() {
        return icebergProperties;
    }

    public void setIcebergProperties(Map<String, String> icebergProperties) {
        this.icebergProperties = icebergProperties;
    }

    public org.apache.iceberg.Table getIcebergTable() {
        return icebergTable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, icebergDb);
        Text.writeString(out, icebergTbl);

        out.writeInt(icebergProperties.size());
        for (Map.Entry<String, String> entry : icebergProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        icebergDb = Text.readString(in);
        icebergTbl = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            icebergProperties.put(key, value);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        TIcebergTable tIcebergTable = new TIcebergTable(getIcebergDb(), getIcebergTbl(), getIcebergProperties());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.BROKER_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setIcebergTable(tIcebergTable);
        return tTableDescriptor;
    }
}
