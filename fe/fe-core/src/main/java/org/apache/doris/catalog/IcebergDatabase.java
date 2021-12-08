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

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Internal represent Iceberg database in Doris
 */
public class IcebergDatabase extends Database {
    private static final Logger LOG = LogManager.getLogger(IcebergDatabase.class);

    private String icebergDb;
    private Map<String, String> dbProperties = Maps.newHashMap();

    public IcebergDatabase() {
        super(0, null, DbType.ICEBERG);
    }
    public IcebergDatabase(long id, String dbName, Map<String, String> properties) {
        super(id, dbName, DbType.ICEBERG);
        this.icebergDb = properties.get(IcebergCatalogMgr.DATABASE);
        dbProperties.put(IcebergCatalogMgr.HIVE_METASTORE_URIS, properties.get(IcebergCatalogMgr.HIVE_METASTORE_URIS));
        dbProperties.put(IcebergCatalogMgr.CATALOG_TYPE, properties.get(IcebergCatalogMgr.CATALOG_TYPE));
    }

    public String getIcebergDb() {
        return icebergDb;
    }

    public Map<String, String> getDbProperties() {
        return dbProperties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        // write db name
        Text.writeString(out, icebergDb);
        // write properties
        out.writeInt(dbProperties.size());
        for (Map.Entry<String, String> entry : dbProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        icebergDb = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            dbProperties.put(key, value);
        }
    }
}
