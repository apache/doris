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
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.IcebergExternalCatalog;
import org.apache.doris.datasource.IcebergHMSExternalCatalog;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class IcebergExternalTable extends ExternalTable {

    IcebergExternalCatalog icebergCatalog;

    public IcebergExternalTable(long id, String name, String dbName, IcebergExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.ICEBERG_EXTERNAL_TABLE);
        icebergCatalog = catalog;
    }

    public HMSExternalTable toHMSTable() {
        if (icebergCatalog.getIcebergCatalogType().equals("hms")) {
            IcebergHMSExternalCatalog icebergHMSCatalog = (IcebergHMSExternalCatalog) icebergCatalog;
            HMSExternalCatalog hmsCatalog = icebergHMSCatalog.getHmsExternalCatalog();
            if (!hmsCatalog.getDb(dbName).isPresent()) {
                throw new IllegalArgumentException("Database " + dbName + " exists ");
            }
            HMSExternalDatabase hmsDb = (HMSExternalDatabase) hmsCatalog.getDb(dbName).get();
            Optional<HMSExternalTable> hmsTable = hmsDb.getTable(name);
            if (!hmsTable.isPresent()) {
                throw new IllegalArgumentException("Database " + name + " exists ");
            }
            return hmsTable.get();
        } else {
            throw new RuntimeException("Only iceberg hive catalog can use");
        }
    }

    public String getIcebergCatalogType() {
        return icebergCatalog.getIcebergCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        if (icebergCatalog.getIcebergCatalogType().equals("hms")) {
            List<Column> schema = getFullSchema();
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new RuntimeException("Unsupported iceberg catalog type");
        }
    }
}
