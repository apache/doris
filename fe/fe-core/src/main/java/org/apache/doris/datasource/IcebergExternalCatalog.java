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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.IcebergExternalDatabase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    protected final String icebergCatalogType;

    public IcebergExternalCatalog(long catalogId, String name, String type) {
        super(catalogId, name);
        this.icebergCatalogType = type;
    }

    @Override
    protected void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.ICEBERG);
        List<String> allDatabaseNames = getDbNames();
        for (String dbName : allDatabaseNames) {
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(dbName)) {
                dbId = dbNameToId.get(dbName);
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase db = idToDb.get(dbId);
                db.setUnInitialized(invalidCacheInInit);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(dbName, dbId);
                IcebergExternalDatabase db = new IcebergExternalDatabase(this, dbId, dbName);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, dbName);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    protected Type icebergTypeToDorisTypeBeta(org.apache.iceberg.types.Type type) {
        switch (type.typeId()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case LONG:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
            case BINARY:
            case FIXED:
            case UUID:
                return Type.STRING;
            case STRUCT:
                return Type.STRUCT;
            case LIST:
                return Type.ARRAY;
            case MAP:
                return Type.MAP;
            case DATE:
                return Type.DATEV2;
            case TIME:
                return Type.TIMEV2;
            case TIMESTAMP:
                return Type.DATETIMEV2;
            case DECIMAL:
                return Type.DECIMALV2;
            default:
                throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }

    public String getIcebergCatalogType() {
        return icebergCatalogType;
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        List<Types.NestedField> columns = getIcebergTable(dbName, tblName).schema().columns();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        for (Types.NestedField field : columns) {
            tmpSchema.add(new Column(field.name(),
                    icebergTypeToDorisTypeBeta(field.type()), true, null,
                    true, null, field.doc(), true, null, -1));
        }
        return tmpSchema;
    }

    public abstract org.apache.iceberg.Table getIcebergTable(String dbName, String tblName);
}
