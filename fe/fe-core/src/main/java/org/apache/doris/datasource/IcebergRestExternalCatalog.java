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
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog {

    private RESTCatalog restCatalog;
    private Map<String, String> restProperties;

    public  IcebergRestExternalCatalog(long catalogId, String name, String resource, String catalogType,
                                       Map<String, String> props) {
        super(catalogId, name, catalogType);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void init() {
        restCatalog = new RESTCatalog();
        restCatalog.initialize(icebergCatalogType, restProperties);
    }

    public org.apache.iceberg.Table getIcebergTable(String dbName, String tblName) {
        makeSureInitialized();
        return restCatalog.loadTable(TableIdentifier.of(dbName, tblName));
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return new ArrayList<>(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<TableIdentifier> tableIdentifiers = restCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return restCatalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    @Override
    protected void initLocalObjectsImpl() {
        restProperties = new HashMap<>();
        String restUri = catalogProperty.getProperties().get(CatalogProperties.URI);
        if (restUri == null) {
            throw new IllegalArgumentException("Missing 'uri' property for rest catalog");
        }
        restProperties.put(CatalogProperties.URI, restUri);
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        List<Types.NestedField> columns = restCatalog.loadTable(TableIdentifier.of(dbName, tblName))
                .schema().columns();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        for (Types.NestedField field : columns) {
            tmpSchema.add(new Column(field.name(),
                    icebergTypeToDorisTypeBeta(field.type()), true, null,
                    true, null, field.doc(), true, null, -1));
        }
        return tmpSchema;
    }

    private Type icebergTypeToDorisTypeBeta(org.apache.iceberg.types.Type type) {
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
}
