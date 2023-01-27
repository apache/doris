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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

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
}
