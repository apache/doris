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
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog {

    public  IcebergRestExternalCatalog(long catalogId, String name, String resource, String catalogType,
                                       Map<String, String> props) {
        super(catalogId, name, catalogType);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> restProperties = new HashMap<>();
        String restUri = catalogProperty.getProperties().getOrDefault(CatalogProperties.URI, "");
        restProperties.put(CatalogProperties.URI, restUri);
        RESTCatalog restCatalog = new RESTCatalog();
        restCatalog.initialize(icebergCatalogType, restProperties);
        catalog = restCatalog;
    }

    @Override
    public List<String> listDatabaseNames() {
        return ((RESTCatalog) catalog).listNamespaces().stream()
            .map(Namespace::toString).collect(Collectors.toList());
    }
}
