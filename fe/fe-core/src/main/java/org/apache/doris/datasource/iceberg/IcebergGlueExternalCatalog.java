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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.CatalogProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergGlueExternalCatalog extends IcebergExternalCatalog {

    public IcebergGlueExternalCatalog(long catalogId, String name, String resource, Map<String, String> props) {
        super(catalogId, name);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        icebergCatalogType = ICEBERG_GLUE;
        GlueCatalog glueCatalog = new GlueCatalog();
        // AWSGlueAsync glueClient;
        Configuration conf = setGlueProperties(getConfiguration());
        glueCatalog.setConf(conf);
        // initialize glue catalog
        Map<String, String> catalogProperties = catalogProperty.getProperties();
        // check AwsProperties.GLUE_CATALOG_ENDPOINT
        String metastoreUris = catalogProperty.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
        if (StringUtils.isEmpty(metastoreUris)) {
            throw new IllegalArgumentException("Missing glue properties 'warehouse'.");
        }
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, metastoreUris);
        glueCatalog.initialize(icebergCatalogType, catalogProperties);
        catalog = glueCatalog;
    }

    private Configuration setGlueProperties(Configuration configuration) {
        return configuration;
    }

    @Override
    protected List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
            .map(Namespace::toString)
            .collect(Collectors.toList());
    }
}
