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
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.hadoop.fs.s3a.Constants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergGlueExternalCatalog extends IcebergExternalCatalog {

    // As a default placeholder. The path just use for 'create table', query stmt will not use it.
    private static final String CHECKED_WAREHOUSE = "s3://doris";

    public IcebergGlueExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        icebergCatalogType = ICEBERG_GLUE;
        GlueCatalog glueCatalog = new GlueCatalog();
        glueCatalog.setConf(getConfiguration());
        // initialize glue catalog
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        String warehouse = catalogProperty.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, CHECKED_WAREHOUSE);
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        // read from converted s3 endpoint or default by BE s3 endpoint
        String endpoint = catalogProperties.getOrDefault(Constants.ENDPOINT,
                catalogProperties.get(S3Properties.Env.ENDPOINT));
        catalogProperties.putIfAbsent(S3FileIOProperties.ENDPOINT, endpoint);

        glueCatalog.initialize(icebergCatalogType, catalogProperties);
        catalog = glueCatalog;
    }

    @Override
    protected List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
            .map(Namespace::toString)
            .collect(Collectors.toList());
    }
}
