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
import org.apache.doris.datasource.iceberg.s3tables.CustomAwsCredentialsProvider;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

import com.google.common.collect.Maps;
import org.apache.iceberg.CatalogProperties;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.Map;

public class IcebergS3TablesExternalCatalog extends IcebergExternalCatalog {

    public IcebergS3TablesExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_S3_TABLES;
        S3TablesCatalog s3TablesCatalog = new S3TablesCatalog();
        Map<String, String> s3TablesCatalogProperties = convertToS3TablesCatalogProperties();
        String warehouse = catalogProperty.getHadoopProperties().get(CatalogProperties.WAREHOUSE_LOCATION);
        s3TablesCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        s3TablesCatalog.initialize(getName(), s3TablesCatalogProperties);
        catalog = s3TablesCatalog;
    }

    private Map<String, String> convertToS3TablesCatalogProperties() {
        Map<String, String> props = catalogProperty.getProperties();
        Map<String, String> s3Properties = Maps.newHashMap();
        s3Properties.put("client.credentials-provider", CustomAwsCredentialsProvider.class.getName());
        if (props.containsKey(S3Properties.ACCESS_KEY)) {
            s3Properties.put("client.credentials-provider.s3.access-key-id", props.get(S3Properties.ACCESS_KEY));
        }
        if (props.containsKey(S3Properties.SECRET_KEY)) {
            s3Properties.put("client.credentials-provider.s3.secret-access-key", props.get(S3Properties.SECRET_KEY));
        }
        if (props.containsKey(S3Properties.REGION)) {
            s3Properties.put("client.region", props.get(S3Properties.REGION));
        }
        return s3Properties;
    }
}
