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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.s3tables.CustomAwsCredentialsProvider;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergS3TablesMetaStoreProperties extends AbstractIcebergProperties {

    private S3Properties s3Properties;

    public IcebergS3TablesMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_S3_TABLES;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        s3Properties = S3Properties.of(origProps);
        s3Properties.initNormalizeAndCheckProps();
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        checkInitialized();

        Map<String, String> props = buildS3CatalogProperties();

        S3TablesCatalog catalog = new S3TablesCatalog();
        try {
            catalog.initialize(catalogName, props);
            return catalog;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize S3TablesCatalog for Iceberg. "
                    + "CatalogName=" + catalogName + ", region=" + s3Properties.getRegion(), e);
        }
    }

    private Map<String, String> buildS3CatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("client.credentials-provider", CustomAwsCredentialsProvider.class.getName());
        props.put("client.credentials-provider.s3.access-key-id", s3Properties.getAccessKey());
        props.put("client.credentials-provider.s3.secret-access-key", s3Properties.getSecretKey());
        props.put("client.credentials-provider.s3.session-token", s3Properties.getSessionToken());
        props.put("client.region", s3Properties.getRegion());

        if (StringUtils.isNotBlank(warehouse)) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }

        return props;
    }

    private void checkInitialized() {
        if (s3Properties == null) {
            throw new IllegalStateException("S3Properties not initialized."
                    + " Please call initNormalizeAndCheckProps() before using.");
        }
    }
}
