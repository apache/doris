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
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergGlueMetaStoreProperties extends AbstractIcebergProperties {

    public AWSGlueMetaStoreBaseProperties glueProperties;

    public S3Properties s3Properties;

    // As a default placeholder. The path just use for 'create table', query stmt will not use it.
    private static final String CHECKED_WAREHOUSE = "s3://doris";

    public IcebergGlueMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_GLUE;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        glueProperties = AWSGlueMetaStoreBaseProperties.of(origProps);
        glueProperties.checkAndInit();
        s3Properties = S3Properties.of(origProps);
        s3Properties.initNormalizeAndCheckProps();
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storageProperties) {
        Map<String, String> props = prepareBaseCatalogProps();
        appendS3Props(props);
        appendGlueProps(props);

        props.put("client.region", glueProperties.glueRegion);


        if (StringUtils.isNotBlank(warehouse)) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        } else {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, CHECKED_WAREHOUSE);
        }

        GlueCatalog catalog = new GlueCatalog();
        catalog.initialize(catalogName, props);
        return catalog;
    }

    private Map<String, String> prepareBaseCatalogProps() {
        return new HashMap<>(origProps);
    }

    private void appendS3Props(Map<String, String> props) {
        props.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        props.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        props.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        props.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        props.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
    }

    private void appendGlueProps(Map<String, String> props) {
        props.put(AwsProperties.GLUE_CATALOG_ENDPOINT, glueProperties.glueEndpoint);
        props.put("client.credentials-provider",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        props.put("client.credentials-provider.glue.access_key", glueProperties.glueAccessKey);
        props.put("client.credentials-provider.glue.secret_key", glueProperties.glueSecretKey);
        props.put("aws.catalog.credentials.provider.factory.class",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProviderFactory");
    }
}
