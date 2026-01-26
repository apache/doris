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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

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
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
                               List<StorageProperties> storagePropertiesList) {
        checkInitialized();

        buildS3CatalogProperties(catalogProps);

        S3TablesCatalog catalog = new S3TablesCatalog();
        try {
            catalog.initialize(catalogName, catalogProps);
            return catalog;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize S3TablesCatalog for Iceberg. "
                + "CatalogName=" + catalogName + ", region=" + s3Properties.getRegion()
                + ", msg: " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private void buildS3CatalogProperties(Map<String, String> props) {
        props.put("client.region", s3Properties.getRegion());

        // Priority order:
        // 1. Explicit credentials (access_key + secret_key)
        // 2. AssumeRole with role ARN
        // 3. Configured credentials provider type

        boolean hasExplicitCredentials = StringUtils.isNotBlank(s3Properties.getAccessKey())
                && StringUtils.isNotBlank(s3Properties.getSecretKey());

        if (hasExplicitCredentials) {
            // Use StaticCredentialsProvider for explicit credentials
            props.put("client.credentials-provider", StaticCredentialsProvider.class.getName());
            props.put("client.credentials-provider.s3.access-key-id", s3Properties.getAccessKey());
            props.put("client.credentials-provider.s3.secret-access-key", s3Properties.getSecretKey());
            if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
                props.put("client.credentials-provider.s3.session-token", s3Properties.getSessionToken());
            }
            return;
        }

        if (StringUtils.isNotBlank(s3Properties.getS3IAMRole())) {
            props.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
            props.put("aws.region", s3Properties.getRegion());
            props.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, s3Properties.getRegion());
            props.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, s3Properties.getS3IAMRole());
            props.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, s3Properties.getRegion());
            if (StringUtils.isNotBlank(s3Properties.getS3ExternalId())) {
                props.put(AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID, s3Properties.getS3ExternalId());
            }
        }
        // If none of the above is set, S3TablesCatalog will fail
        // with a clear error message asking for credentials configuration
    }

    private void checkInitialized() {
        if (s3Properties == null) {
            throw new IllegalStateException("S3Properties not initialized."
                + " Please call initNormalizeAndCheckProps() before using.");
        }
    }
}
