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
import org.apache.doris.datasource.property.common.AwsCredentialsProviderFactory;
import org.apache.doris.datasource.property.common.IcebergAwsAssumeRoleProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.catalog.Catalog;
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

        // Priority 1: Use explicit credentials (AK/SK)
        boolean hasExplicitCredentials = StringUtils.isNotBlank(s3Properties.getAccessKey())
                && StringUtils.isNotBlank(s3Properties.getSecretKey());
        if (hasExplicitCredentials) {
            props.put("client.credentials-provider", CustomAwsCredentialsProvider.class.getName());
            props.put("client.credentials-provider.s3.access-key-id", s3Properties.getAccessKey());
            props.put("client.credentials-provider.s3.secret-access-key", s3Properties.getSecretKey());
            if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
                props.put("client.credentials-provider.s3.session-token", s3Properties.getSessionToken());
            }
            return;
        }

        // Priority 2: Use IAM Role (AssumeRole)
        if (StringUtils.isNotBlank(s3Properties.getS3IAMRole())) {
            IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(props,
                    s3Properties.getRegion(), s3Properties.getS3IAMRole(), s3Properties.getS3ExternalId());
            return;
        }

        // Priority 3: Use credentials provider chain
        // S3Properties already has awsCredentialsProviderMode initialized
        // For S3Tables, use the same provider class name as Iceberg REST
        if (s3Properties.getAwsCredentialsProviderMode() != null) {
            String providerClassName = AwsCredentialsProviderFactory
                    .getV2ClassName(s3Properties.getAwsCredentialsProviderMode());
            props.put("client.credentials-provider", providerClassName);
        }
    }

    private void checkInitialized() {
        if (s3Properties == null) {
            throw new IllegalStateException("S3Properties not initialized."
                + " Please call initNormalizeAndCheckProps() before using.");
        }
    }
}
