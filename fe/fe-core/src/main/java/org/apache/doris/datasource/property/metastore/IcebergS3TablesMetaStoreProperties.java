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
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderFactory;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.List;
import java.util.Map;

public class IcebergS3TablesMetaStoreProperties extends AbstractIcebergProperties {

    @ConnectorProperty(names = {"s3tables.credentials-provider-type"},
            required = false,
            description = "The AWS credentials provider type for S3Tables catalog. "
                    + "Options: DEFAULT, ENV, SYSTEM_PROPERTIES, WEB_IDENTITY, CONTAINER, INSTANCE_PROFILE. "
                    + "When explicit credentials (access_key/secret_key) are provided, they take precedence. "
                    + "When no explicit credentials are provided, this determines how credentials are resolved.")
    private String s3tablesCredentialsProviderType = "";

    @ConnectorProperty(names = {"s3tables.assume-role.arn", "s3.role_arn"},
            required = false,
            description = "The IAM role ARN to assume for cross-account access. "
                    + "When set, uses STS AssumeRole to get temporary credentials.")
    private String s3tablesAssumeRoleArn = "";

    @ConnectorProperty(names = {"s3tables.assume-role.external-id", "s3.external_id"},
            required = false,
            description = "The external ID for STS AssumeRole, used for cross-account access security.")
    private String s3tablesAssumeRoleExternalId = "";

    private S3Properties s3Properties;
    private AwsCredentialsProviderMode awsCredentialsProviderMode;

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
        initAwsCredentialsProviderMode();
    }

    private void initAwsCredentialsProviderMode() {
        if (StringUtils.isNotBlank(s3tablesCredentialsProviderType)) {
            awsCredentialsProviderMode = AwsCredentialsProviderMode.fromString(s3tablesCredentialsProviderType);
        }
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
        } else if (StringUtils.isNotBlank(s3tablesAssumeRoleArn)) {
            // Use AssumeRoleAwsClientFactory for cross-account access
            props.put("client.factory", "org.apache.iceberg.aws.AssumeRoleAwsClientFactory");
            props.put("client.assume-role.arn", s3tablesAssumeRoleArn);
            props.put("client.assume-role.region", s3Properties.getRegion());
            if (StringUtils.isNotBlank(s3tablesAssumeRoleExternalId)) {
                props.put("client.assume-role.external-id", s3tablesAssumeRoleExternalId);
            }
        } else if (awsCredentialsProviderMode != null) {
            // Use configured credentials provider when no explicit credentials
            String providerClassName = AwsCredentialsProviderFactory.getV2ClassName(
                    awsCredentialsProviderMode, false);
            props.put("client.credentials-provider", providerClassName);
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
