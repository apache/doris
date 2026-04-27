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
import org.apache.doris.datasource.property.common.AwsCredentialsProviderFactory;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.datasource.property.common.IcebergAwsAssumeRoleProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.S3TablesClientBuilder;
import software.amazon.s3tables.iceberg.S3TablesCatalog;
import software.amazon.s3tables.iceberg.S3TablesProperties;
import software.amazon.s3tables.iceberg.imports.HttpClientProperties;

import java.net.URI;
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
        buildS3CatalogProperties(catalogProps);
        S3TablesClient client = buildS3TablesClient(catalogProps);
        S3TablesCatalog catalog = new S3TablesCatalog();
        try {
            catalog.initialize(catalogName, catalogProps, client);
            return catalog;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize S3TablesCatalog for Iceberg. "
                    + "CatalogName=" + catalogName + ", region=" + s3Properties.getRegion()
                    + ", msg: " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private void buildS3CatalogProperties(Map<String, String> props) {
        props.put(AwsClientProperties.CLIENT_REGION, s3Properties.getRegion());
        putS3FileIOProperties(props);

        if (StringUtils.isNotBlank(s3Properties.getAccessKey())
                && StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            return;
        }
        if (StringUtils.isNotBlank(s3Properties.getS3IAMRole())) {
            IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(props, s3Properties);
            return;
        }
        putFileIOCredentialsProvider(props, s3Properties.getAwsCredentialsProviderMode());
    }

    private void putS3FileIOProperties(Map<String, String> props) {
        if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
            props.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        }
        if (StringUtils.isNotBlank(s3Properties.getUsePathStyle())) {
            props.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        }
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())) {
            props.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            props.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
            props.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
        }
    }

    private void putFileIOCredentialsProvider(Map<String, String> props,
            AwsCredentialsProviderMode providerMode) {
        if (providerMode == null || providerMode == AwsCredentialsProviderMode.DEFAULT) {
            return;
        }
        props.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                AwsCredentialsProviderFactory.getV2ClassName(providerMode));
    }

    private S3TablesClient buildS3TablesClient(Map<String, String> props) {
        S3TablesClientBuilder builder = S3TablesClient.builder()
                .region(Region.of(s3Properties.getRegion()))
                .credentialsProvider(getS3TablesCredentialsProvider());
        String s3TablesEndpoint = props.get(S3TablesProperties.S3TABLES_ENDPOINT);
        if (StringUtils.isNotBlank(s3TablesEndpoint)) {
            builder.endpointOverride(URI.create(s3TablesEndpoint));
        }
        new HttpClientProperties(props).applyHttpClientConfigurations(builder);
        return builder.build();
    }

    private AwsCredentialsProvider getS3TablesCredentialsProvider() {
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())
                && StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            return s3Properties.getAwsCredentialsProvider();
        }
        if (StringUtils.isNotBlank(s3Properties.getS3IAMRole())) {
            return s3Properties.getAwsCredentialsProvider();
        }
        return AwsCredentialsProviderFactory.createV2(
                s3Properties.getAwsCredentialsProviderMode(), false);
    }
}
