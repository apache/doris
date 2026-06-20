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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for AWS S3 and S3-compatible storage (MinIO, etc.).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by presence of AWS_ACCESS_KEY with either AWS_ENDPOINT or AWS_REGION.
 * S3 is intentionally the last-resort provider; cloud-specific providers (OSS, COS, OBS)
 * should match first via their endpoint domain patterns.
 */
public class S3FileSystemProvider implements FileSystemProvider<S3FileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_S3 = "S3";
    private static final String PROVIDER_KEY = "provider";
    private static final String FS_S3_SUPPORT = "fs.s3.support";
    private static final String[] ACCESS_KEY_NAMES = {
            S3FileSystemProperties.ACCESS_KEY, "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY",
            "glue.access_key", "aws.glue.access-key",
            "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
            "s3.access-key-id", "minio.access_key"};
    private static final String[] ENDPOINT_NAMES = {
            S3FileSystemProperties.ENDPOINT, "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
            "glue.endpoint", "aws.glue.endpoint", "minio.endpoint"};
    private static final String[] REGION_NAMES = {
            S3FileSystemProperties.REGION, "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
            "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region",
            "minio.region"};
    private static final String[] ROLE_ARN_NAMES = {
            S3FileSystemProperties.ROLE_ARN, "AWS_ROLE_ARN", "glue.role_arn"};
    private static final String[] CREDENTIALS_PROVIDER_TYPE_NAMES = {
            S3FileSystemProperties.CREDENTIALS_PROVIDER_TYPE, "AWS_CREDENTIALS_PROVIDER_TYPE",
            "glue.credentials_provider_type", "iceberg.rest.credentials_provider_type"};

    @Override
    public boolean supports(Map<String, String> properties) {
        boolean hasCredential = hasAny(properties, ACCESS_KEY_NAMES)
                || hasAny(properties, ROLE_ARN_NAMES)
                || hasAny(properties, CREDENTIALS_PROVIDER_TYPE_NAMES);
        boolean hasLocation = hasAny(properties, ENDPOINT_NAMES) || hasAny(properties, REGION_NAMES);
        if (isExplicitS3(properties)) {
            return hasLocation;
        }
        // Support both AK/SK and IAM role based access for cloud snapshot and stage flows.
        return hasCredential && hasLocation;
    }

    private boolean isExplicitS3(Map<String, String> properties) {
        return STORAGE_TYPE_S3.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))
                || STORAGE_TYPE_S3.equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || Boolean.parseBoolean(properties.getOrDefault(FS_S3_SUPPORT, "false"));
    }

    @Override
    public S3FileSystemProperties bind(Map<String, String> properties) {
        return S3FileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(S3FileSystemProperties properties) throws IOException {
        return new S3FileSystem(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "S3";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(S3FileSystemProperties.class);
    }

    private boolean hasAny(Map<String, String> properties, String[] names) {
        for (String name : names) {
            if (StringUtils.isNotBlank(properties.get(name))) {
                return true;
            }
        }
        return false;
    }
}
