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

package org.apache.doris.datasource.property.common;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

public final class IcebergAwsClientCredentialsProperties {

    private IcebergAwsClientCredentialsProperties() {}

    public static void putCredentialProviderProperties(Map<String, String> target, StorageAdapter s3Adapter) {
        switch (getCredentialType(s3Adapter)) {
            case EXPLICIT:
                S3CompatibleFileSystemProperties s3 = spi(s3Adapter);
                putExplicitRestCredentials(target, s3.getAccessKey(), s3.getSecretKey(),
                        s3.getSessionToken());
                return;
            case ASSUME_ROLE:
                IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(target, s3Adapter);
                return;
            case PROVIDER_CHAIN:
                putCredentialsProvider(target, s3Adapter.getAwsCredentialsProviderMode());
                return;
            default:
                throw new IllegalStateException("Unsupported Iceberg AWS credential type");
        }
    }

    public static void putCredentialProviderProperties(Map<String, String> target,
            String accessKey, String secretKey, String sessionToken, AwsCredentialsProviderMode providerMode) {
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            putExplicitRestCredentials(target, accessKey, secretKey, sessionToken);
            return;
        }
        putCredentialsProvider(target, providerMode);
    }

    public static void putS3FileIOCredentialProperties(Map<String, String> target,
            StorageAdapter s3Adapter) {
        putS3FileIOProperties(target, spi(s3Adapter));
        switch (getCredentialType(s3Adapter)) {
            case EXPLICIT:
                return;
            case ASSUME_ROLE:
                IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(target, s3Adapter);
                return;
            case PROVIDER_CHAIN:
                putCredentialsProvider(target, s3Adapter.getAwsCredentialsProviderMode());
                return;
            default:
                throw new IllegalStateException("Unsupported Iceberg AWS credential type");
        }
    }

    public static AwsCredentialsProvider createAwsCredentialsProvider(StorageAdapter s3Adapter,
            boolean includeAnonymousInDefault) {
        switch (getCredentialType(s3Adapter)) {
            case EXPLICIT:
            case ASSUME_ROLE:
                return s3Adapter.getAwsCredentialsProvider();
            case PROVIDER_CHAIN:
                return AwsCredentialsProviderFactory.createV2(
                        s3Adapter.getAwsCredentialsProviderMode(), includeAnonymousInDefault);
            default:
                throw new IllegalStateException("Unsupported Iceberg AWS credential type");
        }
    }

    private static S3CompatibleFileSystemProperties spi(StorageAdapter s3Adapter) {
        return (S3CompatibleFileSystemProperties) s3Adapter.getSpiProperties();
    }

    private static CredentialType getCredentialType(StorageAdapter s3Adapter) {
        S3CompatibleFileSystemProperties s3 = spi(s3Adapter);
        if (StringUtils.isNotBlank(s3.getAccessKey())
                && StringUtils.isNotBlank(s3.getSecretKey())) {
            return CredentialType.EXPLICIT;
        }
        // legacy getS3IAMRole == SPI getRoleArn
        if (StringUtils.isNotBlank(s3.getRoleArn())) {
            return CredentialType.ASSUME_ROLE;
        }
        return CredentialType.PROVIDER_CHAIN;
    }

    private static void putExplicitRestCredentials(Map<String, String> target,
            String accessKey, String secretKey, String sessionToken) {
        target.put(AwsProperties.REST_ACCESS_KEY_ID, accessKey);
        target.put(AwsProperties.REST_SECRET_ACCESS_KEY, secretKey);
        if (StringUtils.isNotBlank(sessionToken)) {
            target.put(AwsProperties.REST_SESSION_TOKEN, sessionToken);
        }
    }

    private static void putS3FileIOProperties(Map<String, String> target,
            S3CompatibleFileSystemProperties s3Properties) {
        if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
            target.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        }
        if (StringUtils.isNotBlank(s3Properties.getUsePathStyle())) {
            target.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        }
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())) {
            target.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            target.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
            target.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
        }
    }

    private static void putCredentialsProvider(Map<String, String> target,
            AwsCredentialsProviderMode providerMode) {
        if (providerMode == null || providerMode == AwsCredentialsProviderMode.DEFAULT) {
            return;
        }
        target.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                AwsCredentialsProviderFactory.getV2ClassName(providerMode));
    }

    private enum CredentialType {
        EXPLICIT,
        ASSUME_ROLE,
        PROVIDER_CHAIN
    }
}
