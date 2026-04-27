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

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;

import java.util.Map;
import java.util.Objects;

public final class IcebergAwsClientCredentialsProperties {

    private IcebergAwsClientCredentialsProperties() {}

    public static void putRestCredentialProviderProperties(Map<String, String> target,
            String region, String accessKey, String secretKey, String sessionToken,
            String roleArn, String externalId, AwsCredentialsProviderMode providerMode) {
        switch (getCredentialType(accessKey, secretKey, roleArn)) {
            case EXPLICIT:
                putRestExplicitCredentials(target, accessKey, secretKey, sessionToken);
                return;
            case ASSUME_ROLE:
                IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(target, region, roleArn,
                        externalId, providerMode);
                return;
            case PROVIDER_CHAIN:
                putCredentialsProvider(target, providerMode);
                return;
            default:
                throw new IllegalStateException("Unsupported Iceberg AWS credential type");
        }
    }

    public static void putS3TablesCredentialProviderProperties(Map<String, String> target,
            String region, String accessKey, String secretKey, String sessionToken,
            String roleArn, String externalId, AwsCredentialsProviderMode providerMode,
            String explicitCredentialsProviderClassName) {
        switch (getCredentialType(accessKey, secretKey, roleArn)) {
            case EXPLICIT:
                putS3TablesExplicitCredentials(target, accessKey, secretKey, sessionToken,
                        explicitCredentialsProviderClassName);
                return;
            case ASSUME_ROLE:
                IcebergAwsAssumeRoleProperties.putAssumeRoleProperties(target, region, roleArn,
                        externalId, providerMode);
                return;
            case PROVIDER_CHAIN:
                putCredentialsProvider(target, providerMode);
                return;
            default:
                throw new IllegalStateException("Unsupported Iceberg AWS credential type");
        }
    }

    private static CredentialType getCredentialType(String accessKey, String secretKey, String roleArn) {
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            return CredentialType.EXPLICIT;
        }
        if (StringUtils.isNotBlank(roleArn)) {
            return CredentialType.ASSUME_ROLE;
        }
        return CredentialType.PROVIDER_CHAIN;
    }

    private static void putRestExplicitCredentials(Map<String, String> target,
            String accessKey, String secretKey, String sessionToken) {
        target.put(AwsProperties.REST_ACCESS_KEY_ID, accessKey);
        target.put(AwsProperties.REST_SECRET_ACCESS_KEY, secretKey);
        if (StringUtils.isNotBlank(sessionToken)) {
            target.put(AwsProperties.REST_SESSION_TOKEN, sessionToken);
        }
    }

    private static void putS3TablesExplicitCredentials(Map<String, String> target,
            String accessKey, String secretKey, String sessionToken,
            String explicitCredentialsProviderClassName) {
        Objects.requireNonNull(explicitCredentialsProviderClassName,
                "explicitCredentialsProviderClassName is required");
        target.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, explicitCredentialsProviderClassName);
        target.put("client.credentials-provider.s3.access-key-id", accessKey);
        target.put("client.credentials-provider.s3.secret-access-key", secretKey);
        if (StringUtils.isNotBlank(sessionToken)) {
            target.put("client.credentials-provider.s3.session-token", sessionToken);
        }
    }

    private static void putCredentialsProvider(Map<String, String> target,
            AwsCredentialsProviderMode providerMode) {
        Objects.requireNonNull(providerMode, "providerMode is required");
        target.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                AwsCredentialsProviderFactory.getV2ClassName(providerMode));
    }

    private enum CredentialType {
        EXPLICIT,
        ASSUME_ROLE,
        PROVIDER_CHAIN
    }
}
