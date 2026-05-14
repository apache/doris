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

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Credential provider factory for S3 filesystem AWS SDK v2 clients.
 */
public final class S3CredentialsProviderFactory {

    private S3CredentialsProviderFactory() {
    }

    public static AwsCredentialsProvider createClientProvider(S3FileSystemProperties properties) {
        return createClientProvider(properties, S3CredentialsProviderFactory::buildStsClient);
    }

    public static AwsCredentialsProvider createClientProvider(S3FileSystemProperties properties,
            BiFunction<AwsCredentialsProvider, String, StsClient> stsClientFactory) {
        if (properties.hasAssumeRole()) {
            return createAssumeRoleProvider(properties, stsClientFactory);
        }
        return createBaseProvider(properties, true);
    }

    public static AwsCredentialsProvider createStsSourceProvider(S3FileSystemProperties properties) {
        return createBaseProvider(properties, false);
    }

    public static AwsCredentialsProvider create(
            S3CredentialsProviderType type, boolean includeAnonymousInDefault) {
        switch (type) {
            case ENV:
                return EnvironmentVariableCredentialsProvider.create();
            case SYSTEM_PROPERTIES:
                return SystemPropertyCredentialsProvider.create();
            case WEB_IDENTITY:
                return WebIdentityTokenFileCredentialsProvider.create();
            case CONTAINER:
                return ContainerCredentialsProvider.create();
            case INSTANCE_PROFILE:
                return InstanceProfileCredentialsProvider.create();
            case ANONYMOUS:
                return AnonymousCredentialsProvider.create();
            case DEFAULT:
                return createDefault(includeAnonymousInDefault);
            default:
                throw new UnsupportedOperationException(
                        "AWS SDK V2 does not support credentials provider mode: " + type);
        }
    }

    public static String hadoopClassName(
            S3CredentialsProviderType type, boolean includeAnonymousInDefault) {
        switch (type) {
            case ENV:
                return EnvironmentVariableCredentialsProvider.class.getName();
            case SYSTEM_PROPERTIES:
                return SystemPropertyCredentialsProvider.class.getName();
            case WEB_IDENTITY:
                return WebIdentityTokenFileCredentialsProvider.class.getName();
            case CONTAINER:
                return ContainerCredentialsProvider.class.getName();
            case INSTANCE_PROFILE:
                return InstanceProfileCredentialsProvider.class.getName();
            case ANONYMOUS:
                return AnonymousCredentialsProvider.class.getName();
            case DEFAULT:
                List<String> providers = new ArrayList<>();
                providers.add(EnvironmentVariableCredentialsProvider.class.getName());
                providers.add(SystemPropertyCredentialsProvider.class.getName());
                providers.add(InstanceProfileCredentialsProvider.class.getName());
                if (isWebIdentityConfigured()) {
                    providers.add(WebIdentityTokenFileCredentialsProvider.class.getName());
                }
                if (isContainerCredentialsConfigured()) {
                    providers.add(ContainerCredentialsProvider.class.getName());
                }
                if (includeAnonymousInDefault) {
                    providers.add(AnonymousCredentialsProvider.class.getName());
                }
                return String.join(",", providers);
            default:
                throw new UnsupportedOperationException(
                        "AWS SDK V2 does not support credentials provider mode: " + type);
        }
    }

    private static AwsCredentialsProvider createAssumeRoleProvider(S3FileSystemProperties properties,
            BiFunction<AwsCredentialsProvider, String, StsClient> stsClientFactory) {
        StsClient stsClient = stsClientFactory.apply(createStsSourceProvider(properties), properties.getRegion());
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(builder -> {
                    builder.roleArn(properties.getRoleArn())
                            .roleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
                    String externalId = properties.getExternalId();
                    if (StringUtils.isNotBlank(externalId)) {
                        builder.externalId(externalId);
                    }
                }).build();
    }

    private static StsClient buildStsClient(AwsCredentialsProvider credentialsProvider, String region) {
        return StsClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();
    }

    private static AwsCredentialsProvider createBaseProvider(S3FileSystemProperties properties,
            boolean includeAnonymousInDefault) {
        AwsCredentialsProvider staticProvider = createStaticProvider(
                properties.getAccessKey(),
                properties.getSecretKey(),
                properties.getSessionToken());
        if (staticProvider != null) {
            return staticProvider;
        }
        return create(properties.getCredentialsProviderType(), includeAnonymousInDefault);
    }

    private static AwsCredentialsProvider createStaticProvider(String accessKey, String secretKey,
            String sessionToken) {
        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            return null;
        }
        if (StringUtils.isNotBlank(sessionToken)) {
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
        }
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }

    private static AwsCredentialsProvider createDefault(boolean includeAnonymous) {
        List<AwsCredentialsProvider> providers = new ArrayList<>();
        providers.add(InstanceProfileCredentialsProvider.create());
        if (isWebIdentityConfigured()) {
            providers.add(WebIdentityTokenFileCredentialsProvider.create());
        }
        if (isContainerCredentialsConfigured()) {
            providers.add(ContainerCredentialsProvider.create());
        }
        providers.add(EnvironmentVariableCredentialsProvider.create());
        providers.add(SystemPropertyCredentialsProvider.create());
        if (includeAnonymous) {
            providers.add(AnonymousCredentialsProvider.create());
        }
        return AwsCredentialsProviderChain.builder()
                .credentialsProviders(providers)
                .build();
    }

    private static boolean isWebIdentityConfigured() {
        return StringUtils.isNotBlank(System.getenv("AWS_ROLE_ARN"))
                && StringUtils.isNotBlank(System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"));
    }

    private static boolean isContainerCredentialsConfigured() {
        return StringUtils.isNotBlank(System.getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"))
                || StringUtils.isNotBlank(System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"));
    }
}
